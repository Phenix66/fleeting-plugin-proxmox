package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
)

const (
	removalTestNode                 = "pve-node"
	removalTestPool                 = "test-pool"
	removalTestStorage              = "local"
	removalTestMemberTypeLXC        = "lxc"
	removalTestForeignCreating      = "other-creating"
	removalTestPools                = "/pools"
	removalTestSemicolonTags        = "a;b"
	removalTestTemplateTagsCreating = "template-tag,manual-tag,fleeting-creating"
	removalTestTagsCreating         = "template-tag,fleeting-creating"
)

func TestInstanceGroup_templateCloneOptions(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		isTemplate        bool
		configuredStorage string
		targetNode        string
		expectedFull      bool
		expectedErr       error
	}

	testCases := []testCase{
		{
			name:              "VM with unconfigured storage", // Error?
			isTemplate:        false,
			configuredStorage: "",
			expectedFull:      true,
			expectedErr:       ErrCloneVMWithoutConfiguredStorage,
		},
		{
			name:              "VM with configured storage",
			isTemplate:        false,
			configuredStorage: removalTestStorage,
			expectedFull:      true,
			expectedErr:       nil,
		},
		{
			name:              "Template with unconfigured storage",
			isTemplate:        true,
			configuredStorage: "",
			expectedFull:      false,
			expectedErr:       nil,
		},
		{
			name:              "Template with configured storage",
			isTemplate:        true,
			configuredStorage: removalTestStorage,
			expectedFull:      true,
			expectedErr:       nil,
		},
		{
			name:              "Template with configured target node",
			isTemplate:        true,
			configuredStorage: removalTestStorage,
			targetNode:        "pve-target",
			expectedFull:      true,
			expectedErr:       nil,
		},
		{
			name:              "VM with configured storage and target node",
			isTemplate:        false,
			configuredStorage: removalTestStorage,
			targetNode:        "pve-target",
			expectedFull:      true,
			expectedErr:       nil,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			template := proxmox.VirtualMachine{
				Template: proxmox.IsTemplate(testCase.isTemplate),
			}

			ig := InstanceGroup{
				Storage:            testCase.configuredStorage,
				InstanceTargetNode: testCase.targetNode,
			}

			result, err := ig.getTemplateCloneOptions(&template)
			require.ErrorIs(t, err, testCase.expectedErr)

			if err == nil {
				require.Equal(t, testCase.configuredStorage, result.Storage)
				require.Equal(t, testCase.targetNode, result.Target)
				require.Equal(t, testCase.expectedFull, bool(result.Full))
			}
		})
	}
}

// removalTestServer parameterises newRemovalTestGroup. The zero value is the default fake:
// one stale instance whose rename the API accepts and whose task then fails, and a logger
// that discards.
type removalTestServer struct {
	// log is the group's logger; nil means discard.
	log hclog.Logger

	// configResponse is the body the rename POST answers with; empty means a task UPID.
	configResponse string

	// members is the pool the fake serves; empty means the one stale instance above.
	members []removalTestMember

	// requests tallies the requests the fake served; nil means do not report them.
	requests *removalRequestCounts

	// failPaths makes the fake answer 500 to every request whose path contains one of the
	// substrings, so a test can fail one route of the fake without rebuilding it.
	failPaths []string

	// networkIfaceBodies, when non-empty, are served one per network-get-interfaces request
	// in order, the last one repeating; they let a test simulate an interface list that
	// changes while ConnectInfo retries.
	networkIfaceBodies []string

	networkIfaceCalls *atomic.Int64
}

// removalTestMember is one VM in the fake's pool. It appears in the pool listing and, under its
// own vmid, answers the status and config fetches and the rename, stop, delete, agent OS-info,
// resume, suspend and network-interfaces requests.
type removalTestMember struct {
	vmid uint64
	name string
	// memberType is the type the pool listing reports for the member; empty means qemu.
	memberType string
	// tags is the tag list the VM's config carries, so a test can give a VM template or
	// manually applied tags and assert on what a rename does to them.
	tags string
	// fetchedName, when set, is the name the VM's config carries (what a rename writes), so a
	// test can simulate a VM renamed since the pool was listed. status/current keeps serving
	// the listed name, which pins fetchedName's preference for the config.
	fetchedName string
	// fetchedNameAfterStop, when set, replaces fetchedName once the VM has been asked to stop,
	// so a test can simulate a VM renamed while the collector waited for the stop.
	fetchedNameAfterStop string
	// ifaces is the raw JSON array the VM's agent reports as its network interfaces; empty
	// means defaultRemovalTestIfaces.
	ifaces string
	// osinfoFails makes the VM's agent/get-osinfo answer 500, so a test can simulate a dead
	// QEMU agent.
	osinfoFails bool
	// stopFails makes the VM's stop task report a failure, so a test can simulate a stop
	// that never completed.
	stopFails bool
	// vmStatus is the status the VM's status/current reports; empty means running.
	vmStatus string
	// noDigest makes the VM's config report no digest, so a rename is sent without one.
	noDigest bool
	// deleteFails makes the VM's delete task report a failure, so a test can simulate a
	// delete that never completed.
	deleteFails bool
}

// defaultRemovalTestIfaces is what a fake member reports for network-get-interfaces unless a
// test gives it its own list: one interface holding a private and a global IPv4 address.
const defaultRemovalTestIfaces = `{"name":"eth0","hardware-address":"12:34:56:AB:CD:EF",` +
	`"ip-addresses":[{"ip-address-type":"ipv4","ip-address":"192.168.0.1","prefix":24},` +
	`{"ip-address-type":"ipv4","ip-address":"8.8.8.8","prefix":32}]}`

// removalRequestCounts records how often the fake was asked for each of the two things a
// rename can do, and every request it served, so a test can assert on what was *not*
// requested. It is safe for concurrent use because markInstancesForRemoval drives the fake from
// one goroutine per instance.
type removalRequestCounts struct {
	configPOSTs atomic.Int64
	taskPolls   atomic.Int64
	mu          sync.Mutex
	paths       []string         // method + " " + path for every request; guarded by mu
	renames     []map[string]any // body of every config POST; guarded by mu
}

// removalTestDigest is the config digest the fake serves for vmid.
func removalTestDigest(vmid uint64) string {
	return fmt.Sprintf("digest-%d", vmid)
}

// upidVMID reads the vmid out of a /tasks/<upid>/status path, so the fake can answer a task
// by the member its UPID names.
func upidVMID(path string) string {
	upid := strings.TrimSuffix(strings.TrimPrefix(path, "/nodes/pve-node/tasks/"), "/status")

	fields := strings.Split(upid, ":")
	if len(fields) < 7 {
		return ""
	}

	return fields[6]
}

// requestsFor returns the recorded request entries for /qemu/<vmid> itself (the route a VM
// DELETE uses) and every route below it, so a test can assert on exactly which operations
// were performed for a specific VM.
func (c *removalRequestCounts) requestsFor(vmid uint64) []string {
	vmPath := fmt.Sprintf("/qemu/%d", vmid)

	c.mu.Lock()
	defer c.mu.Unlock()

	var out []string
	for _, p := range c.paths {
		if strings.HasSuffix(p, vmPath) || strings.Contains(p, vmPath+"/") {
			out = append(out, p)
		}
	}

	return out
}

// requested reports whether any recorded request for vmid used method and a path containing
// substr; an empty method matches any.
func (c *removalRequestCounts) requested(vmid uint64, method, substr string) bool {
	return slices.ContainsFunc(c.requestsFor(vmid), func(p string) bool {
		return strings.HasPrefix(p, method) && strings.Contains(p, substr)
	})
}

// newLogBuffer returns a logger and the buffer it writes to, so a test can assert on what a
// call logged rather than only on what it returned.
//
//nolint:ireturn // hclog exposes no exported concrete logger type
func newLogBuffer(t *testing.T) (hclog.Logger, *bytes.Buffer) {
	t.Helper()

	buf := &bytes.Buffer{}

	return hclog.New(&hclog.LoggerOptions{Output: buf}), buf
}

// removalTestTaskStatus answers a task's /status route by the task type in its UPID: a stop
// or delete the member flags as failing reports that failure, everything else succeeds, and
// an unrecognised UPID is answered as the (failing) rename task.
func removalTestTaskStatus(t *testing.T, memberByVMID map[string]removalTestMember) http.HandlerFunc {
	t.Helper()

	renameTask := taskHandler(t, "qmconfig", taskStatusStopped, "VM is locked (clone)", "TASK ERROR: VM is locked (clone)")
	stopTask := taskHandler(t, "qmstop", taskStatusStopped, taskExitStatusOK, "")
	destroyTask := taskHandler(t, "qmdestroy", taskStatusStopped, taskExitStatusOK, "")

	return func(writer http.ResponseWriter, request *http.Request) {
		// Task.Ping replaces the whole task with the answer, so answering a stop with the
		// rename's status would turn it into one.
		switch {
		case strings.Contains(request.URL.Path, ":qmstop:"):
			if member, ok := memberByVMID[upidVMID(request.URL.Path)]; ok && member.stopFails {
				fmt.Fprint(writer, taskStatusBody("qmstop", taskStatusStopped, "TASK ERROR: VM is locked (clone)"))
			} else {
				stopTask(writer, request)
			}
		case strings.Contains(request.URL.Path, ":qmdestroy:"):
			if member, ok := memberByVMID[upidVMID(request.URL.Path)]; ok && member.deleteFails {
				fmt.Fprint(writer, taskStatusBody("qmdestroy", taskStatusStopped, "TASK ERROR: device write failed"))
			} else {
				destroyTask(writer, request)
			}
		default:
			renameTask(writer, request)
		}
	}
}

// removalTestFailPath reports whether the request hits one of the fake's injected failure
// paths.
func removalTestFailPath(request *http.Request, failPaths []string) bool {
	for _, failPath := range failPaths {
		if strings.Contains(request.URL.Path, failPath) {
			return true
		}
	}

	return false
}

// removalTestVMRoute resolves a request to the listed member and route it addresses; an
// unlisted or non-VM request resolves to the zero member and an empty route.
func removalTestVMRoute(request *http.Request, memberByVMID map[string]removalTestMember) (removalTestMember, string) {
	// Per-VM routes are "<method> <suffix>" of /nodes/pve-node/qemu/<vmid>[/<suffix>].
	rest, isVM := strings.CutPrefix(request.URL.Path, "/nodes/pve-node/qemu/")
	if !isVM {
		return removalTestMember{}, ""
	}

	vmid, suffix, _ := strings.Cut(rest, "/")

	member, ok := memberByVMID[vmid]
	if !ok {
		return removalTestMember{}, ""
	}

	return member, request.Method + " " + suffix
}

// removalTestServeStatus answers a VM's GET status/current route.
func removalTestServeStatus(writer http.ResponseWriter, matched removalTestMember) {
	status := matched.vmStatus
	if status == "" {
		status = "running"
	}

	fmt.Fprintf(writer, `{"data":{"vmid":%d,"name":%q,"status":%q}}`, matched.vmid, matched.name, status)
}

// removalTestServeConfig answers a VM's GET config route. An empty config name falls back to
// the status name, as an absent one would.
func removalTestServeConfig(writer http.ResponseWriter, matched removalTestMember, counts *removalRequestCounts) {
	name := matched.fetchedName
	if matched.fetchedNameAfterStop != "" && counts.requested(matched.vmid, http.MethodPost, "/status/stop") {
		name = matched.fetchedNameAfterStop
	}

	if matched.noDigest {
		fmt.Fprintf(writer, `{"data":{"name":%q,"tags":%q}}`, name, matched.tags)

		return
	}

	fmt.Fprintf(writer, `{"data":{"digest":%q,"name":%q,"tags":%q}}`, removalTestDigest(matched.vmid), name, matched.tags)
}

// removalTestServeRename answers a VM's POST config (rename) route, recording the body and
// answering with the configured response.
func removalTestServeRename(t *testing.T, writer http.ResponseWriter, counts *removalRequestCounts,
	request *http.Request, configResponse string,
) {
	t.Helper()

	counts.configPOSTs.Add(1)

	var body map[string]any

	err := json.NewDecoder(request.Body).Decode(&body)
	if err != nil {
		t.Errorf("config POST body: %v", err)
	}

	counts.mu.Lock()
	counts.renames = append(counts.renames, body)
	counts.mu.Unlock()

	fmt.Fprint(writer, configResponse)
}

// removalTestServeInterfaces answers a VM's GET agent/network-get-interfaces route, serving
// the member's interface list, or the test's scripted bodies when it has any.
func removalTestServeInterfaces(writer http.ResponseWriter, matched removalTestMember, opts removalTestServer) {
	ifaces := matched.ifaces
	if ifaces == "" {
		ifaces = defaultRemovalTestIfaces
	}

	if len(opts.networkIfaceBodies) > 0 {
		call := int(opts.networkIfaceCalls.Add(1)) - 1
		if call >= len(opts.networkIfaceBodies) {
			call = len(opts.networkIfaceBodies) - 1
		}

		ifaces = opts.networkIfaceBodies[call]
	}

	fmt.Fprintf(writer, `{"data":{"result":[%s]}}`, ifaces)
}

// removalTestHandler serves the fake Proxmox routes the removal tests drive: the pool, the
// node and VM status and config, the tasks, the rename, the stop, the delete, the agent and
// the network interfaces.
func removalTestHandler(t *testing.T, opts removalTestServer, counts *removalRequestCounts,
	poolBody string, memberByVMID map[string]removalTestMember,
) http.HandlerFunc {
	t.Helper()

	renameTask := taskHandler(t, "qmconfig", taskStatusStopped, "VM is locked (clone)", "TASK ERROR: VM is locked (clone)")
	taskStatus := removalTestTaskStatus(t, memberByVMID)

	configResponse := opts.configResponse
	if configResponse == "" {
		configResponse = fmt.Sprintf(`{"data":%q}`, testUPID("qmconfig"))
	}

	// upidByRoute answers the one-shot task routes with a fresh task of the named type.
	upidByRoute := map[string]string{
		"POST status/stop":    "qmstop",
		"DELETE ":             "qmdestroy",
		"POST status/resume":  "qmresume",
		"POST status/suspend": "qmsuspend",
	}

	return func(writer http.ResponseWriter, request *http.Request) {
		// A client built by getProxmoxClient prefixes every route with /api2/json, as does
		// real Proxmox; answer both spellings.
		request.URL.Path = strings.TrimPrefix(request.URL.Path, "/api2/json")

		counts.mu.Lock()
		counts.paths = append(counts.paths, request.Method+" "+request.URL.Path)
		counts.mu.Unlock()

		if removalTestFailPath(request, opts.failPaths) {
			http.Error(writer, "injected failure", http.StatusInternalServerError)

			return
		}

		matched, route := removalTestVMRoute(request, memberByVMID)
		upIDTaskType, isUpIDRoute := upidByRoute[route]

		switch {
		case strings.HasPrefix(request.URL.Path, removalTestPools):
			fmt.Fprint(writer, poolBody)
		case request.URL.Path == "/nodes/pve-node/status":
			fmt.Fprint(writer, `{"data":{}}`)
		case strings.Contains(request.URL.Path, "/tasks/") && strings.HasSuffix(request.URL.Path, "/status"):
			counts.taskPolls.Add(1)

			taskStatus(writer, request)
		case isUpIDRoute:
			fmt.Fprintf(writer, `{"data":%q}`, testUPID(upIDTaskType))
		case route == "GET status/current":
			removalTestServeStatus(writer, matched)
		case route == "GET config":
			removalTestServeConfig(writer, matched, counts)
		case route == "POST config":
			removalTestServeRename(t, writer, counts, request, configResponse)
		case route == "GET agent/get-osinfo":
			if matched.osinfoFails {
				http.Error(writer, "agent unavailable", http.StatusInternalServerError)

				return
			}

			fmt.Fprint(writer, `{"data":{"result":{}}}`)
		case route == "GET agent/network-get-interfaces":
			removalTestServeInterfaces(writer, matched, opts)
		default:
			renameTask(writer, request)
		}
	}
}

// newRemovalTestGroup wires an InstanceGroup to an httptest Proxmox serving opts.members, by
// default one stale instance. A mark-for-removal rename is accepted by the API but its task
// then fails; stop and destroy tasks succeed, so the collector can run to the end.
func newRemovalTestGroup(t *testing.T, opts removalTestServer) *InstanceGroup {
	t.Helper()

	log := opts.log
	if log == nil {
		log = hclog.NewNullLogger()
	}

	counts := opts.requests
	if counts == nil {
		counts = &removalRequestCounts{}
	}

	if opts.networkIfaceCalls == nil {
		opts.networkIfaceCalls = &atomic.Int64{}
	}

	members := opts.members
	if len(members) == 0 {
		members = []removalTestMember{{vmid: 100, name: DefaultInstanceNameCreating}}
	}

	poolMembers := make([]string, 0, len(members))
	memberByVMID := make(map[string]removalTestMember, len(members))

	for _, member := range members {
		memberType := member.memberType
		if memberType == "" {
			memberType = vmTypeQEMU
		}

		poolMembers = append(poolMembers,
			fmt.Sprintf(`{"vmid":%d,"type":%q,"name":%q,"node":"pve-node"}`, member.vmid, memberType, member.name))
		memberByVMID[strconv.FormatUint(member.vmid, 10)] = member
	}

	poolBody := fmt.Sprintf(`{"data":[{"poolid":"test-pool","members":[%s]}]}`, strings.Join(poolMembers, ","))

	server := httptest.NewServer(removalTestHandler(t, opts, counts, poolBody, memberByVMID))
	t.Cleanup(server.Close)

	templateID := 200

	ig := newWaitTestGroup()
	ig.Pool = removalTestPool
	ig.TemplateID = &templateID
	ig.InstanceNameCreating = DefaultInstanceNameCreating
	ig.InstanceNameRunning = DefaultInstanceNameRunning
	ig.InstanceNameRemoving = DefaultInstanceNameRemoving
	ig.InstanceTagsRemoving = DefaultInstanceNameRemoving
	ig.log = log
	ig.URL = server.URL
	ig.proxmox = proxmox.NewClient(server.URL)
	ig.instanceCollectionTrigger = make(chan struct{}, 1)

	return ig
}

// Decrease's path: a rename the API accepted whose task then fails must surface as an error,
// so the instance is not reported as successfully removed.
func TestMarkInstanceForRemovalReportsTaskFailure(t *testing.T) {
	t.Parallel()

	ig := newRemovalTestGroup(t, removalTestServer{})

	member := &proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: DefaultInstanceNameCreating, Node: removalTestNode}

	err := ig.markInstanceForRemoval(context.Background(), member)
	require.ErrorIs(t, err, ErrTaskFailed)
}

// Init's stale sweep is cleanup, not a precondition for serving: a leftover VM that cannot be
// marked for removal must not stop the plugin from starting, and the failure must be logged
// rather than swallowed.
func TestMarkStaleInstancesForRemovalToleratesTaskFailure(t *testing.T) {
	t.Parallel()

	log, logBuffer := newLogBuffer(t)
	ig := newRemovalTestGroup(t, removalTestServer{log: log})

	err := ig.markStaleInstancesForRemoval(context.Background())
	require.NoError(t, err)
	require.Regexp(t, `\[ERROR\].*continuing startup`, logBuffer.String())
}

// markInstanceForRemoval must refuse a VM whose fetched name differs from the listed name used
// to select it. The pool listing and the node fetch are not atomic; between them another
// manager may have renamed the VM. Acting on the stale listing would rename a VM we do not own.
func TestMarkInstanceForRemovalRefusesRenamedVM(t *testing.T) {
	t.Parallel()

	type row struct {
		name        string
		fetchedName string
		wantErr     error
		wantPOSTs   int64
	}

	rows := []row{
		{
			// Fetched name differs from listed name: refuse.
			name:        "renamed since listing",
			fetchedName: removalTestForeignCreating,
			wantErr:     ErrNotOwned,
		},
		{
			// Renamed to another of our own names: still not the VM that was selected.
			name:        "renamed to another own name",
			fetchedName: DefaultInstanceNameRunning,
			wantErr:     ErrNotOwned,
		},
		{
			// Already marked by an earlier attempt the listing has not caught up with: done.
			name:        "already marked for removal",
			fetchedName: DefaultInstanceNameRemoving,
		},
		{
			// No override: fetched name matches listed name; the task fails as usual.
			name:      "control: no rename, task fails",
			wantErr:   ErrTaskFailed,
			wantPOSTs: 1,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()

			log, logBuf := newLogBuffer(t)
			counts := &removalRequestCounts{}
			ig := newRemovalTestGroup(t, removalTestServer{
				log: log,
				members: []removalTestMember{
					{vmid: 100, name: DefaultInstanceNameCreating, fetchedName: row.fetchedName},
				},
				requests: counts,
			})

			member := &proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: DefaultInstanceNameCreating, Node: removalTestNode}

			err := ig.markInstanceForRemoval(context.Background(), member)
			require.ErrorIs(t, err, row.wantErr)
			require.Equal(t, row.wantPOSTs, counts.configPOSTs.Load(), "config POST count")

			if errors.Is(row.wantErr, ErrNotOwned) {
				require.Regexp(t, `\[WARN\]`, logBuf.String(), "expected a Warn for the renamed VM")
				require.Contains(t, err.Error(), fmt.Sprintf("vmid='100' is named %q", row.fetchedName))
			}
		})
	}
}

// The rename must carry the digest of the config whose name getListedVM checked, so Proxmox
// refuses it if another manager changed the VM between the check and the rename.
func TestMarkInstanceForRemovalSendsDigest(t *testing.T) {
	t.Parallel()

	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{requests: counts})

	member := &proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: DefaultInstanceNameCreating, Node: removalTestNode}

	err := ig.markInstanceForRemoval(context.Background(), member)
	require.ErrorIs(t, err, ErrTaskFailed)
	require.Len(t, counts.renames, 1)
	require.Equal(t, map[string]any{
		vmOptName:   DefaultInstanceNameRemoving,
		vmOptTags:   DefaultInstanceNameRemoving,
		vmOptDigest: removalTestDigest(100),
	}, counts.renames[0])
}

// The rename is the only evidence the collector will ever see that an instance is to be
// removed. A rename Proxmox accepts without answering with a task was never observed, so it
// must be reported as an error rather than as a completed removal.
func TestMarkInstanceForRemovalRejectsMissingTask(t *testing.T) {
	t.Parallel()

	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		configResponse: `{"data":null}`,
		requests:       counts,
	})

	member := &proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: DefaultInstanceNameCreating, Node: removalTestNode}

	err := ig.markInstanceForRemoval(context.Background(), member)
	require.ErrorIs(t, err, ErrNoTask)
	require.Equal(t, int64(1), counts.configPOSTs.Load(), "expected exactly one rename POST")
	require.Zero(t, counts.taskPolls.Load(), "expected the rename never to be waited on")
}

func TestInstanceGroup_mergedInstanceTags(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name      string
		creating  string
		running   string
		removing  string
		current   string
		stateTags string
		expected  string
	}

	testCases := []testCase{
		{
			name:      "no existing tags and no state tags leave the VM untagged",
			current:   "",
			stateTags: "",
			expected:  "",
		},
		{
			name:      "state tags are added to an untagged VM",
			creating:  DefaultInstanceNameCreating,
			stateTags: DefaultInstanceNameCreating,
			expected:  DefaultInstanceNameCreating,
		},
		{
			name:      "existing tags are preserved and the state tags are added",
			creating:  DefaultInstanceNameCreating,
			current:   "template-tag,manual-tag",
			stateTags: DefaultInstanceNameCreating,
			expected:  removalTestTemplateTagsCreating,
		},
		{
			name:      "leaving a state removes that state's tags and adds the target's",
			creating:  DefaultInstanceNameCreating,
			running:   DefaultInstanceNameRunning,
			current:   removalTestTagsCreating,
			stateTags: DefaultInstanceNameRunning,
			expected:  "template-tag,fleeting-running",
		},
		{
			name:      "tags shared by two states are kept in both",
			creating:  "keep-me,fleeting-creating",
			running:   "keep-me,fleeting-running",
			current:   "template-tag,keep-me,fleeting-creating",
			stateTags: "keep-me,fleeting-running",
			expected:  "template-tag,keep-me,fleeting-running",
		},
		{
			name:      "a state tag already on the VM is not duplicated",
			creating:  DefaultInstanceNameCreating,
			current:   removalTestTagsCreating,
			stateTags: DefaultInstanceNameCreating,
			expected:  removalTestTagsCreating,
		},
		{
			name:      "an unconfigured state removes the managed tags it finds",
			creating:  DefaultInstanceNameCreating,
			current:   removalTestTagsCreating,
			stateTags: "",
			expected:  "template-tag",
		},
		{
			name:      "state tags are split on semicolons",
			creating:  removalTestSemicolonTags,
			current:   "template-tag",
			stateTags: removalTestSemicolonTags,
			expected:  "template-tag,a,b",
		},
		{
			name:      "existing tags are split on commas and whitespace is dropped",
			creating:  DefaultInstanceNameCreating,
			current:   " template-tag , manual-tag ",
			stateTags: DefaultInstanceNameCreating,
			expected:  removalTestTemplateTagsCreating,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			ig := InstanceGroup{
				InstanceTagsCreating: testCase.creating,
				InstanceTagsRunning:  testCase.running,
				InstanceTagsRemoving: testCase.removing,
			}

			require.Equal(t, testCase.expected, ig.mergedInstanceTags(testCase.current, testCase.stateTags))
		})
	}
}

// The removal rename must preserve the tags the group does not manage: the instance's
// creating tags are replaced by the removing tags while the template's and manually applied
// tags stay.
func TestMarkInstanceForRemovalPreservesExistingTags(t *testing.T) {
	t.Parallel()

	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{
			{vmid: 100, name: DefaultInstanceNameCreating, tags: removalTestTemplateTagsCreating},
		},
		requests: counts,
	})
	ig.InstanceTagsCreating = DefaultInstanceNameCreating

	member := &proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: DefaultInstanceNameCreating, Node: removalTestNode}

	err := ig.markInstanceForRemoval(context.Background(), member)
	require.ErrorIs(t, err, ErrTaskFailed)
	require.Len(t, counts.renames, 1)
	require.Equal(t, map[string]any{
		vmOptName:   DefaultInstanceNameRemoving,
		vmOptTags:   "template-tag,manual-tag,fleeting-removing",
		vmOptDigest: removalTestDigest(100),
	}, counts.renames[0])
}

// The stale sweep marks only VMs still listed under InstanceNameCreating; running and
// removing members are left alone, and a pool that cannot be listed fails the sweep.
func TestMarkStaleInstancesForRemoval(t *testing.T) {
	t.Parallel()

	t.Run("no stale instances means no rename", func(t *testing.T) {
		t.Parallel()

		counts := &removalRequestCounts{}
		ig := newRemovalTestGroup(t, removalTestServer{
			members: []removalTestMember{
				{vmid: 100, name: DefaultInstanceNameRunning},
				{vmid: 101, name: DefaultInstanceNameRemoving},
			},
			requests: counts,
		})

		err := ig.markStaleInstancesForRemoval(context.Background())
		require.NoError(t, err)
		require.Zero(t, counts.configPOSTs.Load())
	})

	t.Run("only creating members are marked", func(t *testing.T) {
		t.Parallel()

		log, _ := newLogBuffer(t)
		counts := &removalRequestCounts{}
		ig := newRemovalTestGroup(t, removalTestServer{
			log: log,
			members: []removalTestMember{
				{vmid: 100, name: DefaultInstanceNameCreating},
				{vmid: 101, name: DefaultInstanceNameRunning},
			},
			requests: counts,
		})

		err := ig.markStaleInstancesForRemoval(context.Background())

		// The rename task fails by default; the sweep must swallow that and keep starting.
		require.NoError(t, err)
		require.Equal(t, int64(1), counts.configPOSTs.Load())
		require.False(t, counts.requested(101, http.MethodPost, ""), "a running instance was renamed: %v", counts.requestsFor(101))
	})

	t.Run("pool fetch failure fails the sweep", func(t *testing.T) {
		t.Parallel()

		ig := newRemovalTestGroup(t, removalTestServer{failPaths: []string{removalTestPools}})

		err := ig.markStaleInstancesForRemoval(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get pool")
	})
}

// A clone of a non-template VM without configured storage is refused before anything is
// asked of Proxmox.
func TestCloneTemplateReportsCloneOptionsError(t *testing.T) {
	t.Parallel()

	ig := newWaitTestGroup()

	template := &proxmox.VirtualMachine{}

	vmid, task, err := ig.cloneTemplate(context.Background(), template, new(sync.Mutex))
	require.ErrorIs(t, err, ErrCloneVMWithoutConfiguredStorage)
	require.Equal(t, -1, vmid)
	require.Nil(t, task)
}

// The digest is sent only when the fetch carried one: it is the proof that the config has
// not changed since the name was checked, and an absent digest has nothing to prove.
func TestMarkInstanceForRemovalOmitsAbsentDigest(t *testing.T) {
	t.Parallel()

	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{
			{vmid: 100, name: DefaultInstanceNameCreating, noDigest: true},
		},
		requests: counts,
	})

	member := &proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: DefaultInstanceNameCreating, Node: removalTestNode}

	err := ig.markInstanceForRemoval(context.Background(), member)
	require.ErrorIs(t, err, ErrTaskFailed)
	require.Len(t, counts.renames, 1)
	require.Equal(t, map[string]any{
		vmOptName: DefaultInstanceNameRemoving,
		vmOptTags: DefaultInstanceNameRemoving,
	}, counts.renames[0])
}

func TestIsProxmoxResourceAnInstance(t *testing.T) {
	t.Parallel()

	templateID := 200

	ig := &InstanceGroup{TemplateID: &templateID}

	tests := []struct {
		name   string
		member proxmox.ClusterResource
		want   bool
	}{
		{name: "a qemu member is an instance", member: proxmox.ClusterResource{Type: vmTypeQEMU, VMID: 100}, want: true},
		{name: "the template is not an instance", member: proxmox.ClusterResource{Type: vmTypeQEMU, VMID: 200}},
		{name: "a non-qemu member is not an instance", member: proxmox.ClusterResource{Type: removalTestMemberTypeLXC, VMID: 100}},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, testCase.want, ig.isProxmoxResourceAnInstance(testCase.member))
		})
	}
}

func TestParseTagList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  []string
	}{
		{name: "empty", value: ""},
		{name: "single tag", value: "a", want: []string{"a"}},
		{name: "semicolon delimited", value: removalTestSemicolonTags, want: []string{"a", "b"}},
		{name: "comma delimited", value: "a,b", want: []string{"a", "b"}},
		{name: "mixed delimiters and whitespace", value: " a;b, c ", want: []string{"a", "b", "c"}},
		{name: "consecutive and leading separators", value: ";;a,,b", want: []string{"a", "b"}},
		{name: "only separators", value: " ; , "},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			parsed := parseTagList(testCase.value)

			// The empty result may be a nil or an empty slice depending on the Go version.
			if len(testCase.want) == 0 {
				require.Empty(t, parsed)

				return
			}

			require.Equal(t, testCase.want, parsed)
		})
	}
}
