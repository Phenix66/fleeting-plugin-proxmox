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

func TestInstanceGroup_templateCloneOptions(t *testing.T) {
	type testCase struct {
		name              string
		isTemplate        bool
		configuredStorage string
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
			configuredStorage: "local",
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
			configuredStorage: "local",
			expectedFull:      true,
			expectedErr:       nil,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			template := proxmox.VirtualMachine{
				Template: proxmox.IsTemplate(testCase.isTemplate),
			}

			ig := InstanceGroup{
				Settings: Settings{
					Storage: testCase.configuredStorage,
				},
			}

			result, err := ig.getTemplateCloneOptions(&template)
			require.ErrorIs(t, err, testCase.expectedErr)

			if err == nil {
				require.Equal(t, testCase.configuredStorage, result.Storage)
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
}

// removalTestMember is one VM in the fake's pool. It appears in the pool listing and, under its
// own vmid, answers the status and config fetches and the rename, stop, delete, agent OS-info,
// resume and suspend requests.
type removalTestMember struct {
	vmid uint64
	name string
	// fetchedName, when set, is the name the VM's config carries (what a rename writes), so a
	// test can simulate a VM renamed since the pool was listed. status/current keeps serving
	// the listed name, which pins fetchedName's preference for the config.
	fetchedName string
	// fetchedNameAfterStop, when set, replaces fetchedName once the VM has been asked to stop,
	// so a test can simulate a VM renamed while the collector waited for the stop.
	fetchedNameAfterStop string
}

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
func newLogBuffer(t *testing.T) (hclog.Logger, *bytes.Buffer) {
	t.Helper()

	buf := &bytes.Buffer{}

	return hclog.New(&hclog.LoggerOptions{Output: buf}), buf
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

	renameUPID := testUPID("qmconfig")
	renameTask := taskHandler(t, "qmconfig", taskStatusStopped, "VM is locked (clone)", "TASK ERROR: VM is locked (clone)")
	stopTask := taskHandler(t, "qmstop", taskStatusStopped, taskExitStatusOK, "")
	destroyTask := taskHandler(t, "qmdestroy", taskStatusStopped, taskExitStatusOK, "")

	configResponse := opts.configResponse
	if configResponse == "" {
		configResponse = fmt.Sprintf(`{"data":%q}`, renameUPID)
	}

	members := opts.members
	if len(members) == 0 {
		members = []removalTestMember{{vmid: 100, name: "fleeting-creating"}}
	}

	poolMembers := make([]string, 0, len(members))
	memberByVMID := make(map[string]removalTestMember, len(members))

	for _, member := range members {
		poolMembers = append(poolMembers,
			fmt.Sprintf(`{"vmid":%d,"type":"qemu","name":%q,"node":"pve-node"}`, member.vmid, member.name))
		memberByVMID[strconv.FormatUint(member.vmid, 10)] = member
	}

	poolBody := fmt.Sprintf(`{"data":[{"poolid":"test-pool","members":[%s]}]}`, strings.Join(poolMembers, ","))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counts.mu.Lock()
		counts.paths = append(counts.paths, r.Method+" "+r.URL.Path)
		counts.mu.Unlock()

		// Per-VM routes are "<method> <suffix>" of /nodes/pve-node/qemu/<vmid>[/<suffix>], set
		// only for a listed vmid.
		var (
			m     removalTestMember
			route string
		)

		if rest, isVM := strings.CutPrefix(r.URL.Path, "/nodes/pve-node/qemu/"); isVM {
			vmid, suffix, _ := strings.Cut(rest, "/")
			if member, ok := memberByVMID[vmid]; ok {
				m, route = member, r.Method+" "+suffix
			}
		}

		switch {
		case strings.HasPrefix(r.URL.Path, "/pools"):
			fmt.Fprint(w, poolBody)
		case r.URL.Path == "/nodes/pve-node/status":
			fmt.Fprint(w, `{"data":{}}`)
		case strings.Contains(r.URL.Path, "/tasks/") && strings.HasSuffix(r.URL.Path, "/status"):
			counts.taskPolls.Add(1)

			// Answer each task by the type in its UPID: Task.Ping replaces the whole task with
			// the answer, so answering a stop with the rename's status would turn it into one.
			switch {
			case strings.Contains(r.URL.Path, ":qmstop:"):
				stopTask(w, r)
			case strings.Contains(r.URL.Path, ":qmdestroy:"):
				destroyTask(w, r)
			default:
				renameTask(w, r)
			}
		case route == "GET status/current":
			fmt.Fprintf(w, `{"data":{"vmid":%d,"name":%q,"status":"running"}}`, m.vmid, m.name)
		case route == "GET config":
			// An empty config name falls back to the status name, as an absent one would.
			name := m.fetchedName
			if m.fetchedNameAfterStop != "" && counts.requested(m.vmid, http.MethodPost, "/status/stop") {
				name = m.fetchedNameAfterStop
			}

			fmt.Fprintf(w, `{"data":{"digest":%q,"name":%q}}`, removalTestDigest(m.vmid), name)
		case route == "POST config":
			counts.configPOSTs.Add(1)

			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("config POST body: %v", err)
			}

			counts.mu.Lock()
			counts.renames = append(counts.renames, body)
			counts.mu.Unlock()

			fmt.Fprint(w, configResponse)
		case route == "POST status/stop":
			fmt.Fprintf(w, `{"data":%q}`, testUPID("qmstop"))
		case route == "DELETE ":
			fmt.Fprintf(w, `{"data":%q}`, testUPID("qmdestroy"))
		case route == "GET agent/get-osinfo":
			fmt.Fprint(w, `{"data":{"result":{}}}`)
		case route == "POST status/resume":
			fmt.Fprintf(w, `{"data":%q}`, testUPID("qmresume"))
		case route == "POST status/suspend":
			fmt.Fprintf(w, `{"data":%q}`, testUPID("qmsuspend"))
		default:
			renameTask(w, r)
		}
	}))
	t.Cleanup(server.Close)

	templateID := 200

	ig := newWaitTestGroup()
	ig.Pool = "test-pool"
	ig.TemplateID = &templateID
	ig.InstanceNameCreating = "fleeting-creating"
	ig.InstanceNameRunning = "fleeting-running"
	ig.InstanceNameRemoving = "fleeting-removing"
	ig.InstanceTagsRemoving = "fleeting-removing"
	ig.log = log
	ig.proxmox = proxmox.NewClient(server.URL)
	ig.instanceCollectionTrigger = make(chan struct{}, 1)

	return ig
}

// Decrease's path: a rename the API accepted whose task then fails must surface as an error,
// so the instance is not reported as successfully removed.
func TestMarkInstanceForRemovalReportsTaskFailure(t *testing.T) {
	ig := newRemovalTestGroup(t, removalTestServer{})

	member := &proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: "fleeting-creating", Node: "pve-node"}

	err := ig.markInstanceForRemoval(context.Background(), member)
	require.ErrorIs(t, err, ErrTaskFailed)
}

// Init's stale sweep is cleanup, not a precondition for serving: a leftover VM that cannot be
// marked for removal must not stop the plugin from starting, and the failure must be logged
// rather than swallowed.
func TestMarkStaleInstancesForRemovalToleratesTaskFailure(t *testing.T) {
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
			fetchedName: "other-creating",
			wantErr:     ErrNotOwned,
		},
		{
			// Renamed to another of our own names: still not the VM that was selected.
			name:        "renamed to another own name",
			fetchedName: "fleeting-running",
			wantErr:     ErrNotOwned,
		},
		{
			// Already marked by an earlier attempt the listing has not caught up with: done.
			name:        "already marked for removal",
			fetchedName: "fleeting-removing",
		},
		{
			// No override: fetched name matches listed name; the task fails as usual.
			name:      "control: no rename, task fails",
			wantErr:   ErrTaskFailed,
			wantPOSTs: 1,
		},
	}

	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			log, logBuf := newLogBuffer(t)
			counts := &removalRequestCounts{}
			ig := newRemovalTestGroup(t, removalTestServer{
				log: log,
				members: []removalTestMember{
					{vmid: 100, name: "fleeting-creating", fetchedName: tc.fetchedName},
				},
				requests: counts,
			})

			member := &proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: "fleeting-creating", Node: "pve-node"}

			err := ig.markInstanceForRemoval(context.Background(), member)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantPOSTs, counts.configPOSTs.Load(), "config POST count")

			if errors.Is(tc.wantErr, ErrNotOwned) {
				require.Regexp(t, `\[WARN\]`, logBuf.String(), "expected a Warn for the renamed VM")
				require.Contains(t, err.Error(), fmt.Sprintf("vmid='100' is named %q", tc.fetchedName))
			}
		})
	}
}

// The rename must carry the digest of the config whose name getListedVM checked, so Proxmox
// refuses it if another manager changed the VM between the check and the rename.
func TestMarkInstanceForRemovalSendsDigest(t *testing.T) {
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{requests: counts})

	member := &proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: "fleeting-creating", Node: "pve-node"}

	err := ig.markInstanceForRemoval(context.Background(), member)
	require.ErrorIs(t, err, ErrTaskFailed)
	require.Len(t, counts.renames, 1)
	require.Equal(t, map[string]any{
		vmOptName:   "fleeting-removing",
		vmOptTags:   "fleeting-removing",
		vmOptDigest: removalTestDigest(100),
	}, counts.renames[0])
}

// The rename is the only evidence the collector will ever see that an instance is to be
// removed. A rename Proxmox accepts without answering with a task was never observed, so it
// must be reported as an error rather than as a completed removal.
func TestMarkInstanceForRemovalRejectsMissingTask(t *testing.T) {
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		configResponse: `{"data":null}`,
		requests:       counts,
	})

	member := &proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: "fleeting-creating", Node: "pve-node"}

	err := ig.markInstanceForRemoval(context.Background(), member)
	require.ErrorIs(t, err, ErrNoTask)
	require.Equal(t, int64(1), counts.configPOSTs.Load(), "expected exactly one rename POST")
	require.Zero(t, counts.taskPolls.Load(), "expected the rename never to be waited on")
}
