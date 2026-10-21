package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
)

func TestUnmarshallingPluginSettings(t *testing.T) {
	t.Parallel()

	settingsJSON := `{"url":"sample_url","template_id": 5,"instance_target_node":"pve-target"}`
	instance := InstanceGroup{}

	err := json.Unmarshal([]byte(settingsJSON), &instance)
	require.NoError(t, err)

	require.Equal(t, "sample_url", instance.URL)
	require.Equal(t, 5, *instance.TemplateID)
	require.Equal(t, "pve-target", instance.InstanceTargetNode)
}

func TestShutdownIsIdempotent(t *testing.T) {
	t.Parallel()

	ig := &InstanceGroup{
		collectorShutdownTrigger:              make(chan struct{}),
		sessionTicketRefresherShutdownTrigger: make(chan struct{}),
	}

	ig.collectorWaitGroup.Go(func() {
		<-ig.collectorShutdownTrigger
	})

	ig.sessionTicketRefresherWaitGroup.Go(func() {
		<-ig.sessionTicketRefresherShutdownTrigger
	})

	// Shutdown must return every time, not just the first.
	for attempt := range 3 {
		done := make(chan error, 1)

		go func() {
			done <- ig.Shutdown(context.Background())
		}()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatalf("Shutdown call #%d did not return: possible deadlock", attempt+1)
		}
	}
}

func TestShutdownWithoutInitDoesNotPanic(t *testing.T) {
	t.Parallel()

	// A group whose Init never ran still has nil trigger channels, and closing a nil channel
	// panics.
	ig := &InstanceGroup{}

	require.NotPanics(t, func() {
		require.NoError(t, ig.Shutdown(context.Background()))
	})
}

func TestShutdownAfterInitCycleDoesNotHang(t *testing.T) {
	t.Parallel()

	ig := &InstanceGroup{}

	// A Shutdown before the first Init has nothing to close, and resetLifecycle re-arms the
	// guard on every Init, so what that first call does with the guard cannot strand a later
	// cycle. The requirement is only that every Shutdown here returns.
	requireShutdownReturns(t, ig, "before the first Init")

	for cycle := range 2 {
		ig.resetLifecycle()
		startFakeWorkers(ig)
		requireShutdownReturns(t, ig, fmt.Sprintf("after Init cycle #%d", cycle+1))
	}
}

// An Init without a Shutdown in between must not leak the previous cycle's workers: the new
// lifecycle stops them before handing out fresh channels, and the next Shutdown then drains
// only the current cycle.
func TestInitWithoutShutdownStopsPreviousWorkers(t *testing.T) {
	t.Parallel()

	ig := &InstanceGroup{}

	// Cycle one: a collector and a refresher that capture their triggers the way the real
	// workers' selects do on entry, and report when they exit.
	ig.resetLifecycle()

	ready := make(chan struct{}, 2)
	cycleOneCollectorDone := make(chan struct{})
	cycleOneRefresherDone := make(chan struct{})

	ig.collectorWaitGroup.Go(func() {
		trigger := ig.collectorShutdownTrigger

		ready <- struct{}{}

		defer close(cycleOneCollectorDone)

		<-trigger
	})

	ig.sessionTicketRefresherWaitGroup.Go(func() {
		trigger := ig.sessionTicketRefresherShutdownTrigger

		ready <- struct{}{}

		defer close(cycleOneRefresherDone)

		<-trigger
	})

	<-ready
	<-ready

	// Cycle two, with no Shutdown in between: the previous workers must be stopped before
	// their channels go away.
	ig.resetLifecycle()

	select {
	case <-cycleOneCollectorDone:
	case <-time.After(time.Second):
		t.Fatal("the previous collector was not stopped by the second reset")
	}

	select {
	case <-cycleOneRefresherDone:
	case <-time.After(time.Second):
		t.Fatal("the previous ticket refresher was not stopped by the second reset")
	}

	// The wait groups now carry only cycle two, so a fresh cycle shuts down cleanly.
	startFakeWorkers(ig)

	requireShutdownReturns(t, ig, "after a second reset without a Shutdown")
}

// requireShutdownReturns fails the test instead of letting the package time out when Shutdown
// blocks -- a Shutdown that waits on workers nothing ever told to stop never returns at all.
func requireShutdownReturns(t *testing.T, ig *InstanceGroup, when string) {
	t.Helper()

	done := make(chan error, 1)

	go func() {
		done <- ig.Shutdown(context.Background())
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatalf("Shutdown %s did not return: possible deadlock", when)
	}
}

// startFakeWorkers stands in for the collector and the session ticket refresher: each returns
// once its shutdown trigger is closed, which is all that Shutdown's waits depend on.
func startFakeWorkers(ig *InstanceGroup) {
	ig.collectorWaitGroup.Go(func() {
		<-ig.collectorShutdownTrigger
	})

	ig.sessionTicketRefresherWaitGroup.Go(func() {
		<-ig.sessionTicketRefresherShutdownTrigger
	})
}

// Decrease counts instances that were already being removed as successes, and reports another
// manager's VM as removed without touching it, but both are bystanders: nothing was attempted
// for them. A batch in which every rename Decrease actually attempted failed is still a failed
// batch, and a bystander must not report it as a success.
func TestDecreaseReportsOnlyAttemptedSuccesses(t *testing.T) {
	t.Parallel()

	for _, bystander := range []string{DefaultInstanceNameRemoving, foreignRunning} {
		t.Run(bystander, func(t *testing.T) {
			t.Parallel()

			counts := &removalRequestCounts{}
			ig := newRemovalTestGroup(t, removalTestServer{
				members: []removalTestMember{
					{vmid: 100, name: DefaultInstanceNameRunning},
					{vmid: 101, name: bystander},
				},
				requests: counts,
			})

			// 100 is the only one attempted -- its rename is accepted and its task then fails --
			// while 101 is counted without a request being made for it.
			succeeded, err := ig.Decrease(context.Background(), []string{"100", "101"})
			require.Equal(t, []string{"101"}, succeeded)
			require.ErrorIs(t, err, ErrTaskFailed)
			require.False(t, counts.requested(101, http.MethodPost, ""), "bystander got a request: %v", counts.requestsFor(101))

			// Control: the same failing rename with no bystander to count. The error was always
			// reported here, so the bystander is the only difference between the two calls.
			succeeded, err = ig.Decrease(context.Background(), []string{"100"})
			require.Empty(t, succeeded)
			require.ErrorIs(t, err, ErrTaskFailed)
		})
	}
}

// A Decrease whose targets are all past the running state has nothing to rename, and a
// rename is the only thing that would give the collector work, so it must not wake the
// collector: the wake costs a trigger wait and a pool listing that would find nothing.
func TestDecreaseWithoutRemovalsDoesNotWakeCollector(t *testing.T) {
	t.Parallel()

	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{
			{vmid: 100, name: DefaultInstanceNameCreating},
			{vmid: 101, name: DefaultInstanceNameRemoving},
		},
	})

	succeeded, err := ig.Decrease(context.Background(), []string{"100", "101"})
	require.NoError(t, err)
	require.Equal(t, []string{"101"}, succeeded)
	require.Empty(t, ig.instanceCollectionTrigger, "a Decrease with nothing to remove woke the collector")

	// Control: a target that is still running gets a rename attempted, and an attempted
	// batch wakes the collector even though the rename's task then fails.
	ig = newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{
			{vmid: 100, name: DefaultInstanceNameRunning},
		},
	})

	succeeded, err = ig.Decrease(context.Background(), []string{"100"})
	require.Empty(t, succeeded)
	require.ErrorIs(t, err, ErrTaskFailed)
	require.Len(t, ig.instanceCollectionTrigger, 1, "an attempted rename did not wake the collector")
}

const (
	// increaseTestNode is the one node the Increase fake serves: it appears in the pool
	// listing, in every VM route and inside every task UPID.
	increaseTestNode = "pve-node"

	// increaseTestTemplateID is the VM the fake reports as a template, and so the only vmid
	// whose clone route exists.
	increaseTestTemplateID = 200

	// increaseTestTemplateTags are the tags the fake's template VM carries, so a deploy
	// starts from a VM that already has tags.
	increaseTestTemplateTags = "template-tag,manual-tag"

	// foreignRunning is a name another manager uses for one of the fake's VMs.
	foreignRunning = "other-running"

	// increaseTestPool and increaseTestStorage are the pool and storage the fake serves.
	increaseTestPool    = "test-pool"
	increaseTestStorage = "local"

	// increaseTestInvalidID is an instance id no VM carries, so the RPCs must refuse it.
	increaseTestInvalidID = "not-a-vmid"

	// increaseTestTemplate and increaseTestMemberTypeLXC name the bystanders Update must
	// skip: the template itself and a pool member that is not a VM at all.
	increaseTestTemplate      = "template"
	increaseTestMemberTypeLXC = "lxc"

	// increaseTestPools and increaseTestStatusCurrent are the routes a test can fail to make
	// a pool or status fetch report an error.
	increaseTestPools         = "/pools"
	increaseTestStatusCurrent = "/status/current"

	// increaseTestTagsCreating is the creating tags on a deploy that started from a tagged
	// template.
	increaseTestTagsCreating = "template-tag,manual-tag,fleeting-creating"
)

// increaseRequestCounts records the body of every config POST the fake served, per vmid, so a
// test can assert on the tags a deploy wrote at each step. It is safe for concurrent use
// because Increase deploys its instances in parallel.
type increaseRequestCounts struct {
	mu     sync.Mutex
	byVMID map[int][]map[string]any
}

func (c *increaseRequestCounts) configsFor(vmid int) []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.byVMID[vmid]
}

// increaseTestCloneVMIDs are the vmids /cluster/nextid hands out, in order. cloneTemplate
// serialises the nextid/clone pair under the cloning mutex, so the first of these is always
// the one whose clone POST is refused and the second is always the one that deploys.
func increaseTestCloneVMIDs() []int {
	return []int{100, 101}
}

// increaseTestUPID builds the UPID of a task of the given type against a vmid on the fake
// node. Task.Ping reads the node, the type and the id back out of the UPID, so the tasks route
// can answer for any task the fake handed out without having recorded it.
func increaseTestUPID(taskType string, vmid int) string {
	return fmt.Sprintf("UPID:%s:00001A2B:00000000:00000000:%s:%d:root@pam:", increaseTestNode, taskType, vmid)
}

// newIncreaseTestGroup wires an InstanceGroup to an httptest Proxmox serving the whole deploy
// path -- nextid, clone, task, config, start, agent -- for the template and for the VMs cloned
// from it, and answers the FIRST clone POST with a 500. That is the smallest way to make one
// instance of a batch fail while the rest come up: Proxmox never accepted the clone, so there
// is no task and no VM behind it. A ServeMux rather than removalTestServer's switch because
// Increase drives eleven distinct routes and tells three of them apart only by method.
func newIncreaseTestGroup(t *testing.T, log hclog.Logger) (*InstanceGroup, *increaseRequestCounts) {
	t.Helper()

	var (
		vmidsHandedOut atomic.Int64
		clonesSeen     atomic.Int64
	)

	counts := &increaseRequestCounts{byVMID: make(map[int][]map[string]any)}

	poolMembers := make([]string, 0, 1+len(increaseTestCloneVMIDs()))
	poolMembers = append(poolMembers,
		fmt.Sprintf(`{"vmid":%d,"type":"qemu","name":"template","node":%q}`, increaseTestTemplateID, increaseTestNode))

	for _, vmid := range increaseTestCloneVMIDs() {
		poolMembers = append(poolMembers,
			fmt.Sprintf(`{"vmid":%d,"type":"qemu","name":"fleeting-creating","node":%q}`, vmid, increaseTestNode))
	}

	poolBody := fmt.Sprintf(`{"data":[{"poolid":"test-pool","members":[%s]}]}`, strings.Join(poolMembers, ","))

	vmRoute := func(suffix string) string {
		return fmt.Sprintf("/nodes/%s/qemu/{vmid}/%s", increaseTestNode, suffix)
	}

	templateRoute := func(suffix string) string {
		return fmt.Sprintf("/nodes/%s/qemu/%d/%s", increaseTestNode, increaseTestTemplateID, suffix)
	}

	constant := func(body string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /pools/", constant(poolBody))
	mux.HandleFunc("GET /cluster/status", constant(`{"data":[]}`))
	mux.HandleFunc("GET /nodes/"+increaseTestNode+"/status", constant(`{"data":{}}`))

	// The template is the one VM that must report itself as a template, or the clone is
	// refused before it reaches the API.
	mux.HandleFunc("GET "+templateRoute("status/current"),
		constant(fmt.Sprintf(`{"data":{"vmid":%d,"name":"template","status":"stopped","template":1}}`, increaseTestTemplateID)))

	mux.HandleFunc("GET "+vmRoute("status/current"), func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"data":{"vmid":%s,"name":"fleeting-creating","status":"running"}}`, r.PathValue("vmid"))
	})

	mux.HandleFunc("GET "+vmRoute("config"),
		constant(fmt.Sprintf(`{"data":{"tags":%q}}`, increaseTestTemplateTags)))
	mux.HandleFunc("GET "+vmRoute("agent/get-osinfo"), constant(`{"data":{"result":{}}}`))

	mux.HandleFunc("POST "+vmRoute("config"), func(writer http.ResponseWriter, r *http.Request) {
		vmid := increaseTestVMID(t, r)
		body := map[string]any{}

		err := json.NewDecoder(r.Body).Decode(&body)
		if err != nil {
			t.Errorf("config POST body: %v", err)
		}

		counts.mu.Lock()
		counts.byVMID[vmid] = append(counts.byVMID[vmid], body)
		counts.mu.Unlock()

		fmt.Fprintf(writer, `{"data":%q}`, increaseTestUPID("qmconfig", vmid))
	})

	mux.HandleFunc("POST "+vmRoute("status/start"), func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"data":%q}`, increaseTestUPID("qmstart", increaseTestVMID(t, r)))
	})

	mux.HandleFunc("GET /cluster/nextid", func(writer http.ResponseWriter, _ *http.Request) {
		vmids := increaseTestCloneVMIDs()

		index := int(vmidsHandedOut.Add(1)) - 1
		if index >= len(vmids) {
			t.Errorf("fake was asked for more vmids than the %d it has", len(vmids))
			http.Error(writer, "out of vmids", http.StatusInternalServerError)

			return
		}

		fmt.Fprintf(writer, `{"data":"%d"}`, vmids[index])
	})

	mux.HandleFunc("POST "+templateRoute("clone"), func(writer http.ResponseWriter, r *http.Request) {
		var cloneOptions proxmox.VirtualMachineCloneOptions

		err := json.NewDecoder(r.Body).Decode(&cloneOptions)
		if err != nil {
			t.Errorf("failed to decode clone options: %v", err)
			http.Error(writer, "undecodable clone options", http.StatusBadRequest)

			return
		}

		if clonesSeen.Add(1) == 1 {
			http.Error(writer, "clone refused", http.StatusInternalServerError)

			return
		}

		fmt.Fprintf(writer, `{"data":%q}`, increaseTestUPID(testTaskTypeClone, cloneOptions.NewID))
	})

	// Every task the fake hands out succeeded; the batch's one failure is the refused clone,
	// which never produced a task at all.
	mux.HandleFunc("GET /nodes/"+increaseTestNode+"/tasks/", func(writer http.ResponseWriter, r *http.Request) {
		upid := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/nodes/"+increaseTestNode+"/tasks/"), "/status")

		fields := strings.Split(upid, ":")
		if len(fields) < 8 {
			t.Errorf("task route asked for a malformed UPID: %q", upid)
			http.Error(writer, "malformed upid", http.StatusBadRequest)

			return
		}

		//nolint:gosec//G705 // test-only mock server: every argument is a fixture value, not user input
		fmt.Fprintf(writer, `{"data":{"upid":%q,"node":%q,"type":%q,"id":%q,"user":"root@pam","status":%q,"exitstatus":%q}}`,
			upid, increaseTestNode, fields[5], fields[6], taskStatusStopped, taskExitStatusOK)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	templateID := increaseTestTemplateID

	ig := newWaitTestGroup()
	ig.Pool = increaseTestPool
	ig.Storage = increaseTestStorage
	ig.TemplateID = &templateID
	ig.InstanceNameCreating = DefaultInstanceNameCreating
	ig.InstanceNameRunning = DefaultInstanceNameRunning
	ig.InstanceTagsCreating = DefaultInstanceNameCreating
	ig.InstanceTagsRunning = DefaultInstanceNameRunning
	ig.log = log
	ig.proxmox = proxmox.NewClient(server.URL)

	return ig, counts
}

// increaseTestVMID reads the vmid a VM route was called for, so a task UPID can name the VM
// whose task it is -- Task.Ping parses that id back out and the fake has to stay consistent
// with it.
func increaseTestVMID(t *testing.T, r *http.Request) int {
	t.Helper()

	vmid, err := strconv.Atoi(r.PathValue("vmid"))
	if err != nil {
		t.Errorf("VM route called with a non-numeric vmid %q: %v", r.PathValue("vmid"), err)
	}

	return vmid
}

// Decrease must not act on a VM whose listed name is not one of the three owned names. A
// foreign name means the VMID belongs to another manager or has been reused since fleeting
// last saw it; in both cases we must not rename it. The VM is reported as succeeded so the
// provisioner stops asking; Update will no longer list it and it will be pruned on the next
// cycle. It must not count toward batchError.
func TestDecreaseOwnership(t *testing.T) {
	t.Parallel()

	type row struct {
		name      string
		members   []removalTestMember
		wantSucc  []string
		wantPOSTs int64
		wantWarn  bool
		wantErr   error
	}

	rows := []row{
		{
			// Also the VMID-reuse case: the provisioner still holds 100 as fleeting-running,
			// but the pool now lists it under a foreign name.
			name:     "foreign other-running",
			members:  []removalTestMember{{vmid: 100, name: foreignRunning}},
			wantSucc: []string{"100"},
			wantWarn: true,
		},
		{
			name:     "unrelated name backup-2026",
			members:  []removalTestMember{{vmid: 100, name: "backup-2026"}},
			wantSucc: []string{"100"},
			wantWarn: true,
		},
		{
			name:    "own fleeting-creating",
			members: []removalTestMember{{vmid: 100, name: DefaultInstanceNameCreating}},
		},
		{
			name:     "own fleeting-removing",
			members:  []removalTestMember{{vmid: 100, name: DefaultInstanceNameRemoving}},
			wantSucc: []string{"100"},
		},
		{
			name:      "own fleeting-running task fails",
			members:   []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning}},
			wantPOSTs: 1,
			wantErr:   ErrTaskFailed,
		},
		{
			// Pool lists under our name but the VM was renamed before we fetched it (lag
			// window). The rename must be refused and the instance is not in succeeded.
			name:    "lag: listed fleeting-running fetched other-running",
			members: []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning, fetchedName: foreignRunning}},
			wantErr: ErrNotOwned,
		},
		{
			// An earlier attempt's rename landed but the listing still trails it: the VM is
			// already marked, so it is reported without a second rename.
			name:     "lag: listed fleeting-running fetched fleeting-removing",
			members:  []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning, fetchedName: DefaultInstanceNameRemoving}},
			wantSucc: []string{"100"},
		},
		{
			// No name in the listing (Proxmox has no fresh status for the VM): the name on the
			// VM decides, and our own running VM is still renamed.
			name:      "unnamed listing, fetched fleeting-running",
			members:   []removalTestMember{{vmid: 100, name: "", fetchedName: DefaultInstanceNameRunning}},
			wantPOSTs: 1,
			wantErr:   ErrTaskFailed,
		},
		{
			name:     "unnamed listing, fetched other-running",
			members:  []removalTestMember{{vmid: 100, name: "", fetchedName: foreignRunning}},
			wantSucc: []string{"100"},
			wantWarn: true,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()

			log, logBuf := newLogBuffer(t)
			counts := &removalRequestCounts{}
			ig := newRemovalTestGroup(t, removalTestServer{
				log:      log,
				members:  row.members,
				requests: counts,
			})

			succeeded, err := ig.Decrease(context.Background(), []string{"100"})

			if row.wantErr != nil {
				require.ErrorIs(t, err, row.wantErr)
			} else {
				require.NoError(t, err)
			}

			require.ElementsMatch(t, row.wantSucc, succeeded)

			require.Equal(t, row.wantPOSTs, counts.configPOSTs.Load(), "config POST count")

			const warnPattern = `\[WARN\].*refusing to remove instance not owned by this group`
			if row.wantWarn {
				require.Regexp(t, warnPattern, logBuf.String())
			} else {
				require.NotRegexp(t, warnPattern, logBuf.String())
			}
		})
	}
}

// TestRPCsRefuseForeignInstance verifies that ConnectInfo, Heartbeat, Resume, and Suspend all
// refuse to act on a VM whose listed or fetched name is not owned by this group, with a message
// that names the vmid and the foreign name. A VM listed under a foreign name gets no per-vmid
// API request at all; one listed under our name but fetched under a foreign one gets only the
// fetch that revealed it.
func TestRPCsRefuseForeignInstance(t *testing.T) {
	t.Parallel()

	members := []removalTestMember{
		{vmid: 100, name: DefaultInstanceNameRunning},
		{vmid: 101, name: foreignRunning},
		{vmid: 102, name: DefaultInstanceNameRunning, fetchedName: foreignRunning},
		{vmid: 103, name: "", fetchedName: DefaultInstanceNameRunning},
	}

	t.Run("ConnectInfo refusal", func(t *testing.T) {
		t.Parallel()

		counts := &removalRequestCounts{}
		ig := newRemovalTestGroup(t, removalTestServer{members: members, requests: counts})

		_, err := ig.ConnectInfo(context.Background(), "101")
		require.ErrorIs(t, err, ErrNotOwned)
		require.Contains(t, err.Error(), "failed to retrieve instance vmid='101'")
		require.Contains(t, err.Error(), foreignRunning)
		require.Empty(t, counts.requestsFor(101))
	})

	t.Run("Heartbeat refusal", func(t *testing.T) {
		t.Parallel()

		counts := &removalRequestCounts{}
		ig := newRemovalTestGroup(t, removalTestServer{members: members, requests: counts})

		err := ig.Heartbeat(context.Background(), "101")
		require.ErrorIs(t, err, ErrNotOwned)
		require.Contains(t, err.Error(), "failed to retrieve instance vmid='101'")
		require.Contains(t, err.Error(), foreignRunning)
		require.Empty(t, counts.requestsFor(101))
	})

	t.Run("Resume refusal", func(t *testing.T) {
		t.Parallel()

		counts := &removalRequestCounts{}
		ig := newRemovalTestGroup(t, removalTestServer{members: members, requests: counts})

		succeeded, err := ig.Resume(context.Background(), []string{"100", "101"})
		require.Equal(t, []string{"100"}, succeeded)
		require.ErrorIs(t, err, ErrResumeFailed)
		require.Contains(t, err.Error(), "not owned")
		require.Empty(t, counts.requestsFor(101))
		require.True(t, counts.requested(100, "", "status/resume"), "vm 100 should have received a resume request")
	})

	t.Run("Suspend refusal", func(t *testing.T) {
		t.Parallel()

		counts := &removalRequestCounts{}
		ig := newRemovalTestGroup(t, removalTestServer{members: members, requests: counts})

		succeeded, err := ig.Suspend(context.Background(), []string{"100", "101"})
		require.Equal(t, []string{"100"}, succeeded)
		require.ErrorIs(t, err, ErrSuspendFailed)
		require.Contains(t, err.Error(), "not owned")
		require.Empty(t, counts.requestsFor(101))
		require.True(t, counts.requested(100, "", "status/suspend"), "vm 100 should have received a suspend request")
	})

	t.Run("Heartbeat control", func(t *testing.T) {
		t.Parallel()

		counts := &removalRequestCounts{}
		ig := newRemovalTestGroup(t, removalTestServer{members: members, requests: counts})

		err := ig.Heartbeat(context.Background(), "100")
		require.NoError(t, err)
		require.True(t, counts.requested(100, "", "agent/get-osinfo"), "vm 100 should have received an agent/get-osinfo request")
	})

	// An unnamed listing is no evidence of a foreign VM: the fetched name decides.
	t.Run("Heartbeat control: unnamed listing", func(t *testing.T) {
		t.Parallel()

		counts := &removalRequestCounts{}
		ig := newRemovalTestGroup(t, removalTestServer{members: members, requests: counts})

		err := ig.Heartbeat(context.Background(), "103")
		require.NoError(t, err)
		require.True(t, counts.requested(103, "", "agent/get-osinfo"), "vm 103 should have received an agent/get-osinfo request")
	})

	// Listed under our name, fetched under a foreign one: every RPC refuses without acting.
	fetchedForeign := []struct {
		name string
		call func(ig *InstanceGroup) error
	}{
		{"ConnectInfo", func(ig *InstanceGroup) error {
			_, err := ig.ConnectInfo(context.Background(), "102")

			return err
		}},
		{"Heartbeat", func(ig *InstanceGroup) error { return ig.Heartbeat(context.Background(), "102") }},
		{"Resume", func(ig *InstanceGroup) error {
			_, err := ig.Resume(context.Background(), []string{"102"})

			return err
		}},
		{"Suspend", func(ig *InstanceGroup) error {
			_, err := ig.Suspend(context.Background(), []string{"102"})

			return err
		}},
	}

	for _, testCase := range fetchedForeign {
		t.Run(testCase.name+" refusal: fetched name", func(t *testing.T) {
			t.Parallel()

			counts := &removalRequestCounts{}
			ig := newRemovalTestGroup(t, removalTestServer{members: members, requests: counts})

			err := testCase.call(ig)
			require.Error(t, err)
			require.Contains(t, err.Error(), "vmid='102' is named \"other-running\": not owned")

			for _, route := range []string{"agent/", "status/resume", "status/suspend"} {
				require.False(t, counts.requested(102, "", route), "acted on vm 102 via %q: %v", route, counts.requestsFor(102))
			}
		})
	}
}

// Increase must not report a partially successful batch as an error. fleeting's gRPC shim
// returns the response alongside the error and grpc-go discards the response whenever the
// error is non-nil, so the instances that did come up would reach the provisioner as no
// instances at all. What the nil error hides goes to the log instead. This is the only test
// that drives Increase at the RPC boundary.
func TestIncreaseReportsPartialBatch(t *testing.T) {
	t.Parallel()

	log, logBuffer := newLogBuffer(t)
	ig, _ := newIncreaseTestGroup(t, log)

	succeeded, err := ig.Increase(context.Background(), 2)

	require.NoError(t, err, "a partial success reported as an error reaches the provisioner as no instances at all")
	require.Equal(t, 1, succeeded, "the second clone deployed; the first was refused")
	require.Regexp(t, `\[WARN\].*failed to deploy some instances: attempted=2 failed=1`, logBuffer.String())
}

// A deploy must preserve the tags the clone inherited from the template: the creating tags
// are added to them right after the clone, and the running tags replace the creating tags
// once the instance is up.
func TestIncreasePreservesTemplateTags(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)
	ig, counts := newIncreaseTestGroup(t, log)

	succeeded, err := ig.Increase(context.Background(), 2)

	require.NoError(t, err, "a partial success reported as an error reaches the provisioner as no instances at all")
	require.Equal(t, 1, succeeded, "the second clone deployed; the first was refused")

	// Only the deployed clone (101) got config POSTs; the refused one (100) never existed.
	deployed := counts.configsFor(101)
	require.Len(t, deployed, 2)
	require.Equal(t, map[string]any{
		vmOptTags: increaseTestTagsCreating,
	}, deployed[0])
	require.Equal(t, map[string]any{
		vmOptName: DefaultInstanceNameRunning,
		vmOptTags: "template-tag,manual-tag,fleeting-running",
	}, deployed[1])
}

// While Increase waits on the clone task, the cloning mutex must already be free: the mutex
// guards the pool listings against an in-flight clone POST, not the minutes-long wait that
// follows it. Holding it for the whole batch would stall the provisioner's Update and
// Decrease for the entire deployment.
func TestIncreaseReleasesCloningMutexWhileCloneTaskRuns(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)

	node := increaseTestNode
	cloneVMID := increaseTestCloneVMIDs()[0]

	var (
		cloneAccepted    = make(chan struct{})
		taskUnblock      = make(chan struct{})
		cloneStatusPings atomic.Int64
	)

	constant := func(body string) http.HandlerFunc {
		return func(writer http.ResponseWriter, _ *http.Request) { fmt.Fprint(writer, body) }
	}

	templateRoute := func(suffix string) string {
		return fmt.Sprintf("/nodes/%s/qemu/%d/%s", node, increaseTestTemplateID, suffix)
	}

	cloneRoute := func(suffix string) string {
		return fmt.Sprintf("/nodes/%s/qemu/%d/%s", node, cloneVMID, suffix)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /pools/", constant(fmt.Sprintf(
		`{"data":[{"poolid":"%s","members":[%s,%s]}]}`,
		increaseTestPool,
		fmt.Sprintf(`{"vmid":%d,"type":"qemu","name":"template","node":%q}`, increaseTestTemplateID, node),
		fmt.Sprintf(`{"vmid":%d,"type":"qemu","name":"%s","node":%q}`, cloneVMID, DefaultInstanceNameCreating, node))))

	mux.HandleFunc("GET /cluster/status", constant(`{"data":[]}`))
	mux.HandleFunc("GET /cluster/nextid", constant(fmt.Sprintf(`{"data":"%d"}`, cloneVMID)))
	mux.HandleFunc("GET /nodes/"+node+"/status", constant(`{"data":{}}`))

	mux.HandleFunc("GET "+templateRoute("status/current"),
		constant(fmt.Sprintf(`{"data":{"vmid":%d,"name":"template","status":"stopped","template":1}}`, increaseTestTemplateID)))
	mux.HandleFunc("GET "+templateRoute("config"), constant(`{"data":{"tags":"template-tag"}}`))

	mux.HandleFunc("POST "+templateRoute("clone"), func(writer http.ResponseWriter, _ *http.Request) {
		close(cloneAccepted)

		fmt.Fprintf(writer, `{"data":%q}`, increaseTestUPID(testTaskTypeClone, cloneVMID))
	})

	// The clone task stays running until the test releases it, so Increase sits in its task
	// wait; every other task the fake hands out is already stopped.
	mux.HandleFunc("GET /nodes/"+node+"/tasks/", func(writer http.ResponseWriter, request *http.Request) {
		upid := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/nodes/"+node+"/tasks/"), "/status")

		fields := strings.Split(upid, ":")
		if len(fields) < 8 {
			t.Errorf("task route asked for a malformed UPID: %q", upid)
			http.Error(writer, "malformed upid", http.StatusBadRequest)

			return
		}

		status := taskStatusStopped

		if fields[5] == testTaskTypeClone {
			cloneStatusPings.Add(1)
			<-taskUnblock
		}

		//nolint:gosec//G705 // test-only mock server: every argument is a fixture value, not user input
		fmt.Fprintf(writer, `{"data":{"upid":%q,"node":%q,"type":%q,"id":%q,"user":"root@pam","status":%q,"exitstatus":%q}}`,
			upid, node, fields[5], fields[6], status, taskExitStatusOK)
	})

	mux.HandleFunc("GET "+cloneRoute("status/current"),
		constant(fmt.Sprintf(`{"data":{"vmid":%d,"name":"%s","status":"running"}}`, cloneVMID, DefaultInstanceNameCreating)))
	mux.HandleFunc("GET "+cloneRoute("config"), constant(`{"data":{"tags":"template-tag"}}`))

	mux.HandleFunc("POST "+cloneRoute("config"), func(writer http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(writer, `{"data":%q}`, increaseTestUPID("qmconfig", cloneVMID))
	})
	mux.HandleFunc("POST "+cloneRoute("status/start"), func(writer http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(writer, `{"data":%q}`, increaseTestUPID("qmstart", cloneVMID))
	})
	mux.HandleFunc("GET "+cloneRoute("agent/get-osinfo"), constant(`{"data":{"result":{}}}`))

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	templateID := increaseTestTemplateID

	ig := newWaitTestGroup()
	ig.Pool = increaseTestPool
	ig.Storage = increaseTestStorage
	ig.TemplateID = &templateID
	ig.InstanceNameCreating = DefaultInstanceNameCreating
	ig.InstanceNameRunning = DefaultInstanceNameRunning
	ig.InstanceNameRemoving = DefaultInstanceNameRemoving
	ig.InstanceTagsCreating = DefaultInstanceNameCreating
	ig.InstanceTagsRunning = DefaultInstanceNameRunning
	ig.InstanceTagsRemoving = DefaultInstanceNameRemoving
	ig.log = log
	ig.proxmox = proxmox.NewClient(server.URL)

	result := make(chan struct {
		succeeded int
		err       error
	}, 1)

	go func() {
		succeeded, err := ig.Increase(context.Background(), 1)
		result <- struct {
			succeeded int
			err       error
		}{succeeded: succeeded, err: err}
	}()

	select {
	case <-cloneAccepted:
	case <-time.After(5 * time.Second):
		t.Fatal("the clone POST was never served")
	}

	// The first clone-task ping is only sent once the clone call returned and Increase is in
	// its task wait, so by the time it is in flight the mutex must already be released.
	deadline := time.Now().Add(5 * time.Second)
	for cloneStatusPings.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if cloneStatusPings.Load() < 1 {
		t.Fatal("Increase never started waiting on the clone task")
	}

	if !ig.instanceCloningMu.TryLock() {
		t.Fatal("the cloning mutex is still held while Increase waits on the clone task")
	}

	ig.instanceCloningMu.Unlock()
	close(taskUnblock)

	select {
	case res := <-result:
		require.NoError(t, res.err, "the deploy continued after the clone task finished")
		require.Equal(t, 1, res.succeeded)
	case <-time.After(10 * time.Second):
		t.Fatal("Increase did not return after the clone task was released")
	}
}

// Update reports each pool member that carries one of this group's names under the state the
// name maps to, and skips everything else: another manager's VM, the template, and pool
// members that are not VMs at all.
func TestUpdateReportsInstanceStates(t *testing.T) {
	t.Parallel()

	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{
			{vmid: 100, name: DefaultInstanceNameCreating},
			{vmid: 101, name: DefaultInstanceNameRunning},
			{vmid: 102, name: DefaultInstanceNameRemoving},
			{vmid: 103, name: foreignRunning},
			{vmid: 200, name: increaseTestTemplate},
			{vmid: 300, name: DefaultInstanceNameRunning, memberType: increaseTestMemberTypeLXC},
		},
	})

	type reportedInstance struct {
		id    string
		state provider.State
	}

	var reported []reportedInstance

	err := ig.Update(context.Background(), func(id string, state provider.State) {
		reported = append(reported, reportedInstance{id, state})
	})
	require.NoError(t, err)
	require.Equal(t, []reportedInstance{
		{"100", provider.StateCreating},
		{"101", provider.StateRunning},
		{"102", provider.StateDeleting},
	}, reported)
}

// An empty listed name (Proxmox's status cache holds no fresh entry for the VM) is no evidence
// of ownership, so Update asks the VM itself, the way Decrease does: a member whose config
// carries one of this group's names is reported under it, a member that resolves to a foreign
// name or to nothing is not, and a member whose fetch fails stays out of the report for this
// cycle with a warning.
func TestUpdateResolvesEmptyListedNames(t *testing.T) {
	t.Parallel()

	log, logBuf := newLogBuffer(t)

	ig := newRemovalTestGroup(t, removalTestServer{
		log: log,
		members: []removalTestMember{
			{vmid: 100, name: "", fetchedName: DefaultInstanceNameRunning},
			{vmid: 101, name: "", fetchedName: foreignRunning},
			{vmid: 102, name: ""},
			{vmid: 103, name: ""},
			{vmid: 104, name: DefaultInstanceNameCreating},
		},
		failPaths: []string{"/qemu/103/"},
	})

	type reportedInstance struct {
		id    string
		state provider.State
	}

	var reported []reportedInstance

	err := ig.Update(context.Background(), func(id string, state provider.State) {
		reported = append(reported, reportedInstance{id, state})
	})
	require.NoError(t, err)
	require.Equal(t, []reportedInstance{
		{"100", provider.StateRunning},
		{"104", provider.StateCreating},
	}, reported)
	require.Regexp(t, `\[WARN\].*cannot read the name of an instance`, logBuf.String())
}

func TestUpdatePoolFetchFailure(t *testing.T) {
	t.Parallel()

	ig := newRemovalTestGroup(t, removalTestServer{failPaths: []string{increaseTestPools}})

	err := ig.Update(context.Background(), func(string, provider.State) {})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to get pool")
}

// The pool listing must wait for the cloning mutex: while a clone is in flight, a recycled
// VMID is reported with its predecessor's stale name, and a snapshot taken in that window
// would let Update report a just-created VM under the old one's state.
func TestUpdateListsPoolUnderCloningMutex(t *testing.T) {
	t.Parallel()

	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		members:  []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning}},
		requests: counts,
	})

	// Hold the cloning mutex as an in-flight Increase would.
	ig.instanceCloningMu.Lock()

	type updateResult struct {
		reported int
		err      error
	}

	result := make(chan updateResult, 1)

	go func() {
		reported := 0

		err := ig.Update(context.Background(), func(string, provider.State) { reported++ })
		result <- updateResult{reported: reported, err: err}
	}()

	// The pool listing must wait for the mutex. Poll the request log briefly rather than
	// trusting a fixed sleep: the moment a listing is served while the mutex is held, the
	// window the mutex exists to close has been read.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if counts.poolRequested() {
			t.Fatal("Update listed the pool while a clone was in flight")
		}

		time.Sleep(10 * time.Millisecond)
	}

	ig.instanceCloningMu.Unlock()

	select {
	case res := <-result:
		require.NoError(t, res.err)
		require.Equal(t, 1, res.reported, "the member was not reported after the clone finished")
	case <-time.After(time.Second):
		t.Fatal("Update did not return after the cloning mutex was released")
	}
}

// While Update runs its per-instance callback, the cloning mutex must be free: the mutex
// guards the pool listing against an in-flight clone POST, not the callback, which can
// round-trip to the provisioner. Holding it through the callback would stall Increase,
// Decrease and the collector for the whole reconcile.
func TestUpdateReleasesCloningMutexBeforeCallback(t *testing.T) {
	t.Parallel()

	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{
			{vmid: 100, name: DefaultInstanceNameCreating},
			{vmid: 101, name: DefaultInstanceNameRunning},
		},
	})

	inCallback := make(chan struct{})
	release := make(chan struct{})

	type updateResult struct {
		reported int
		err      error
	}

	result := make(chan updateResult, 1)

	go func() {
		reported := 0

		err := ig.Update(context.Background(), func(string, provider.State) {
			reported++

			if reported == 1 {
				close(inCallback)
				<-release
			}
		})
		result <- updateResult{reported: reported, err: err}
	}()

	// By the time the first callback is in flight the listing is long done, so a mutex
	// still held there is one held for the callback.
	select {
	case <-inCallback:
	case <-time.After(5 * time.Second):
		t.Fatal("Update never reached the per-instance callback")
	}

	if !ig.instanceCloningMu.TryLock() {
		t.Fatal("the cloning mutex is still held while Update's callback is in flight")
	}

	ig.instanceCloningMu.Unlock()
	close(release)

	select {
	case res := <-result:
		require.NoError(t, res.err)
		require.Equal(t, 2, res.reported)
	case <-time.After(5 * time.Second):
		t.Fatal("Update did not return after the callback was released")
	}
}

func TestConnectInfo(t *testing.T) {
	t.Parallel()

	t.Run("returns the instance addresses", func(t *testing.T) {
		t.Parallel()

		ig := newRemovalTestGroup(t, removalTestServer{
			members: []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning}},
		})
		ig.InstanceNetworkProtocol = NetworkProtocolIPv4
		connectorConfig := provider.ConnectorConfig{OS: "linux", Protocol: "ssh"}
		ig.FleetingSettings = provider.Settings{ConnectorConfig: connectorConfig}

		info, err := ig.ConnectInfo(context.Background(), "100")
		require.NoError(t, err)
		require.Equal(t, "100", info.ID)
		require.Equal(t, "192.168.0.1", info.InternalAddr)
		require.Equal(t, "8.8.8.8", info.ExternalAddr)
		require.Equal(t, connectorConfig, info.ConnectorConfig)
	})

	t.Run("prefers IPv6 under the any protocol", func(t *testing.T) {
		t.Parallel()

		ifaces := `{"name":"eth0","hardware-address":"12:34:56:AB:CD:EF",` +
			`"ip-addresses":[{"ip-address-type":"ipv4","ip-address":"192.168.0.1","prefix":24},` +
			`{"ip-address-type":"ipv6","ip-address":"fd3b:47fc:de09::1","prefix":64},` +
			`{"ip-address-type":"ipv6","ip-address":"2001:4860:4860::8888","prefix":128}]}`

		ig := newRemovalTestGroup(t, removalTestServer{
			members: []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning, ifaces: ifaces}},
		})
		ig.InstanceNetworkProtocol = NetworkProtocolAny

		info, err := ig.ConnectInfo(context.Background(), "100")
		require.NoError(t, err)
		require.Equal(t, "fd3b:47fc:de09::1", info.InternalAddr)
		require.Equal(t, "2001:4860:4860::8888", info.ExternalAddr)
	})

	t.Run("refuses a non-numeric instance id", func(t *testing.T) {
		t.Parallel()

		ig := newRemovalTestGroup(t, removalTestServer{})

		_, err := ig.ConnectInfo(context.Background(), increaseTestInvalidID)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to parse instance name")
	})

	// A fetch that yields no usable address is retried, not failed: the agent can report
	// before the interface has an address.
	t.Run("retries until the addresses appear", func(t *testing.T) {
		t.Parallel()

		log, logBuf := newLogBuffer(t)
		counts := &removalRequestCounts{}

		// The first fetch reports an interface with a loopback hardware address, which is
		// filtered out and yields no address; the retry reports the real one.
		ig := newRemovalTestGroup(t, removalTestServer{
			log: log,
			members: []removalTestMember{
				{vmid: 100, name: DefaultInstanceNameRunning},
			},
			networkIfaceBodies: []string{
				`{"name":"eth0","hardware-address":"00:00:00:00:00:00","ip-addresses":[{"ip-address-type":"ipv4","ip-address":"192.168.0.1","prefix":24}]}`,
				defaultRemovalTestIfaces,
			},
			requests: counts,
		})
		ig.InstanceNetworkProtocol = NetworkProtocolIPv4

		info, err := ig.ConnectInfo(context.Background(), "100")
		require.NoError(t, err)
		require.Equal(t, "192.168.0.1", info.InternalAddr)

		fetches := 0
		for _, request := range counts.requestsFor(100) {
			if strings.Contains(request, "agent/network-get-interfaces") {
				fetches++
			}
		}

		require.Equal(t, 2, fetches, "the first fetch must be retried: %v", counts.requestsFor(100))
		require.Regexp(t, `\[ERROR\].*failed to get network interface.*retry=0`, logBuf.String())
	})

	// Per-instance connects share the pool listing the way heartbeats do: the pool
	// endpoint is paid once for the batch of connects, not once per instance.
	t.Run("shares the pool listing across calls", func(t *testing.T) {
		t.Parallel()

		counts := &removalRequestCounts{}
		ig := newRemovalTestGroup(t, removalTestServer{
			members: []removalTestMember{
				{vmid: 100, name: DefaultInstanceNameRunning},
				{vmid: 101, name: DefaultInstanceNameRunning},
			},
			requests: counts,
		})
		ig.InstanceNetworkProtocol = NetworkProtocolIPv4

		_, err := ig.ConnectInfo(context.Background(), "100")
		require.NoError(t, err)

		_, err = ig.ConnectInfo(context.Background(), "101")
		require.NoError(t, err)

		require.Equal(t, 1, counts.poolListings())
	})

	// A canceled ConnectInfo must not sit out the retry sleeps: it returns with the
	// cancellation as soon as the current sleep is interrupted, not after the full retry
	// budget.
	t.Run("returns on context cancel during a retry sleep", func(t *testing.T) {
		t.Parallel()

		ig := newRemovalTestGroup(t, removalTestServer{
			members: []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning}},
			// Every fetch reports only a filtered-out interface, so every retry sleeps.
			networkIfaceBodies: []string{
				`{"name":"eth0","hardware-address":"00:00:00:00:00:00","ip-addresses":[{"ip-address-type":"ipv4","ip-address":"192.168.0.1","prefix":24}]}`,
			},
		})
		ig.InstanceNetworkProtocol = NetworkProtocolIPv4

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error, 1)

		go func() {
			_, err := ig.ConnectInfo(ctx, "100")
			done <- err
		}()

		// Cancel mid first-sleep.
		time.Sleep(200 * time.Millisecond)
		cancel()

		// A fixed implementation returns at the cancel; a sleep that ignores ctx cannot
		// return before the first full retry, so half a retry cleanly separates the two.
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(networkCheckTimeout / 2):
			t.Fatal("ConnectInfo did not return on context cancellation")
		}
	})
}

func TestHeartbeat(t *testing.T) {
	t.Parallel()

	t.Run("refuses a non-numeric instance id", func(t *testing.T) {
		t.Parallel()

		ig := newRemovalTestGroup(t, removalTestServer{})

		err := ig.Heartbeat(context.Background(), increaseTestInvalidID)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid vm id")
	})

	t.Run("fails when the qemu agent is not answering", func(t *testing.T) {
		t.Parallel()

		ig := newRemovalTestGroup(t, removalTestServer{
			members: []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning, osinfoFails: true}},
		})

		err := ig.Heartbeat(context.Background(), "100")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to connect to qemu agent")
	})
}

// The per-instance lookups share one pool listing inside the cache window: a fleet of
// heartbeats and connects pays the pool endpoint once instead of once per instance. An
// aged-out listing is re-fetched, and a re-Init discards a still-fresh one.
func TestHeartbeatSharesPoolListing(t *testing.T) {
	t.Parallel()

	t.Run("heartbeats within the cache window share one listing", func(t *testing.T) {
		t.Parallel()

		counts := &removalRequestCounts{}
		ig := newRemovalTestGroup(t, removalTestServer{
			members: []removalTestMember{
				{vmid: 100, name: DefaultInstanceNameRunning},
				{vmid: 101, name: DefaultInstanceNameRunning},
			},
			requests: counts,
		})

		require.NoError(t, ig.Heartbeat(context.Background(), "100"))
		require.NoError(t, ig.Heartbeat(context.Background(), "101"))
		require.NoError(t, ig.Heartbeat(context.Background(), "100"))

		require.Equal(t, 1, counts.poolListings())
	})

	t.Run("an aged or reset cache lists again", func(t *testing.T) {
		t.Parallel()

		counts := &removalRequestCounts{}
		ig := newRemovalTestGroup(t, removalTestServer{
			members:  []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning}},
			requests: counts,
		})

		require.NoError(t, ig.Heartbeat(context.Background(), "100"))
		require.Equal(t, 1, counts.poolListings())

		// Age the cache past its TTL: the next heartbeat must list again.
		ig.poolCacheAt = time.Now().Add(-2 * poolCacheTTL)

		require.NoError(t, ig.Heartbeat(context.Background(), "100"))
		require.Equal(t, 2, counts.poolListings())

		// A re-Init discards the listing even though it is still fresh.
		ig.resetLifecycle()

		require.NoError(t, ig.Heartbeat(context.Background(), "100"))
		require.Equal(t, 3, counts.poolListings())
	})
}

// Resume and Suspend report each instance's own outcome: a batch in which every attempt
// failed is an error, one in which something succeeded is not, and an unparseable id fails
// its own slot without touching the others.
func TestResumeAndSuspend(t *testing.T) {
	t.Parallel()

	operations := []struct {
		name    string
		call    func(ig *InstanceGroup, instances []string) ([]string, error)
		wantErr error
		route   string
	}{
		{
			name: "Resume",
			call: func(ig *InstanceGroup, instances []string) ([]string, error) {
				return ig.Resume(context.Background(), instances)
			},
			wantErr: ErrResumeFailed,
			route:   "status/resume",
		},
		{
			name: "Suspend",
			call: func(ig *InstanceGroup, instances []string) ([]string, error) {
				return ig.Suspend(context.Background(), instances)
			},
			wantErr: ErrSuspendFailed,
			route:   "status/suspend",
		},
	}

	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			t.Parallel()

			t.Run("all instances succeed", func(t *testing.T) {
				t.Parallel()

				counts := &removalRequestCounts{}
				ig := newRemovalTestGroup(t, removalTestServer{
					members: []removalTestMember{
						{vmid: 100, name: DefaultInstanceNameRunning},
						{vmid: 101, name: DefaultInstanceNameRunning},
					},
					requests: counts,
				})

				succeeded, err := operation.call(ig, []string{"100", "101"})
				require.NoError(t, err)
				require.Equal(t, []string{"100", "101"}, succeeded)
				require.True(t, counts.requested(100, "", operation.route))
				require.True(t, counts.requested(101, "", operation.route))
			})

			t.Run("unparseable id fails its own slot", func(t *testing.T) {
				t.Parallel()

				ig := newRemovalTestGroup(t, removalTestServer{
					members: []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning}},
				})

				succeeded, err := operation.call(ig, []string{"100", increaseTestInvalidID})
				require.ErrorIs(t, err, operation.wantErr)
				require.Contains(t, err.Error(), "invalid vm id 'not-a-vmid'")
				require.Equal(t, []string{"100"}, succeeded)
			})

			t.Run("refused rename target is not attempted", func(t *testing.T) {
				t.Parallel()

				ig := newRemovalTestGroup(t, removalTestServer{
					members:   []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning}},
					failPaths: []string{operation.route},
				})

				succeeded, err := operation.call(ig, []string{"100"})
				require.ErrorIs(t, err, operation.wantErr)
				require.Contains(t, err.Error(), "api call failed for vm id '100'")
				require.Empty(t, succeeded)
			})

			// A batch shares one pool listing: the per-instance work runs over a single
			// shared listing, so the pool endpoint is paid once, not once per instance.
			t.Run("batch shares one pool listing", func(t *testing.T) {
				t.Parallel()

				counts := &removalRequestCounts{}
				ig := newRemovalTestGroup(t, removalTestServer{
					members: []removalTestMember{
						{vmid: 100, name: DefaultInstanceNameRunning},
						{vmid: 101, name: DefaultInstanceNameRunning},
						{vmid: 102, name: DefaultInstanceNameRunning},
					},
					requests: counts,
				})

				succeeded, err := operation.call(ig, []string{"100", "101", "102"})
				require.NoError(t, err)
				require.Equal(t, []string{"100", "101", "102"}, succeeded)
				require.Equal(t, 1, counts.poolListings(), "a batch must list the pool once")
			})
		})
	}
}

// Init validates the settings, builds the client, sweeps the stale instances, and starts the
// background workers; it reports the pool as the group id and the configured maximum as its
// size. A stale instance whose rename fails must not stop the startup.
func TestInitLifecycle(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)

	tempDir := t.TempDir()
	credentialsPath := path.Join(tempDir, "credentials.json")

	require.NoError(t, os.WriteFile(credentialsPath, []byte(`{"realm":"pve","username":"test","password":"secret"}`), 0o600))

	maxInstances := 7

	ig := newRemovalTestGroup(t, removalTestServer{log: log})
	ig.CredentialsFilePath = credentialsPath
	ig.MaxInstances = &maxInstances

	info, err := ig.Init(context.Background(), log, provider.Settings{})
	require.NoError(t, err)
	require.Equal(t, provider.ProviderInfo{ID: "test-pool", MaxSize: 7}, info)

	// Init's stale sweep leaves a collection wake-up pending; the collector may spend up to
	// collectionWaitAfterTrigger serving it before it notices the shutdown, so the budget
	// covers that.
	select {
	case <-ig.instanceCollectionTrigger:
	default:
	}

	done := make(chan error, 1)

	go func() {
		done <- ig.Shutdown(context.Background())
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("Shutdown after Init did not return: possible deadlock")
	}
}

// A pool member that is not in the removal list is not touched.
func TestDecreaseIgnoresInstancesNotListedForRemoval(t *testing.T) {
	t.Parallel()

	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{
			{vmid: 100, name: DefaultInstanceNameRunning},
			{vmid: 101, name: DefaultInstanceNameRunning},
		},
		requests: counts,
	})

	succeeded, err := ig.Decrease(context.Background(), []string{"100"})

	// The one listed instance's rename task fails by default; the other one is untouched.
	require.ErrorIs(t, err, ErrTaskFailed)
	require.Empty(t, succeeded)
	require.Empty(t, counts.requestsFor(101))
}

// A pool member that is not in the removal list is not touched. The listing itself must wait
// for the cloning mutex: while a clone is in flight, a recycled VMID is reported with its
// predecessor's stale name, and a snapshot taken in that window would let Decrease report a
// just-created VM as removed or queue a rename for it.
func TestDecreaseListsPoolUnderCloningMutex(t *testing.T) {
	t.Parallel()

	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		members:  []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning}},
		requests: counts,
	})

	// Hold the cloning mutex as an in-flight Increase would.
	ig.instanceCloningMu.Lock()

	type decreaseResult struct {
		succeeded []string
		err       error
	}

	result := make(chan decreaseResult, 1)

	go func() {
		succeeded, err := ig.Decrease(context.Background(), []string{"100"})
		result <- decreaseResult{succeeded: succeeded, err: err}
	}()

	// The pool listing must wait for the mutex. Poll the request log briefly rather than
	// trusting a fixed sleep: the moment a listing is served while the mutex is held, the
	// window the mutex exists to close has been read.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if counts.poolRequested() {
			t.Fatal("Decrease listed the pool while a clone was in flight")
		}

		time.Sleep(10 * time.Millisecond)
	}

	ig.instanceCloningMu.Unlock()

	select {
	case res := <-result:
		require.True(t, counts.poolRequested(), "Decrease never listed the pool after the clone finished")

		// The fake's rename task fails by default, so the release unblocked a real listing
		// that ran to the end rather than a skipped one.
		require.ErrorIs(t, res.err, ErrTaskFailed)
	case <-time.After(time.Second):
		t.Fatal("Decrease did not return after the cloning mutex was released")
	}
}

// Init fails fast -- before it sleeps or starts any worker -- when it cannot build the
// client or cannot list the pool for the stale sweep.
func TestInitFailsFastWhenItCannotReachProxmox(t *testing.T) {
	t.Parallel()

	maxInstances := 7

	t.Run("client cannot be built", func(t *testing.T) {
		t.Parallel()

		log, _ := newLogBuffer(t)
		ig := newRemovalTestGroup(t, removalTestServer{log: log})
		ig.CredentialsFilePath = path.Join(t.TempDir(), "does-not-exist.json")
		ig.MaxInstances = &maxInstances

		_, err := ig.Init(context.Background(), log, provider.Settings{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to open credentials file")

		requireShutdownReturns(t, ig, "after a failed Init")
	})

	t.Run("pool cannot be listed", func(t *testing.T) {
		t.Parallel()

		log, _ := newLogBuffer(t)

		tempDir := t.TempDir()
		credentialsPath := path.Join(tempDir, "credentials.json")
		require.NoError(t, os.WriteFile(credentialsPath, []byte(`{"realm":"pve","username":"test","password":"secret"}`), 0o600))

		ig := newRemovalTestGroup(t, removalTestServer{log: log, failPaths: []string{increaseTestPools}})
		ig.CredentialsFilePath = credentialsPath
		ig.MaxInstances = &maxInstances

		_, err := ig.Init(context.Background(), log, provider.Settings{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get pool")

		requireShutdownReturns(t, ig, "after a failed Init")
	})
}

// Increase with no template in the pool fails before it asks Proxmox to clone anything.
func TestIncreaseFailsWhenTemplateMissing(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)
	ig := newRemovalTestGroup(t, removalTestServer{
		log:     log,
		members: []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning}},
	})

	count, err := ig.Increase(context.Background(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to find template")
	require.Zero(t, count)
}

// A pool member listed without a name whose VM itself cannot be read is left alone and is
// reported neither as removed (it is still running) nor as an attempt (nothing was asked of
// it).
func TestDecreaseSkipsInstanceWithUnreadableName(t *testing.T) {
	t.Parallel()

	log, logBuf := newLogBuffer(t)
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		log:       log,
		members:   []removalTestMember{{vmid: 100, name: ""}},
		requests:  counts,
		failPaths: []string{increaseTestStatusCurrent},
	})

	succeeded, err := ig.Decrease(context.Background(), []string{"100"})
	require.NoError(t, err)
	require.Empty(t, succeeded)
	require.False(t, counts.requested(100, http.MethodPost, ""), "a VM whose name cannot be read must not be renamed: %v", counts.requestsFor(100))
	require.Regexp(t, `\[WARN\].*cannot read the name of an instance to remove`, logBuf.String())
}
