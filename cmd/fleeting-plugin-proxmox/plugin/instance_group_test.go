package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
)

func TestUnmarshallingPluginSettings(t *testing.T) {
	settingsJSON := `{"url":"sample_url","template_id": 5}`
	instance := InstanceGroup{}

	err := json.Unmarshal([]byte(settingsJSON), &instance)
	require.NoError(t, err)

	require.Equal(t, "sample_url", instance.URL)
	require.Equal(t, 5, *instance.TemplateID)
}

func TestShutdownIsIdempotent(t *testing.T) {
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
	for i := range 3 {
		done := make(chan error, 1)

		go func() {
			done <- ig.Shutdown(context.Background())
		}()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatalf("Shutdown call #%d did not return: possible deadlock", i+1)
		}
	}
}

func TestShutdownWithoutInitDoesNotPanic(t *testing.T) {
	// A group whose Init never ran still has nil trigger channels, and closing a nil channel
	// panics.
	ig := &InstanceGroup{}

	require.NotPanics(t, func() {
		require.NoError(t, ig.Shutdown(context.Background()))
	})
}

func TestShutdownAfterInitCycleDoesNotHang(t *testing.T) {
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
	for _, bystander := range []string{"fleeting-removing", "other-running"} {
		t.Run(bystander, func(t *testing.T) {
			counts := &removalRequestCounts{}
			ig := newRemovalTestGroup(t, removalTestServer{
				members: []removalTestMember{
					{vmid: 100, name: "fleeting-running"},
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

const (
	// increaseTestNode is the one node the Increase fake serves: it appears in the pool
	// listing, in every VM route and inside every task UPID.
	increaseTestNode = "pve-node"

	// increaseTestTemplateID is the VM the fake reports as a template, and so the only vmid
	// whose clone route exists.
	increaseTestTemplateID = 200
)

// increaseTestCloneVMIDs are the vmids /cluster/nextid hands out, in order. Increase
// serialises the nextid/clone pair under its clone mutex, so the first of these is always the
// one whose clone POST is refused and the second is always the one that deploys.
var increaseTestCloneVMIDs = []int{100, 101}

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
func newIncreaseTestGroup(t *testing.T, log hclog.Logger) *InstanceGroup {
	t.Helper()

	var (
		vmidsHandedOut atomic.Int64
		clonesSeen     atomic.Int64
	)

	poolMembers := []string{
		fmt.Sprintf(`{"vmid":%d,"type":"qemu","name":"template","node":%q}`, increaseTestTemplateID, increaseTestNode),
	}

	for _, vmid := range increaseTestCloneVMIDs {
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

	mux.HandleFunc("GET "+vmRoute("config"), constant(`{"data":{}}`))
	mux.HandleFunc("GET "+vmRoute("agent/get-osinfo"), constant(`{"data":{"result":{}}}`))

	mux.HandleFunc("POST "+vmRoute("config"), func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"data":%q}`, increaseTestUPID("qmconfig", increaseTestVMID(t, r)))
	})

	mux.HandleFunc("POST "+vmRoute("status/start"), func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"data":%q}`, increaseTestUPID("qmstart", increaseTestVMID(t, r)))
	})

	mux.HandleFunc("GET /cluster/nextid", func(w http.ResponseWriter, _ *http.Request) {
		index := int(vmidsHandedOut.Add(1)) - 1
		if index >= len(increaseTestCloneVMIDs) {
			t.Errorf("fake was asked for more vmids than the %d it has", len(increaseTestCloneVMIDs))
			http.Error(w, "out of vmids", http.StatusInternalServerError)

			return
		}

		fmt.Fprintf(w, `{"data":"%d"}`, increaseTestCloneVMIDs[index])
	})

	mux.HandleFunc("POST "+templateRoute("clone"), func(w http.ResponseWriter, r *http.Request) {
		var cloneOptions proxmox.VirtualMachineCloneOptions
		if err := json.NewDecoder(r.Body).Decode(&cloneOptions); err != nil {
			t.Errorf("failed to decode clone options: %v", err)
			http.Error(w, "undecodable clone options", http.StatusBadRequest)

			return
		}

		if clonesSeen.Add(1) == 1 {
			http.Error(w, "clone refused", http.StatusInternalServerError)

			return
		}

		fmt.Fprintf(w, `{"data":%q}`, increaseTestUPID("qmclone", cloneOptions.NewID))
	})

	// Every task the fake hands out succeeded; the batch's one failure is the refused clone,
	// which never produced a task at all.
	mux.HandleFunc("GET /nodes/"+increaseTestNode+"/tasks/", func(w http.ResponseWriter, r *http.Request) {
		upid := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/nodes/"+increaseTestNode+"/tasks/"), "/status")

		fields := strings.Split(upid, ":")
		if len(fields) < 8 {
			t.Errorf("task route asked for a malformed UPID: %q", upid)
			http.Error(w, "malformed upid", http.StatusBadRequest)

			return
		}

		fmt.Fprintf(w, `{"data":{"upid":%q,"node":%q,"type":%q,"id":%q,"user":"root@pam","status":%q,"exitstatus":%q}}`,
			upid, increaseTestNode, fields[5], fields[6], taskStatusStopped, taskExitStatusOK)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	templateID := increaseTestTemplateID

	ig := newWaitTestGroup()
	ig.Pool = "test-pool"
	ig.Storage = "local"
	ig.TemplateID = &templateID
	ig.InstanceNameCreating = "fleeting-creating"
	ig.InstanceNameRunning = "fleeting-running"
	ig.InstanceTagsCreating = "fleeting-creating"
	ig.InstanceTagsRunning = "fleeting-running"
	ig.log = log
	ig.proxmox = proxmox.NewClient(server.URL)

	return ig
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
	type row struct {
		name      string
		members   []removalTestMember
		wantSucc  []string
		wantPOSTs int64
		wantWarn  bool
		wantErr   error
	}

	const foreignRunning = "other-running"

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
			members: []removalTestMember{{vmid: 100, name: "fleeting-creating"}},
		},
		{
			name:     "own fleeting-removing",
			members:  []removalTestMember{{vmid: 100, name: "fleeting-removing"}},
			wantSucc: []string{"100"},
		},
		{
			name:      "own fleeting-running task fails",
			members:   []removalTestMember{{vmid: 100, name: "fleeting-running"}},
			wantPOSTs: 1,
			wantErr:   ErrTaskFailed,
		},
		{
			// Pool lists under our name but the VM was renamed before we fetched it (lag
			// window). The rename must be refused and the instance is not in succeeded.
			name:    "lag: listed fleeting-running fetched other-running",
			members: []removalTestMember{{vmid: 100, name: "fleeting-running", fetchedName: foreignRunning}},
			wantErr: ErrNotOwned,
		},
		{
			// An earlier attempt's rename landed but the listing still trails it: the VM is
			// already marked, so it is reported without a second rename.
			name:     "lag: listed fleeting-running fetched fleeting-removing",
			members:  []removalTestMember{{vmid: 100, name: "fleeting-running", fetchedName: "fleeting-removing"}},
			wantSucc: []string{"100"},
		},
		{
			// No name in the listing (Proxmox has no fresh status for the VM): the name on the
			// VM decides, and our own running VM is still renamed.
			name:      "unnamed listing, fetched fleeting-running",
			members:   []removalTestMember{{vmid: 100, name: "", fetchedName: "fleeting-running"}},
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

	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			log, logBuf := newLogBuffer(t)
			counts := &removalRequestCounts{}
			ig := newRemovalTestGroup(t, removalTestServer{
				log:      log,
				members:  tc.members,
				requests: counts,
			})

			succeeded, err := ig.Decrease(context.Background(), []string{"100"})

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			require.ElementsMatch(t, tc.wantSucc, succeeded)

			require.Equal(t, tc.wantPOSTs, counts.configPOSTs.Load(), "config POST count")

			const warnPattern = `\[WARN\].*refusing to remove instance not owned by this group`
			if tc.wantWarn {
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
	members := []removalTestMember{
		{vmid: 100, name: "fleeting-running"},
		{vmid: 101, name: "other-running"},
		{vmid: 102, name: "fleeting-running", fetchedName: "other-running"},
		{vmid: 103, name: "", fetchedName: "fleeting-running"},
	}

	t.Run("ConnectInfo refusal", func(t *testing.T) {
		counts := &removalRequestCounts{}
		ig := newRemovalTestGroup(t, removalTestServer{members: members, requests: counts})

		_, err := ig.ConnectInfo(context.Background(), "101")
		require.ErrorIs(t, err, ErrNotOwned)
		require.Contains(t, err.Error(), "failed to retrieve instance vmid='101'")
		require.Contains(t, err.Error(), "other-running")
		require.Empty(t, counts.requestsFor(101))
	})

	t.Run("Heartbeat refusal", func(t *testing.T) {
		counts := &removalRequestCounts{}
		ig := newRemovalTestGroup(t, removalTestServer{members: members, requests: counts})

		err := ig.Heartbeat(context.Background(), "101")
		require.ErrorIs(t, err, ErrNotOwned)
		require.Contains(t, err.Error(), "failed to retrieve instance vmid='101'")
		require.Contains(t, err.Error(), "other-running")
		require.Empty(t, counts.requestsFor(101))
	})

	t.Run("Resume refusal", func(t *testing.T) {
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
		counts := &removalRequestCounts{}
		ig := newRemovalTestGroup(t, removalTestServer{members: members, requests: counts})

		err := ig.Heartbeat(context.Background(), "100")
		require.NoError(t, err)
		require.True(t, counts.requested(100, "", "agent/get-osinfo"), "vm 100 should have received an agent/get-osinfo request")
	})

	// An unnamed listing is no evidence of a foreign VM: the fetched name decides.
	t.Run("Heartbeat control: unnamed listing", func(t *testing.T) {
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

	for _, tc := range fetchedForeign {
		t.Run(tc.name+" refusal: fetched name", func(t *testing.T) {
			counts := &removalRequestCounts{}
			ig := newRemovalTestGroup(t, removalTestServer{members: members, requests: counts})

			err := tc.call(ig)
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
	log, logBuffer := newLogBuffer(t)
	ig := newIncreaseTestGroup(t, log)

	succeeded, err := ig.Increase(context.Background(), 2)

	require.NoError(t, err, "a partial success reported as an error reaches the provisioner as no instances at all")
	require.Equal(t, 1, succeeded, "the second clone deployed; the first was refused")
	require.Regexp(t, `\[WARN\].*failed to deploy some instances: attempted=2 failed=1`, logBuffer.String())
}
