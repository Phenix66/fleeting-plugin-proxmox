package plugin

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
)

// collectInstance must refuse a VM whose fetched name is not InstanceNameRemoving: the pool
// listing and the node fetch are not atomic, and acting on a stale listing could stop or
// delete a VM we do not own.
func TestCollectInstanceRefusesForeignVM(t *testing.T) {
	type row struct {
		name        string
		fetchedName string
		wantRefuse  bool
	}

	rows := []row{
		{
			// Fetched name is not InstanceNameRemoving: refuse.
			name:        "refuse: fetched name differs from InstanceNameRemoving",
			fetchedName: "other-creating",
			wantRefuse:  true,
		},
		{
			// Our own fresh clone under a recycled VMID the listing still shows as removing:
			// one of our names, but not the one it was listed under, so refuse.
			name:        "refuse: fetched fleeting-creating",
			fetchedName: "fleeting-creating",
			wantRefuse:  true,
		},
		{
			// The same recycled VMID once its deploy has finished: a live runner.
			name:        "refuse: fetched fleeting-running",
			fetchedName: "fleeting-running",
			wantRefuse:  true,
		},
		{
			// No override: fetched name equals InstanceNameRemoving; stopped, then deleted.
			name: "control: name matches, stopped and deleted",
		},
	}

	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			log, logBuf := newLogBuffer(t)
			counts := &removalRequestCounts{}
			ig := newRemovalTestGroup(t, removalTestServer{
				log: log,
				members: []removalTestMember{
					{vmid: 100, name: "fleeting-removing", fetchedName: tc.fetchedName},
				},
				requests: counts,
			})

			member := proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: "fleeting-removing", Node: "pve-node"}
			ig.collectInstance(context.Background(), member)

			hasStop := counts.requested(100, http.MethodPost, "/status/stop")
			require.Equal(t, !tc.wantRefuse, hasStop, "stop request: expected=%v, requests=%v", !tc.wantRefuse, counts.requestsFor(100))
			if tc.wantRefuse {
				require.Regexp(t, `\[WARN\]`, logBuf.String(), "expected Warn for foreign VM")
				require.NotContains(t, logBuf.String(), "[ERROR]", "a refusal is expected under VMID reuse, not an error")
				require.Equal(t, []string{
					"GET /nodes/pve-node/qemu/100/status/current",
					"GET /nodes/pve-node/qemu/100/config",
				}, counts.requestsFor(100), "a refused VM must get no request beyond the fetch")
			} else {
				require.NotContains(t, logBuf.String(), "refusing to act on VM", "unexpected ownership Warn")
				require.True(t, counts.requested(100, http.MethodDelete, ""), "owned VM was not deleted: %v", counts.requestsFor(100))
			}
		})
	}
}

// collectInstance must check the name again after waiting for a running VM to stop: the stop
// takes at least one task poll, long enough for the VM to be deleted and its VMID reused.
func TestCollectInstanceRechecksNameAfterStop(t *testing.T) {
	log, logBuf := newLogBuffer(t)
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		log: log,
		members: []removalTestMember{
			{vmid: 100, name: "fleeting-removing", fetchedNameAfterStop: "other-creating"},
		},
		requests: counts,
	})

	member := proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: "fleeting-removing", Node: "pve-node"}
	ig.collectInstance(context.Background(), member)

	require.True(t, counts.requested(100, http.MethodPost, "/status/stop"), "the owned VM was not stopped: %v", counts.requestsFor(100))
	require.False(t, counts.requested(100, http.MethodDelete, ""), "a VM renamed during the stop was deleted: %v", counts.requestsFor(100))
	require.Contains(t, logBuf.String(), "refusing to act on VM")
	require.NotContains(t, logBuf.String(), "[ERROR]")
}

// collectRemovedInstances stops and deletes every pool member listed under
// InstanceNameRemoving and touches nothing else: not a member under another name, not one
// that is not a VM at all.
func TestCollectRemovedInstances(t *testing.T) {
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{
			{vmid: 100, name: "fleeting-removing"},
			{vmid: 101, name: "fleeting-running"},
			{vmid: 102, name: "other-removing"},
			{vmid: 103, name: "fleeting-removing", memberType: "lxc"},
		},
		requests: counts,
	})

	ig.collectRemovedInstances()

	require.True(t, counts.requested(100, http.MethodPost, "/status/stop"), "the marked instance was not stopped: %v", counts.requestsFor(100))
	require.True(t, counts.requested(100, http.MethodDelete, ""), "the marked instance was not deleted: %v", counts.requestsFor(100))
	require.Empty(t, counts.requestsFor(101), "an unmarked instance was touched")
	require.Empty(t, counts.requestsFor(102), "another manager's instance was touched")
	require.Empty(t, counts.requestsFor(103), "a non-VM pool member was touched")
}

func TestCollectRemovedInstancesPoolFetchFailure(t *testing.T) {
	log, logBuf := newLogBuffer(t)
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		log:       log,
		failPaths: []string{"/pools"},
		requests:  counts,
	})

	ig.collectRemovedInstances()

	require.Regexp(t, `\[ERROR\].*collector failed to list instances`, logBuf.String())
	require.Empty(t, counts.requestsFor(100), "a pool fetch failure must not reach the members")
}

// fetchToCollect logs a failed fetch that is not an ownership refusal, so a VM that cannot
// be read is not dropped silently.
func TestFetchToCollectLogsFailedFetch(t *testing.T) {
	log, logBuf := newLogBuffer(t)
	ig := newRemovalTestGroup(t, removalTestServer{
		log:       log,
		failPaths: []string{"/status/current"},
	})

	member := &proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: "fleeting-removing", Node: "pve-node"}

	vm, ok := ig.fetchToCollect(context.Background(), member)
	require.False(t, ok)
	require.Nil(t, vm)
	require.Regexp(t, `\[ERROR\].*collector failed to fetch instance info`, logBuf.String())
}

// A stop whose task fails must not be followed by a delete: the delete is the irreversible
// step, and it happens only after a verified stop.
func TestCollectInstanceStopTaskFailureSkipsDelete(t *testing.T) {
	log, logBuf := newLogBuffer(t)
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		log: log,
		members: []removalTestMember{
			{vmid: 100, name: "fleeting-removing", stopFails: true},
		},
		requests: counts,
	})

	member := proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: "fleeting-removing", Node: "pve-node"}
	ig.collectInstance(context.Background(), member)

	require.True(t, counts.requested(100, http.MethodPost, "/status/stop"))
	require.False(t, counts.requested(100, http.MethodDelete, ""), "a VM whose stop failed was deleted: %v", counts.requestsFor(100))
	require.Regexp(t, `\[ERROR\].*collector failed to stop instance`, logBuf.String())
}

// A delete whose task fails is logged, not swallowed: the instance stays for the next run.
func TestCollectInstanceDeleteTaskFailureIsLogged(t *testing.T) {
	log, logBuf := newLogBuffer(t)
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		log: log,
		members: []removalTestMember{
			{vmid: 100, name: "fleeting-removing", deleteFails: true},
		},
		requests: counts,
	})

	member := proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: "fleeting-removing", Node: "pve-node"}
	ig.collectInstance(context.Background(), member)

	require.True(t, counts.requested(100, http.MethodDelete, ""), "the delete was not attempted: %v", counts.requestsFor(100))
	require.Regexp(t, `\[ERROR\].*collector failed to delete instance`, logBuf.String())
}

// A VM that is already stopped is deleted without a stop: the stop exists so the name can be
// re-checked after the VM has been made safe, not to stop a stopped VM.
func TestCollectInstanceSkipsStopForStoppedVM(t *testing.T) {
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{
			{vmid: 100, name: "fleeting-removing", vmStatus: "stopped"},
		},
		requests: counts,
	})

	member := proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: "fleeting-removing", Node: "pve-node"}
	ig.collectInstance(context.Background(), member)

	require.False(t, counts.requested(100, http.MethodPost, "/status/stop"), "a stopped VM was asked to stop: %v", counts.requestsFor(100))
	require.True(t, counts.requested(100, http.MethodDelete, ""), "the stopped VM was not deleted: %v", counts.requestsFor(100))
}

// The collection trigger is how markInstancesForRemoval wakes the collector: a token on the
// channel starts another collection run without waiting for the interval.
func TestCollectorTriggerWakesCollector(t *testing.T) {
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{
			{vmid: 100, name: "fleeting-removing"},
		},
		requests: counts,
	})

	ig.startRemovedInstanceCollector()

	stopCount := func() int {
		n := 0

		for _, request := range counts.requestsFor(100) {
			if strings.HasPrefix(request, http.MethodPost+" ") && strings.Contains(request, "/status/stop") {
				n++
			}
		}

		return n
	}

	// The run the collector does on start serves the first stop.
	require.Eventually(t, func() bool { return stopCount() >= 1 }, 2*time.Second, 10*time.Millisecond,
		"the collector never collected the marked instance")

	// The fake's pool still lists the VM, so the wake-up starts a second run: the trigger
	// arm serves it after collectionWaitAfterTrigger.
	ig.triggerCollection()

	require.Eventually(t, func() bool { return stopCount() >= 2 }, 15*time.Second, 100*time.Millisecond,
		"the trigger did not start another collection run")
}

// The collector loop returns once the shutdown trigger fires, so Shutdown cannot hang on it.
func TestCollectorStopsOnShutdown(t *testing.T) {
	ig := newRemovalTestGroup(t, removalTestServer{})

	ig.startRemovedInstanceCollector()

	requireShutdownReturns(t, ig, "with the collector running")
}

// An empty channel accepts the wake-up: triggerCollection never drops it.
func TestTriggerCollectionSendsOnEmptyChannel(t *testing.T) {
	ig := &InstanceGroup{instanceCollectionTrigger: make(chan struct{}, 1)}

	ig.triggerCollection()

	require.Len(t, ig.instanceCollectionTrigger, 1)
}

func TestTriggerCollectionOnFullChannelReturnsImmediately(t *testing.T) {
	ig := &InstanceGroup{
		instanceCollectionTrigger: make(chan struct{}, 1),
	}

	// Fill the channel so a blocking send would stall forever.
	ig.instanceCollectionTrigger <- struct{}{}

	done := make(chan struct{})

	go func() {
		ig.triggerCollection()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("triggerCollection blocked on a full channel")
	}
}
