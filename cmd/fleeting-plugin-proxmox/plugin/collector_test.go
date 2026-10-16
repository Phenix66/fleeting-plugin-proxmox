package plugin

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
)

const (
	collectorTestNode            = "pve-node"
	collectorTestForeignCreating = "other-creating"
	collectorTestMemberTypeLXC   = "lxc"
	collectorTestPools           = "/pools"
	collectorTestStatusCurrent   = "/status/current"
)

// collectInstance must refuse a VM whose fetched name is not InstanceNameRemoving: the pool
// listing and the node fetch are not atomic, and acting on a stale listing could stop or
// delete a VM we do not own.
func TestCollectInstanceRefusesForeignVM(t *testing.T) {
	t.Parallel()

	type row struct {
		name        string
		fetchedName string
		wantRefuse  bool
	}

	rows := []row{
		{
			// Fetched name is not InstanceNameRemoving: refuse.
			name:        "refuse: fetched name differs from InstanceNameRemoving",
			fetchedName: collectorTestForeignCreating,
			wantRefuse:  true,
		},
		{
			// Our own fresh clone under a recycled VMID the listing still shows as removing:
			// one of our names, but not the one it was listed under, so refuse.
			name:        "refuse: fetched fleeting-creating",
			fetchedName: DefaultInstanceNameCreating,
			wantRefuse:  true,
		},
		{
			// The same recycled VMID once its deploy has finished: a live runner.
			name:        "refuse: fetched fleeting-running",
			fetchedName: DefaultInstanceNameRunning,
			wantRefuse:  true,
		},
		{
			// No override: fetched name equals InstanceNameRemoving; stopped, then deleted.
			name: "control: name matches, stopped and deleted",
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
					{vmid: 100, name: DefaultInstanceNameRemoving, fetchedName: row.fetchedName},
				},
				requests: counts,
			})

			member := proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: DefaultInstanceNameRemoving, Node: collectorTestNode}
			ig.collectInstance(context.Background(), member)

			hasStop := counts.requested(100, http.MethodPost, "/status/stop")
			require.Equal(t, !row.wantRefuse, hasStop, "stop request: expected=%v, requests=%v", !row.wantRefuse, counts.requestsFor(100))

			if row.wantRefuse {
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
	t.Parallel()

	log, logBuf := newLogBuffer(t)
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		log: log,
		members: []removalTestMember{
			{vmid: 100, name: DefaultInstanceNameRemoving, fetchedNameAfterStop: collectorTestForeignCreating},
		},
		requests: counts,
	})

	member := proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: DefaultInstanceNameRemoving, Node: collectorTestNode}
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
	t.Parallel()

	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{
			{vmid: 100, name: DefaultInstanceNameRemoving},
			{vmid: 101, name: DefaultInstanceNameRunning},
			{vmid: 102, name: "other-removing"},
			{vmid: 103, name: DefaultInstanceNameRemoving, memberType: collectorTestMemberTypeLXC},
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

// A creating instance with no deployment in flight is an orphan a failed deployment left: the
// round marks it for removal and wakes the collector for the round that deletes it. One a
// deployment is working on is creating by design: the round leaves it untouched. Removing
// members are collected either way.
func TestCollectRemovedInstancesMarksOrphanedCreatingInstances(t *testing.T) {
	t.Parallel()

	counts := &removalRequestCounts{}

	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{
			{vmid: 100, name: DefaultInstanceNameRemoving},
			{vmid: 101, name: DefaultInstanceNameCreating},
			{vmid: 102, name: DefaultInstanceNameCreating},
		},
		requests: counts,
	})

	// vmid 102 is a deployment in flight: creating by design until its deployment renames it.
	ig.trackDeploying(102)

	ig.collectRemovedInstances()

	require.True(t, counts.requested(101, http.MethodPost, "/config"),
		"the orphaned creating instance was not marked for removal: %v", counts.requestsFor(101))

	require.Empty(t, counts.requestsFor(102),
		"the collector touched an instance a deployment is working on: %v", counts.requestsFor(102))

	require.True(t, counts.requested(100, http.MethodDelete, ""),
		"the removing instance was not deleted: %v", counts.requestsFor(100))

	require.Len(t, ig.instanceCollectionTrigger, 1, "the mark must wake the collector for the round that deletes")
}

// A collection round can name as many members as the pool holds: its per-instance work must
// run under the same batch cap as every other batch, not one goroutine per member.
func TestCollectRemovedInstancesCapsConcurrency(t *testing.T) {
	t.Parallel()

	members := make([]removalTestMember, 0, 2*maxParallelInstanceOperations)

	for vmid := range 2 * maxParallelInstanceOperations {
		members = append(members, removalTestMember{
			vmid: uint64(100 + vmid),
			name: DefaultInstanceNameRemoving,
		})
	}

	block := make(chan struct{})
	inFlight := &atomic.Int64{}
	counts := &removalRequestCounts{}

	ig := newRemovalTestGroup(t, removalTestServer{
		members:               members,
		requests:              counts,
		stopStatusBlock:       block,
		stopStatusConcurrency: inFlight,
	})

	// The sampler tracks the peak in-flight stop count; the stops stay held until the test
	// releases them, so the peak is visible for at least one tick.
	var maxInFlight atomic.Int64

	stopSampling := make(chan struct{})
	sampled := make(chan struct{})

	go func() {
		defer close(sampled)

		for {
			if current := inFlight.Load(); current > maxInFlight.Load() {
				maxInFlight.Store(current)
			}

			select {
			case <-stopSampling:
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()

	done := make(chan struct{})

	go func() {
		ig.collectRemovedInstances()
		close(done)
	}()

	require.Eventually(t, func() bool {
		return maxInFlight.Load() == int64(maxParallelInstanceOperations)
	}, 10*time.Second, 5*time.Millisecond,
		"the round never had a full batch of %d stops in flight", maxParallelInstanceOperations)

	close(block)

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the round did not finish after the stops were released")
	}

	close(stopSampling)
	<-sampled

	require.Equal(t, int64(maxParallelInstanceOperations), maxInFlight.Load(), "more than the batch cap ran at once")

	for _, member := range members {
		id := member.vmid

		require.True(t, counts.requested(id, http.MethodPost, "/status/stop"), "vm %d was not stopped: %v", id, counts.requestsFor(id))
		require.True(t, counts.requested(id, http.MethodDelete, ""), "vm %d was not deleted: %v", id, counts.requestsFor(id))
	}
}

// A member that starts in a later wave of the batch cap must still get the full per-member
// budget for its stop and delete: wave one is held just under the budget, then wave two is
// held past the point a deadline shared by the whole round would have expired.
func TestCollectRemovedInstancesLateWaveKeepsFullBudget(t *testing.T) {
	t.Parallel()

	const (
		memberBudget  = time.Second
		firstWaveHold = 400 * time.Millisecond
		lateWaveHold  = 700 * time.Millisecond
	)

	members := make([]removalTestMember, 0, 2*maxParallelInstanceOperations+1)

	for vmid := range 2*maxParallelInstanceOperations + 1 {
		members = append(members, removalTestMember{
			vmid: uint64(100 + vmid),
			name: DefaultInstanceNameRemoving,
		})
	}

	// Wave one's polls arrive first and are held until firstWaveHold; every later poll is
	// held for lateWaveHold, past firstWaveHold + memberBudget - firstWaveHold, the moment a
	// round-shared deadline started with the round would have expired.
	start := time.Now()

	counts := &removalRequestCounts{}

	ig := newRemovalTestGroup(t, removalTestServer{
		members:  members,
		requests: counts,
		stopStatusHold: func() {
			if elapsed := time.Since(start); elapsed < firstWaveHold {
				time.Sleep(firstWaveHold - elapsed)
			} else {
				time.Sleep(lateWaveHold)
			}
		},
	})

	ig.collectionMemberTimeout = memberBudget

	done := make(chan struct{})

	go func() {
		ig.collectRemovedInstances()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the collection round did not finish")
	}

	for _, member := range members {
		require.True(t, counts.requested(member.vmid, http.MethodDelete, ""),
			"vm %d was not deleted: %v", member.vmid, counts.requestsFor(member.vmid))
	}
}

func TestCollectRemovedInstancesPoolFetchFailure(t *testing.T) {
	t.Parallel()

	log, logBuf := newLogBuffer(t)
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		log:       log,
		failPaths: []string{collectorTestPools},
		requests:  counts,
	})

	ig.collectRemovedInstances()

	require.Regexp(t, `\[ERROR\].*collector failed to list instances`, logBuf.String())
	require.Empty(t, counts.requestsFor(100), "a pool fetch failure must not reach the members")
}

// While the cloning lock is held -- an in-flight Increase -- a collection round must not
// wait it out: it skips without listing the pool or touching any member, re-queues its
// wake-up, and returns, so a Shutdown waiting on the collector cannot be held hostage by
// the clone.
func TestCollectRoundSkipsWhileCloning(t *testing.T) {
	t.Parallel()

	log, logBuf := newLogBuffer(t)
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		log:      log,
		members:  []removalTestMember{{vmid: 100, name: DefaultInstanceNameRemoving}},
		requests: counts,
	})

	// Shorten the skip bound on this group only, so the test does not wait a real skip.
	ig.collectionLockWait = 100 * time.Millisecond

	ig.instanceCloningMu.Lock()
	t.Cleanup(ig.instanceCloningMu.Unlock)

	done := make(chan struct{})

	go func() {
		ig.collectRemovedInstances()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the collection round did not skip a held cloning lock")
	}

	require.Zero(t, counts.poolListings(), "a skipped round must not list the pool")
	require.Empty(t, counts.requestsFor(100), "a skipped round must not reach the members")
	require.Contains(t, logBuf.String(), "skipping round")
	require.Len(t, ig.instanceCollectionTrigger, 1, "a skipped round must re-queue its wake-up")
}

// A clone that releases the lock before the skip bound lets the round proceed: the round
// waits for the lock, then lists and collects as usual. The skip is for in-flight clones,
// not for every lock hold.
func TestCollectRoundWaitsOutBriefCloneHold(t *testing.T) {
	t.Parallel()

	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		members:  []removalTestMember{{vmid: 100, name: DefaultInstanceNameRemoving}},
		requests: counts,
	})

	ig.instanceCloningMu.Lock()
	time.Sleep(2 * collectionLockPoll)
	ig.instanceCloningMu.Unlock()

	ig.collectRemovedInstances()

	require.True(t, counts.requested(100, http.MethodPost, "/status/stop"), "the marked instance was not stopped: %v", counts.requestsFor(100))
	require.True(t, counts.requested(100, http.MethodDelete, ""), "the marked instance was not deleted: %v", counts.requestsFor(100))
}

// A Shutdown racing an in-flight Increase -- the collector blocked on the cloning lock the
// Increase holds -- must return once the round gives up the lock wait, not wait for the
// clone to finish.
func TestShutdownReturnsWhileCloneHoldsLock(t *testing.T) {
	t.Parallel()

	ig := newRemovalTestGroup(t, removalTestServer{})
	ig.resetLifecycle()

	// Shorten both bounds on this group only so the test cycles fast instead of waiting
	// real skips.
	ig.collectionLockWait = 100 * time.Millisecond
	ig.collectionTriggerWait = 50 * time.Millisecond

	ig.startRemovedInstanceCollector()

	// Hold the cloning lock as an in-flight Increase would, and wake the collector so its
	// round hits the held lock.
	ig.instanceCloningMu.Lock()
	t.Cleanup(ig.instanceCloningMu.Unlock)

	ig.triggerCollection()

	// Let the collector reach the lock wait: trigger sleep, then the round's bounded wait.
	time.Sleep(400 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ig.Shutdown(ctx)

	require.NoError(t, err, "Shutdown must return while a clone holds the cloning lock")
}

// fetchToCollect logs a failed fetch that is not an ownership refusal, so a VM that cannot
// be read is not dropped silently.
func TestFetchToCollectLogsFailedFetch(t *testing.T) {
	t.Parallel()

	log, logBuf := newLogBuffer(t)
	ig := newRemovalTestGroup(t, removalTestServer{
		log:       log,
		failPaths: []string{collectorTestStatusCurrent},
	})

	member := &proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: DefaultInstanceNameRemoving, Node: collectorTestNode}

	vm, ok := ig.fetchToCollect(context.Background(), member)
	require.False(t, ok)
	require.Nil(t, vm)
	require.Regexp(t, `\[ERROR\].*collector failed to fetch instance info`, logBuf.String())
}

// A stop whose task fails must not be followed by a delete: the delete is the irreversible
// step, and it happens only after a verified stop.
func TestCollectInstanceStopTaskFailureSkipsDelete(t *testing.T) {
	t.Parallel()

	log, logBuf := newLogBuffer(t)
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		log: log,
		members: []removalTestMember{
			{vmid: 100, name: DefaultInstanceNameRemoving, stopFails: true},
		},
		requests: counts,
	})

	member := proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: DefaultInstanceNameRemoving, Node: collectorTestNode}
	ig.collectInstance(context.Background(), member)

	require.True(t, counts.requested(100, http.MethodPost, "/status/stop"))
	require.False(t, counts.requested(100, http.MethodDelete, ""), "a VM whose stop failed was deleted: %v", counts.requestsFor(100))
	require.Regexp(t, `\[ERROR\].*collector failed to stop instance`, logBuf.String())
}

// A delete whose task fails is logged, not swallowed: the instance stays for the next run.
func TestCollectInstanceDeleteTaskFailureIsLogged(t *testing.T) {
	t.Parallel()

	log, logBuf := newLogBuffer(t)
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		log: log,
		members: []removalTestMember{
			{vmid: 100, name: DefaultInstanceNameRemoving, deleteFails: true},
		},
		requests: counts,
	})

	member := proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: DefaultInstanceNameRemoving, Node: collectorTestNode}
	ig.collectInstance(context.Background(), member)

	require.True(t, counts.requested(100, http.MethodDelete, ""), "the delete was not attempted: %v", counts.requestsFor(100))
	require.Regexp(t, `\[ERROR\].*collector failed to delete instance`, logBuf.String())
}

// A VM that is already stopped is deleted without a stop: the stop exists so the name can be
// re-checked after the VM has been made safe, not to stop a stopped VM.
func TestCollectInstanceSkipsStopForStoppedVM(t *testing.T) {
	t.Parallel()

	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{
			{vmid: 100, name: DefaultInstanceNameRemoving, vmStatus: "stopped"},
		},
		requests: counts,
	})

	member := proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: DefaultInstanceNameRemoving, Node: collectorTestNode}
	ig.collectInstance(context.Background(), member)

	require.False(t, counts.requested(100, http.MethodPost, "/status/stop"), "a stopped VM was asked to stop: %v", counts.requestsFor(100))
	require.True(t, counts.requested(100, http.MethodDelete, ""), "the stopped VM was not deleted: %v", counts.requestsFor(100))
}

// A malformed UPID makes the vendor client panic inside the collector's per-instance worker
// while building the stop task: the panic must be recovered there, or it takes the whole
// plugin process down.
func TestCollectRemovedInstancesRecoversWorkerPanic(t *testing.T) {
	t.Parallel()

	log, logBuf := newLogBuffer(t)
	counts := &removalRequestCounts{}

	// Seven colon-separated fields: the vendor's NewTask passes its length guard and then
	// indexes the eighth, panicking on exactly this shape.
	ig := newRemovalTestGroup(t, removalTestServer{
		log:      log,
		members:  []removalTestMember{{vmid: 100, name: DefaultInstanceNameRemoving}},
		stopUpID: "UPID:pve-node:00001A2B:00000000:00000000:qmstop:100",
		requests: counts,
	})

	ig.collectRemovedInstances()

	require.True(t, counts.requested(100, http.MethodPost, "/status/stop"), "the stop was not attempted: %v", counts.requestsFor(100))
	require.False(t, counts.requested(100, http.MethodDelete, ""), "a VM whose stop panicked was deleted: %v", counts.requestsFor(100))
	require.Regexp(t, `\[ERROR\].*collector panicked while removing an instance`, logBuf.String())
}

// The collection trigger is how markInstancesForRemoval wakes the collector: a token on the
// channel starts another collection run without waiting for the interval.
func TestCollectorTriggerWakesCollector(t *testing.T) {
	t.Parallel()

	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{
			{vmid: 100, name: DefaultInstanceNameRemoving},
		},
		requests: counts,
	})

	ig.startRemovedInstanceCollector()

	stopCount := func() int {
		count := 0

		for _, request := range counts.requestsFor(100) {
			if strings.HasPrefix(request, http.MethodPost+" ") && strings.Contains(request, "/status/stop") {
				count++
			}
		}

		return count
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
	t.Parallel()

	ig := newRemovalTestGroup(t, removalTestServer{})

	ig.startRemovedInstanceCollector()

	requireShutdownReturns(t, ig, "with the collector running")
}

// A Shutdown whose context expires before the workers drain must return the context error
// instead of waiting: a collector blocked on a stuck Proxmox pool fetch must not hold the
// shutdown hostage.
func TestShutdownReturnsOnContextExpiration(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})

	poolFetches := &atomic.Int64{}

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasPrefix(request.URL.Path, "/pools") {
			poolFetches.Add(1)
			<-release

			fmt.Fprint(writer, `{"data":[]}`)

			return
		}

		http.NotFound(writer, request)
	}))

	// Cleanups run LIFO: release the hung pool fetch before closing the server, or
	// server.Close() blocks waiting for the still-active connection.
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	ig := &InstanceGroup{}
	ig.resetLifecycle()
	ig.log = hclog.NewNullLogger()
	ig.Pool = "test-pool"
	ig.proxmox = proxmox.NewClient(server.URL)

	ig.startRemovedInstanceCollector()

	require.Eventually(t, func() bool { return poolFetches.Load() > 0 }, 2*time.Second, 10*time.Millisecond,
		"the collector never started its first pool fetch")

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := ig.Shutdown(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 2*time.Second, "Shutdown did not return when the context expired")
}

// An empty channel accepts the wake-up: triggerCollection never drops it.
func TestTriggerCollectionSendsOnEmptyChannel(t *testing.T) {
	t.Parallel()

	ig := &InstanceGroup{instanceCollectionTrigger: make(chan struct{}, 1)}

	ig.triggerCollection()

	require.Len(t, ig.instanceCollectionTrigger, 1)
}

func TestTriggerCollectionOnFullChannelReturnsImmediately(t *testing.T) {
	t.Parallel()

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
