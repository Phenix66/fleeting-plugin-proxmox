package plugin

import (
	"context"
	"net/http"
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
