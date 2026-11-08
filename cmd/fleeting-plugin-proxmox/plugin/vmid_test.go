package plugin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
)

// A VMID the collector has just deleted is the one Proxmox hands to the next clone. While it is
// quarantined, the clone must take the next free VMID above it instead, so the provisioner
// never matches the new instance to the deleted one's record.
func TestCloneTemplateSkipsQuarantinedVMID(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)

	ig, counts := newDeployTestGroup(t, log, deployTestOptions{})

	template := deployTestTemplate(t, ig)

	ig.trackFreed(deployTestCloneVMID)

	vmid, task, err := ig.cloneTemplate(context.Background(), template)
	require.NoError(t, err)
	require.NotNil(t, task)
	ig.stopTrackingDeploying(vmid)

	require.Equal(t, deployTestCloneVMID+1, vmid, "the clone reused a quarantined VMID")

	counts.mu.Lock()
	defer counts.mu.Unlock()

	require.Equal(t, []float64{deployTestCloneVMID + 1}, counts.cloneNewIDs)
}

// With nothing quarantined the clone leaves the VMID to the vendor, which asks Proxmox for the
// next free one, exactly as before.
func TestCloneTemplateWithoutQuarantineLeavesTheVMIDToProxmox(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)

	ig, counts := newDeployTestGroup(t, log, deployTestOptions{})

	template := deployTestTemplate(t, ig)

	vmid, _, err := ig.cloneTemplate(context.Background(), template)
	require.NoError(t, err)
	ig.stopTrackingDeploying(vmid)

	require.Equal(t, deployTestCloneVMID, vmid)

	counts.mu.Lock()
	defer counts.mu.Unlock()

	require.Equal(t, []float64{deployTestCloneVMID}, counts.cloneNewIDs)
}

// nextUnquarantinedVMID skips quarantined VMIDs and VMIDs Proxmox reports as taken, and gives
// up on any other probe failure.
func TestNextUnquarantinedVMID(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /cluster/nextid", func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Query().Get("vmid") {
		case "":
			fmt.Fprint(writer, `{"data":"109"}`)
		case "110":
			writer.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(writer, `{"data":null,"errors":{"vmid":"VM 110 already exists"}}`)
		case "111":
			fmt.Fprint(writer, `{"data":"111"}`)
		default:
			http.Error(writer, "unexpected probe", http.StatusInternalServerError)
		}
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	ig := newWaitTestGroup()
	ig.proxmox = proxmox.NewClient(server.URL)

	vmid, err := ig.nextUnquarantinedVMID(context.Background())
	require.NoError(t, err)
	require.Zero(t, vmid, "with nothing quarantined the choice is left to the vendor")

	ig.trackFreed(109)

	vmid, err = ig.nextUnquarantinedVMID(context.Background())
	require.NoError(t, err)
	require.Equal(t, 111, vmid, "109 is quarantined and 110 is taken")

	ig.trackFreed(111)

	_, err = ig.nextUnquarantinedVMID(context.Background())
	require.Error(t, err, "a probe Proxmox fails for another reason must fail the allocation")

	ig.freedMu.Lock()
	for freedVMID := range ig.freed {
		ig.freed[freedVMID] = time.Now().Add(-freedVMIDQuarantine)
	}
	ig.freedMu.Unlock()

	require.False(t, ig.isQuarantined(109), "the quarantine must end")

	vmid, err = ig.nextUnquarantinedVMID(context.Background())
	require.NoError(t, err)
	require.Zero(t, vmid, "an expired quarantine must leave the choice to the vendor again")
}

// With vmid_range set, the clone is created with a VMID of the range.
func TestCloneTemplateUsesVMIDRange(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)

	ig, counts := newDeployTestGroup(t, log, deployTestOptions{})
	ig.VMIDRange = "5000-5005"

	template := deployTestTemplate(t, ig)

	vmid, _, err := ig.cloneTemplate(context.Background(), template)
	require.NoError(t, err)
	ig.stopTrackingDeploying(vmid)

	require.Equal(t, 5000, vmid)

	counts.mu.Lock()
	defer counts.mu.Unlock()

	require.Equal(t, []float64{5000}, counts.cloneNewIDs)
}

// newVMIDRangeTestGroup serves a pool holding poolVMIDs and answers the nextid probe of a
// VMID as taken when it is in takenVMIDs, as free otherwise. Every probed VMID is recorded.
func newVMIDRangeTestGroup(t *testing.T, vmidRange string, poolVMIDs, takenVMIDs []int) (*InstanceGroup, *[]string) {
	t.Helper()

	var (
		probesMu sync.Mutex
		probes   []string
	)

	members := make([]string, 0, len(poolVMIDs))
	for _, vmid := range poolVMIDs {
		members = append(members, fmt.Sprintf(`{"vmid":%d,"type":"qemu","name":"fleeting-running","node":"pve-node"}`, vmid))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /pools/", func(writer http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(writer, `{"data":[{"poolid":"test-pool","members":[%s]}]}`, strings.Join(members, ","))
	})
	mux.HandleFunc("GET /cluster/nextid", func(writer http.ResponseWriter, request *http.Request) {
		probe := request.URL.Query().Get("vmid")

		probesMu.Lock()

		probes = append(probes, probe)
		probesMu.Unlock()

		vmid, err := strconv.Atoi(probe)
		if err != nil {
			http.Error(writer, "unexpected nextid request", http.StatusInternalServerError)

			return
		}

		if slices.Contains(takenVMIDs, vmid) {
			writer.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(writer, `{"data":null,"errors":{"vmid":"VM %d already exists"}}`, vmid)

			return
		}

		fmt.Fprintf(writer, `{"data":"%d"}`, vmid)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	ig := newWaitTestGroup()
	ig.Pool = removalTestPool
	ig.VMIDRange = vmidRange
	ig.proxmox = proxmox.NewClient(server.URL)

	return ig, &probes
}

// With vmid_range set, the clone takes the lowest VMID of the range that is free and not
// quarantined: the group's own instances are skipped without a probe, and a VMID another
// manager holds is skipped on Proxmox's answer.
func TestAllocateVMIDInRange(t *testing.T) {
	t.Parallel()

	ig, probes := newVMIDRangeTestGroup(t, "10000-10010", []int{10000, 10001}, []int{10002})

	ig.trackFreed(10003)

	vmid, err := ig.allocateVMID(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10004, vmid)
	require.NotContains(t, *probes, "10000", "the group's own instances need no probe")
	require.NotContains(t, *probes, "", "the range must not fall back to Proxmox's next free VMID")
}

// When every free VMID of the range is quarantined, the one freed the longest ago is reused
// rather than failing the clone; a range with no free VMID at all fails it.
func TestAllocateVMIDInRangeWhenOnlyQuarantinedVMIDsAreFree(t *testing.T) {
	t.Parallel()

	ig, _ := newVMIDRangeTestGroup(t, "10000-10003", []int{10000}, []int{10001})

	ig.trackFreed(10002)
	ig.trackFreed(10003)

	ig.freedMu.Lock()
	ig.freed[10003] = time.Now().Add(-time.Minute)
	ig.freedMu.Unlock()

	vmid, err := ig.allocateVMID(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10003, vmid, "the VMID freed the longest ago must be reused")

	full, _ := newVMIDRangeTestGroup(t, "10000-10002", []int{10000, 10001}, []int{10002})

	_, err = full.allocateVMID(context.Background())
	require.ErrorIs(t, err, ErrVMIDRangeFull)
}

// Proxmox cannot reserve a VMID, so another client can take the one a clone was about to use.
// The clone it then refuses must be retried with the next free VMID instead of failing the
// deployment.
func TestCloneTemplateRetriesVMIDTakenInTheMeantime(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)

	ig, counts := newDeployTestGroup(t, log, deployTestOptions{takenCloneVMIDs: 2})

	template := deployTestTemplate(t, ig)

	vmid, task, err := ig.cloneTemplate(context.Background(), template)
	require.NoError(t, err)
	require.NotNil(t, task)
	ig.stopTrackingDeploying(vmid)

	require.Equal(t, deployTestCloneVMID+2, vmid)

	counts.mu.Lock()
	defer counts.mu.Unlock()

	require.Equal(t, []float64{deployTestCloneVMID, deployTestCloneVMID + 1, deployTestCloneVMID + 2}, counts.cloneNewIDs)
}

// The retry is bounded: a clone refused as taken on every attempt fails.
func TestCloneTemplateGivesUpAfterMaxCloneAttempts(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)

	ig, counts := newDeployTestGroup(t, log, deployTestOptions{takenCloneVMIDs: maxCloneAttempts})

	template := deployTestTemplate(t, ig)

	_, _, err := ig.cloneTemplate(context.Background(), template)
	require.ErrorContains(t, err, "config file already exists")
	require.Equal(t, maxCloneAttempts, counts.cloneCount())
}

// A clone refused for any other reason is not retried.
func TestCloneTemplateDoesNotRetryOtherCloneFailures(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)

	ig, _ := newDeployTestGroup(t, log, deployTestOptions{failStep: deployTestFailStepClone})

	template := deployTestTemplate(t, ig)

	_, _, err := ig.cloneTemplate(context.Background(), template)
	require.ErrorContains(t, err, "failed to clone the template")
}

var (
	errTestVMIDTaken   = errors.New("500 unable to create VM 109: config file already exists")
	errTestCloneFailed = errors.New("500 Internal Server Error")
)

func TestIsVMIDTakenError(t *testing.T) {
	t.Parallel()

	require.True(t, isVMIDTakenError(errTestVMIDTaken, 109))
	require.False(t, isVMIDTakenError(errTestVMIDTaken, 110), "the refusal is about another VMID")
	require.False(t, isVMIDTakenError(errTestCloneFailed, 109))
	require.False(t, isVMIDTakenError(nil, 109))
}
