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

// With vmid_min and vmid_max set, the clone is created with a VMID between them.
func TestCloneTemplateUsesVMIDBounds(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)

	ig, counts := newDeployTestGroup(t, log, deployTestOptions{})
	lower, upper := 5000, 5005
	ig.VMIDMin, ig.VMIDMax = &lower, &upper

	template := deployTestTemplate(t, ig)

	vmid, _, err := ig.cloneTemplate(context.Background(), template)
	require.NoError(t, err)
	ig.stopTrackingDeploying(vmid)

	require.Equal(t, 5000, vmid)

	counts.mu.Lock()
	defer counts.mu.Unlock()

	require.Equal(t, []float64{5000}, counts.cloneNewIDs)
}

// newVMIDRangeTestGroup restricts clones to lower..upper (nil leaves a bound unset), lists
// usedVMIDs as the cluster's guests, and answers the nextid probe of a VMID as taken when it
// is in takenVMIDs, as free otherwise. Every probed VMID is recorded.
func newVMIDRangeTestGroup(t *testing.T, lower, upper *int, usedVMIDs, takenVMIDs []int) (*InstanceGroup, *[]string) {
	t.Helper()

	var (
		probesMu sync.Mutex
		probes   []string
	)

	guests := make([]string, 0, len(usedVMIDs))
	for _, vmid := range usedVMIDs {
		guests = append(guests, fmt.Sprintf(`{"id":"qemu/%d","type":"qemu","vmid":%d,"node":"pve-node"}`, vmid, vmid))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /cluster/resources", func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("type") != "vm" {
			http.Error(writer, "unexpected resources filter", http.StatusInternalServerError)

			return
		}

		fmt.Fprintf(writer, `{"data":[%s]}`, strings.Join(guests, ","))
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
	ig.VMIDMin, ig.VMIDMax = lower, upper
	ig.proxmox = proxmox.NewClient(server.URL)

	return ig, &probes
}

// With vmid_min and vmid_max set, the clone takes the lowest VMID between them that is free
// and not quarantined: VMIDs the cluster lists as used are skipped without a probe, and a
// VMID the plugin cannot see but another client holds is skipped on Proxmox's answer.
func TestAllocateVMIDInRange(t *testing.T) {
	t.Parallel()

	ig, probes := newVMIDRangeTestGroup(t, new(10000), new(10010), []int{10000, 10001}, []int{10002})

	ig.trackFreed(10003)

	vmid, err := ig.allocateVMID(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10004, vmid)
	require.NotContains(t, *probes, "10000", "VMIDs the cluster lists as used need no probe")
	require.NotContains(t, *probes, "", "the range must not fall back to Proxmox's next free VMID")
}

// With only one bound set, the other one is Proxmox's: only vmid_max set starts from VMID 100,
// skipping the cluster's used VMIDs without probing them; only vmid_min set has no upper limit
// short of Proxmox's.
func TestAllocateVMIDWithOneBound(t *testing.T) {
	t.Parallel()

	maxOnly, probes := newVMIDRangeTestGroup(t, nil, new(10000), []int{100, 101, 102}, nil)

	vmid, err := maxOnly.allocateVMID(context.Background())
	require.NoError(t, err)
	require.Equal(t, 103, vmid)
	require.Equal(t, []string{"103"}, *probes)

	minOnly, _ := newVMIDRangeTestGroup(t, new(999999998), nil, []int{999999998}, nil)

	vmid, err = minOnly.allocateVMID(context.Background())
	require.NoError(t, err)
	require.Equal(t, maxProxmoxVMID, vmid)
}

// When every free VMID of the range is quarantined, the one freed the longest ago is reused
// rather than failing the clone; a range with no free VMID at all fails it.
func TestAllocateVMIDInRangeWhenOnlyQuarantinedVMIDsAreFree(t *testing.T) {
	t.Parallel()

	ig, _ := newVMIDRangeTestGroup(t, new(10000), new(10003), []int{10000}, []int{10001})

	ig.trackFreed(10002)
	ig.trackFreed(10003)

	ig.freedMu.Lock()
	ig.freed[10003] = time.Now().Add(-time.Minute)
	ig.freedMu.Unlock()

	vmid, err := ig.allocateVMID(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10003, vmid, "the VMID freed the longest ago must be reused")

	full, _ := newVMIDRangeTestGroup(t, new(10000), new(10002), []int{10000, 10001}, []int{10002})

	_, err = full.allocateVMID(context.Background())
	require.ErrorIs(t, err, ErrVMIDRangeFull)
}

// The probes are bounded: a range whose VMIDs Proxmox all reports as taken, although the
// cluster listing shows none of them, fails after maxVMIDProbes.
func TestAllocateVMIDInRangeBoundsProbes(t *testing.T) {
	t.Parallel()

	taken := make([]int, 0, maxVMIDProbes+1)
	for vmid := 10000; vmid <= 10000+maxVMIDProbes; vmid++ {
		taken = append(taken, vmid)
	}

	ig, probes := newVMIDRangeTestGroup(t, new(10000), nil, nil, taken)

	_, err := ig.allocateVMID(context.Background())
	require.ErrorIs(t, err, ErrVMIDRangeFull)
	require.Len(t, *probes, maxVMIDProbes)
}

// countProbes reports how many times vmid was probed in probes.
func countProbes(probes *[]string, vmid string) int {
	count := 0
	for _, probe := range *probes {
		if probe == vmid {
			count++
		}
	}

	return count
}

// A VMID probed as occupied by a VM the plugin cannot see is remembered briefly, so a burst of
// clones skips re-probing the same foreign VMIDs instead of paying the round-trip for each one;
// a free VMID is never cached and stays re-checked before every clone.
func TestAllocateVMIDInRangeCachesTakenVMIDs(t *testing.T) {
	t.Parallel()

	ig, probes := newVMIDRangeTestGroup(t, new(10000), new(10010), nil, []int{10000, 10001})

	vmid, err := ig.allocateVMID(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10002, vmid)
	require.Equal(t, 1, countProbes(probes, "10000"), "the first clone probes each foreign VMID")
	require.Equal(t, 1, countProbes(probes, "10001"))

	vmid, err = ig.allocateVMID(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10002, vmid)
	require.Equal(t, 1, countProbes(probes, "10000"), "a cached foreign VMID must not be re-probed")
	require.Equal(t, 1, countProbes(probes, "10001"))
	require.Equal(t, 2, countProbes(probes, "10002"), "a free VMID is never cached and stays re-checked")
}

// The taken-VMID memory expires: once takenVMIDCacheTTL has passed, a foreign VMID is probed
// again, so one freed within the window is reconsidered on the next allocation.
func TestAllocateVMIDInRangeRetakesExpiredTakenVMIDs(t *testing.T) {
	t.Parallel()

	ig, probes := newVMIDRangeTestGroup(t, new(10000), new(10010), nil, []int{10000, 10001})

	vmid, err := ig.allocateVMID(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10002, vmid)

	ig.takenMu.Lock()
	ig.taken[10000] = time.Now().Add(-takenVMIDCacheTTL - time.Second)
	ig.taken[10001] = time.Now().Add(-takenVMIDCacheTTL - time.Second)
	ig.takenMu.Unlock()

	vmid, err = ig.allocateVMID(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10002, vmid)
	require.Equal(t, 2, countProbes(probes, "10000"), "an expired foreign VMID must be re-probed")
	require.Equal(t, 2, countProbes(probes, "10001"))
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
