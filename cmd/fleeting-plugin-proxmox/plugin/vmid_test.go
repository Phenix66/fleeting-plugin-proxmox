package plugin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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
