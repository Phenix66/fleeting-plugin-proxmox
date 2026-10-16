package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
)

const (
	deployTestNode               = "pve-node"
	deployTestTemplateID         = 200
	deployTestCloneVMID          = 100
	deployTestPool               = "test-pool"
	deployTestStorage            = "local"
	deployTestFailStepAutoresize = "autoresize"
	deployTestAutoresizeSize     = "10G"
	deployTestAutoresizeDisk     = "scsi1"
	deployTestTagsCreating       = "template-tag,fleeting-creating"
	deployTestDiskSize           = "1G"
)

// deployTestOptions parameterises newDeployTestGroup.
type deployTestOptions struct {
	// failStep names the deploy step the fake makes fail: "clone" (the clone POST is
	// refused), "clone-task" (the clone task reports a failure), deployTestFailStepAutoresize (the resize
	// PUT is refused), "start" (the start POST is refused) or "agent" (the agent never
	// answers). Empty means every step succeeds.
	failStep string

	// autoresize makes the group autoresize a disk, so the deploy exercises the resize step
	// at all.
	autoresize bool

	// configFailOnCall, when non-nil, is the zero-based index of the config POST the fake
	// refuses.
	configFailOnCall *int

	configCalls *atomic.Int64

	// resizeNotApplied makes the resize task succeed without the disk growing, the way a
	// resize Proxmox answers without a task can leave it.
	resizeNotApplied bool

	// storageAvail is the free space, in bytes, the fake reports for the storage.
	storageAvail uint64

	// poolBodies, when non-empty, are served one per pool request, the last one repeating;
	// they let a test simulate a listing that lags the clone the way Proxmox's pool can.
	poolBodies []string
}

// deployTestCounts records what a deploy asked the fake to do, so a test can assert on the
// config writes and resizes a deploy performed.
type deployTestCounts struct {
	mu          sync.Mutex
	configs     []map[string]any
	resizes     []map[string]any
	clones      int
	poolsServed atomic.Int64
}

func (c *deployTestCounts) poolCount() int64 {
	return c.poolsServed.Load()
}

func (c *deployTestCounts) cloneCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.clones
}

func (c *deployTestCounts) lastConfig() (map[string]any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.configs) == 0 {
		return nil, false
	}

	return c.configs[len(c.configs)-1], true
}

func (c *deployTestCounts) allConfigs() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]map[string]any(nil), c.configs...)
}

func (c *deployTestCounts) allResizes() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]map[string]any(nil), c.resizes...)
}

// registerDeployPoolRoute serves the pool's route: the static body, or, when opts.poolBodies
// is non-empty, one body per request, the last repeating -- so a test can simulate a listing
// that lags the clone the way Proxmox's pool can. Every request is counted in counts.
func registerDeployPoolRoute(mux *http.ServeMux, opts deployTestOptions, counts *deployTestCounts, staticBody string) {
	mux.HandleFunc("GET /pools/", func(writer http.ResponseWriter, _ *http.Request) {
		requests := int(counts.poolsServed.Add(1))

		body := staticBody

		if len(opts.poolBodies) > 0 {
			index := requests - 1
			if index >= len(opts.poolBodies) {
				index = len(opts.poolBodies) - 1
			}

			body = opts.poolBodies[index]
		}

		fmt.Fprint(writer, body)
	})
}

// newDeployTestGroup wires an InstanceGroup to an httptest Proxmox serving the whole deploy
// path -- nextid, clone, task, config, resize, start, agent -- for the template and for the
// one VM cloned from it, failing the single step named by opts.
func newDeployTestGroup(t *testing.T, log hclog.Logger, opts deployTestOptions) (*InstanceGroup, *deployTestCounts) {
	t.Helper()

	counts := &deployTestCounts{}

	if opts.configCalls == nil {
		opts.configCalls = &atomic.Int64{}
	}

	poolBody := fmt.Sprintf(`{"data":[{"poolid":"test-pool","members":[%s,%s]}]}`,
		fmt.Sprintf(`{"vmid":%d,"type":"qemu","name":"template","node":%q}`, deployTestTemplateID, deployTestNode),
		fmt.Sprintf(`{"vmid":%d,"type":"qemu","name":"fleeting-creating","node":%q}`, deployTestCloneVMID, deployTestNode))

	constant := func(body string) http.HandlerFunc {
		return func(writer http.ResponseWriter, _ *http.Request) { fmt.Fprint(writer, body) }
	}

	vmRoute := func(suffix string) string {
		return fmt.Sprintf("/nodes/%s/qemu/{vmid}/%s", deployTestNode, suffix)
	}

	templateRoute := func(suffix string) string {
		return fmt.Sprintf("/nodes/%s/qemu/%d/%s", deployTestNode, deployTestTemplateID, suffix)
	}

	decodeBody := func(t *testing.T, request *http.Request) map[string]any {
		t.Helper()

		body := map[string]any{}

		err := json.NewDecoder(request.Body).Decode(&body)
		if err != nil {
			t.Errorf("request body: %v", err)
		}

		return body
	}

	mux := http.NewServeMux()

	registerDeployPoolRoute(mux, opts, counts, poolBody)

	mux.HandleFunc("GET /nodes/"+deployTestNode+"/status", constant(`{"data":{}}`))

	mux.HandleFunc("GET /cluster/status", constant(`{"data":[]}`))
	mux.HandleFunc("GET /cluster/nextid", constant(fmt.Sprintf(`{"data":"%d"}`, deployTestCloneVMID)))

	mux.HandleFunc("GET "+templateRoute("status/current"),
		constant(fmt.Sprintf(`{"data":{"vmid":%d,"name":"template","status":"stopped","template":1}}`, deployTestTemplateID)))

	mux.HandleFunc("GET "+vmRoute("status/current"),
		constant(fmt.Sprintf(`{"data":{"vmid":%d,"name":"fleeting-creating","status":"running"}}`, deployTestCloneVMID)))

	// The template and its clone both carry the autoresize disk; the clone's grows when the
	// fake applies a resize.
	var (
		diskSizeMu sync.Mutex
		diskSize   = deployTestDiskSize
	)

	mux.HandleFunc("GET "+vmRoute("config"), func(writer http.ResponseWriter, _ *http.Request) {
		diskSizeMu.Lock()
		defer diskSizeMu.Unlock()

		fmt.Fprintf(writer, `{"data":{"tags":"template-tag","ide2":"none,media=cdrom","%s":"local:vm-%d-disk-0,size=%s"}}`,
			deployTestAutoresizeDisk, deployTestCloneVMID, diskSize)
	})

	mux.HandleFunc("GET /nodes/"+deployTestNode+"/storage/"+deployTestStorage+"/status",
		constant(fmt.Sprintf(`{"data":{"storage":%q,"avail":%d,"total":%d}}`, deployTestStorage, opts.storageAvail, opts.storageAvail)))

	mux.HandleFunc("POST "+vmRoute("config"), func(writer http.ResponseWriter, request *http.Request) {
		call := int(opts.configCalls.Add(1)) - 1
		if opts.configFailOnCall != nil && call == *opts.configFailOnCall {
			http.Error(writer, "config refused", http.StatusInternalServerError)

			return
		}

		counts.mu.Lock()
		counts.configs = append(counts.configs, decodeBody(t, request))
		counts.mu.Unlock()

		fmt.Fprintf(writer, `{"data":%q}`, increaseTestUPID("qmconfig", deployTestCloneVMID))
	})

	mux.HandleFunc("PUT "+vmRoute("resize"), func(writer http.ResponseWriter, request *http.Request) {
		if opts.failStep == deployTestFailStepAutoresize {
			http.Error(writer, "resize refused", http.StatusInternalServerError)

			return
		}

		body := decodeBody(t, request)

		counts.mu.Lock()
		counts.resizes = append(counts.resizes, body)
		counts.mu.Unlock()

		if !opts.resizeNotApplied {
			diskSizeMu.Lock()
			diskSize, _ = body["size"].(string)
			diskSizeMu.Unlock()
		}

		fmt.Fprintf(writer, `{"data":%q}`, increaseTestUPID("qmresize", deployTestCloneVMID))
	})

	mux.HandleFunc("POST "+templateRoute("clone"), func(writer http.ResponseWriter, _ *http.Request) {
		if opts.failStep == "clone" {
			http.Error(writer, "clone refused", http.StatusInternalServerError)

			return
		}

		counts.mu.Lock()
		counts.clones++
		counts.mu.Unlock()

		fmt.Fprintf(writer, `{"data":%q}`, increaseTestUPID("qmclone", deployTestCloneVMID))
	})

	mux.HandleFunc("POST "+vmRoute("status/start"), func(writer http.ResponseWriter, _ *http.Request) {
		if opts.failStep == "start" {
			http.Error(writer, "start refused", http.StatusInternalServerError)

			return
		}

		fmt.Fprintf(writer, `{"data":%q}`, increaseTestUPID("qmstart", deployTestCloneVMID))
	})

	mux.HandleFunc("GET "+vmRoute("agent/get-osinfo"), func(writer http.ResponseWriter, _ *http.Request) {
		if opts.failStep == "agent" {
			http.Error(writer, "agent unavailable", http.StatusInternalServerError)

			return
		}

		fmt.Fprint(writer, `{"data":{"result":{}}}`)
	})

	mux.HandleFunc("GET /nodes/"+deployTestNode+"/tasks/", func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/log") {
			fmt.Fprint(writer, `{"data":[]}`)

			return
		}

		upid := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/nodes/"+deployTestNode+"/tasks/"), "/status")

		fields := strings.Split(upid, ":")
		if len(fields) < 8 {
			t.Errorf("task route asked for a malformed UPID: %q", upid)
			http.Error(writer, "malformed upid", http.StatusBadRequest)

			return
		}

		exitStatus := taskExitStatusOK
		if fields[5] == "qmclone" && opts.failStep == "clone-task" {
			exitStatus = "TASK ERROR: out of disk space"
		}

		//nolint:gosec//G705 // test-only mock server; the upid is validated above before being written
		fmt.Fprintf(writer, `{"data":{"upid":%q,"node":%q,"type":%q,"id":%q,"user":"root@pam","status":%q,"exitstatus":%q}}`,
			upid, deployTestNode, fields[5], fields[6], taskStatusStopped, exitStatus)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	templateID := deployTestTemplateID

	ig := newWaitTestGroup()
	ig.Pool = deployTestPool
	ig.Storage = deployTestStorage
	ig.TemplateID = &templateID
	ig.InstanceNameCreating = DefaultInstanceNameCreating
	ig.InstanceNameRunning = DefaultInstanceNameRunning
	ig.InstanceNameRemoving = DefaultInstanceNameRemoving
	ig.InstanceTagsCreating = DefaultInstanceNameCreating
	ig.InstanceTagsRunning = DefaultInstanceNameRunning
	ig.InstanceTagsRemoving = DefaultInstanceNameRemoving

	if opts.autoresize {
		ig.InstanceAutoresizeDisk = deployTestAutoresizeDisk
		ig.InstanceAutoresizeSize = deployTestAutoresizeSize
	}

	ig.log = log
	ig.proxmox = proxmox.NewClient(server.URL)

	return ig, counts
}

// deployTestTemplate fetches the template the way Increase does, so deployInstance starts
// from the VM the fake reports it for.
func deployTestTemplate(t *testing.T, ig *InstanceGroup) *proxmox.VirtualMachine {
	t.Helper()

	template, err := ig.getProxmoxVM(context.Background(), *ig.TemplateID)
	require.NoError(t, err)

	return template
}

// A deploy that fails after the VM exists -- resize, start or agent -- must rename it to the
// removing name with the removing tags, so the collector picks it up, and report the failure.
func TestDeployInstanceMarksFailedInstanceForRemoval(t *testing.T) {
	t.Parallel()

	steps := []struct {
		name        string
		failStep    string
		wantErrPart string
	}{
		{name: "resize fails", failStep: deployTestFailStepAutoresize, wantErrPart: "failed to resize disk"},
		{name: "start fails", failStep: "start", wantErrPart: "failed to start newly deployed instance"},
		{name: "agent never comes up", failStep: "agent", wantErrPart: "failed when waiting for qemu agent"},
	}

	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			t.Parallel()

			log, _ := newLogBuffer(t)

			options := deployTestOptions{failStep: step.failStep}
			if step.failStep == deployTestFailStepAutoresize {
				options.autoresize = true
			}

			ig, counts := newDeployTestGroup(t, log, options)

			template := deployTestTemplate(t, ig)

			vmid, err := ig.deployInstance(context.Background(), template)
			require.Error(t, err)
			require.Contains(t, err.Error(), step.wantErrPart)
			require.Contains(t, err.Error(), "marked for removal")
			require.Equal(t, deployTestCloneVMID, vmid)

			lastConfig, ok := counts.lastConfig()
			require.True(t, ok, "the failed instance was never renamed")
			require.Equal(t, map[string]any{
				vmOptName: DefaultInstanceNameRemoving,
				vmOptTags: "template-tag,fleeting-removing",
			}, lastConfig)
		})
	}
}

// A failure before the VM can be configured -- the clone refused, or its task failing --
// returns without renaming anything, because there is nothing to mark for removal.
func TestDeployInstanceCloneFailureLeavesNoInstanceBehind(t *testing.T) {
	t.Parallel()

	t.Run("clone refused", func(t *testing.T) {
		t.Parallel()

		log, _ := newLogBuffer(t)
		ig, counts := newDeployTestGroup(t, log, deployTestOptions{failStep: "clone"})

		template := deployTestTemplate(t, ig)

		vmid, err := ig.deployInstance(context.Background(), template)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to clone the template")
		require.Equal(t, -1, vmid)
		require.Empty(t, counts.allConfigs())
	})

	t.Run("clone task fails", func(t *testing.T) {
		t.Parallel()

		log, _ := newLogBuffer(t)
		ig, counts := newDeployTestGroup(t, log, deployTestOptions{failStep: "clone-task"})

		template := deployTestTemplate(t, ig)

		vmid, err := ig.deployInstance(context.Background(), template)
		require.Error(t, err)
		require.Contains(t, err.Error(), "TASK ERROR: out of disk space")
		require.Equal(t, deployTestCloneVMID, vmid)
		require.Empty(t, counts.allConfigs(), "a failed clone task must not be configured or renamed")
	})
}

// A deploy that fully succeeded but whose final rename failed is reported as a success: the
// instance is up and the failure is logged. It keeps the creating name, so the next Init's
// stale sweep marks it for removal.
func TestDeployInstanceSurvivesFinalRenameFailure(t *testing.T) {
	t.Parallel()

	log, logBuf := newLogBuffer(t)

	failCall := 1

	ig, counts := newDeployTestGroup(t, log, deployTestOptions{configFailOnCall: &failCall})

	template := deployTestTemplate(t, ig)

	vmid, err := ig.deployInstance(context.Background(), template)
	require.NoError(t, err)
	require.Equal(t, deployTestCloneVMID, vmid)

	configs := counts.allConfigs()
	require.Len(t, configs, 1, "the failed rename must not be recorded: %v", configs)
	require.Equal(t, map[string]any{
		vmOptTags: deployTestTagsCreating,
	}, configs[0])
	require.Regexp(t, `\[ERROR\].*failed to rename instance`, logBuf.String())
}

// A failure writing the creating tags is reported before the rename, so no rename happens:
// the instance keeps the creating name the clone was given.
func TestDeployInstanceConfigFailureLeavesNoRenameBehind(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)

	failCall := 0

	ig, counts := newDeployTestGroup(t, log, deployTestOptions{configFailOnCall: &failCall})

	template := deployTestTemplate(t, ig)

	vmid, err := ig.deployInstance(context.Background(), template)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to set creating tags on instance")
	require.Equal(t, deployTestCloneVMID, vmid)
	require.Empty(t, counts.allConfigs(), "a config failure must not be renamed")
}

func TestDeployInstanceAutoresize(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)
	ig, counts := newDeployTestGroup(t, log, deployTestOptions{autoresize: true})

	template := deployTestTemplate(t, ig)

	vmid, err := ig.deployInstance(context.Background(), template)
	require.NoError(t, err)
	require.Equal(t, deployTestCloneVMID, vmid)

	resizes := counts.allResizes()
	require.Len(t, resizes, 1)
	require.Equal(t, map[string]any{"disk": deployTestAutoresizeDisk, "size": deployTestAutoresizeSize}, resizes[0])

	configs := counts.allConfigs()
	require.Len(t, configs, 2)
	require.Equal(t, map[string]any{
		vmOptTags: deployTestTagsCreating,
	}, configs[0])
	require.Equal(t, map[string]any{
		vmOptName: DefaultInstanceNameRunning,
		vmOptTags: "template-tag,fleeting-running",
	}, configs[1])
}

// A resize whose task succeeds but leaves the disk at its old size must fail the deploy and
// mark the instance for removal, never report an undersized instance as running.
func TestDeployInstanceAutoresizeNotApplied(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)
	ig, counts := newDeployTestGroup(t, log, deployTestOptions{autoresize: true, resizeNotApplied: true})

	template := deployTestTemplate(t, ig)

	vmid, err := ig.deployInstance(context.Background(), template)
	require.ErrorIs(t, err, ErrAutoresizeNotApplied)
	require.Contains(t, err.Error(), "marked for removal")
	require.Equal(t, deployTestCloneVMID, vmid)

	lastConfig, ok := counts.lastConfig()
	require.True(t, ok, "the failed instance was never renamed")
	require.Equal(t, DefaultInstanceNameRemoving, lastConfig[vmOptName])
}

// With storage_reserve set, Increase clones only the instances whose disks fit on the storage
// next to the reserve, counting the autoresize disk at its resized size, and reports a demand
// that does not fit at all as nothing created rather than as an error.
func TestIncreaseClonesOnlyWhatFitsOnStorage(t *testing.T) {
	t.Parallel()

	const gib = 1 << 30

	testCases := []struct {
		name       string
		avail      uint64
		requested  int
		wantClones int
	}{
		// Each instance takes 10G (scsi1 resized from 1G); the cdrom takes nothing.
		{name: "nothing fits", avail: 14 * gib, requested: 1, wantClones: 0},
		{name: "one fits", avail: 15 * gib, requested: 1, wantClones: 1},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			log, logBuffer := newLogBuffer(t)
			ig, counts := newDeployTestGroup(t, log, deployTestOptions{autoresize: true, storageAvail: testCase.avail})
			ig.StorageReserve = "5G"

			succeeded, err := ig.Increase(context.Background(), testCase.requested)
			require.NoError(t, err)
			require.Equal(t, testCase.wantClones, succeeded)
			require.Equal(t, testCase.wantClones, counts.cloneCount())

			if testCase.wantClones < testCase.requested {
				require.Regexp(t, `\[WARN\].*not enough free space on storage`, logBuffer.String())
			}
		})
	}
}

// deployTestTemplateMember and deployTestCloneMember are the pool member rows the fixture's
// pool carries, so a test can build a lagging listing from the same data.
func deployTestTemplateMember() string {
	return fmt.Sprintf(`{"vmid":%d,"type":"qemu","name":"template","node":%q}`, deployTestTemplateID, deployTestNode)
}

func deployTestCloneMember() string {
	return fmt.Sprintf(`{"vmid":%d,"type":"qemu","name":"%s","node":%q}`, deployTestCloneVMID, DefaultInstanceNameCreating, deployTestNode)
}

// deployTestPoolBody builds a pool body the way the fixture does, from the members it names.
func deployTestPoolBody(members ...string) string {
	return fmt.Sprintf(`{"data":[{"poolid":"%s","members":[%s]}]}`, deployTestPool, strings.Join(members, ","))
}

// The clone task can finish before Proxmox's pool listing carries the new VM: the post-clone
// lookup retries until the VM is visible, and a lagging listing must not fail a deployment
// whose clone actually succeeded.
func TestDeployInstanceRetriesWhileListingsLagTheClone(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)

	ig, counts := newDeployTestGroup(t, log, deployTestOptions{
		// The first two pool requests lag: the template fetch, then the lookup's first
		// attempt. The third carries the clone.
		poolBodies: []string{
			deployTestPoolBody(deployTestTemplateMember()),
			deployTestPoolBody(deployTestTemplateMember()),
			deployTestPoolBody(deployTestTemplateMember(), deployTestCloneMember()),
		},
	})

	template := deployTestTemplate(t, ig)

	start := time.Now()

	vmid, err := ig.deployInstance(context.Background(), template)
	elapsed := time.Since(start)

	require.NoError(t, err, "a lagging listing must not fail a deployment whose clone succeeded")
	require.Equal(t, deployTestCloneVMID, vmid)
	require.GreaterOrEqual(t, counts.poolCount(), int64(3), "the lookup never retried the lagging listing")
	require.GreaterOrEqual(t, elapsed, deployedVMLookupInterval, "the retry did not wait an interval between attempts")
	require.Equal(t, 1, counts.cloneCount(), "the lookup retry re-cloned the template")
}

// A VM the listings never show fails the deployment once the lookup window elapses: the
// retries are bounded, so a genuinely missing instance cannot hold its slot in the batch.
func TestDeployInstanceFailsWhenListingsNeverShowTheClone(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)

	ig, _ := newDeployTestGroup(t, log, deployTestOptions{
		poolBodies: []string{deployTestPoolBody(deployTestTemplateMember())},
	})

	// Shorten the window on this group only, so the test does not wait a real one.
	ig.deployedVMLookupWindow = 300 * time.Millisecond

	template := deployTestTemplate(t, ig)

	start := time.Now()

	vmid, err := ig.deployInstance(context.Background(), template)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to find newly deployed instance")
	require.Equal(t, deployTestCloneVMID, vmid)
	require.Less(t, elapsed, deployedVMLookupInterval, "the lookup outlived its window")
}

// A cancelled context ends the lookup retries at once, not at the next interval: the retry
// sleep must be interruptible, or a Shutdown could sit out a full interval per in-flight
// deployment.
func TestDeployInstanceLookupStopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)

	ig, _ := newDeployTestGroup(t, log, deployTestOptions{
		poolBodies: []string{deployTestPoolBody(deployTestTemplateMember())},
	})

	template := deployTestTemplate(t, ig)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()

	_, err := ig.deployInstance(ctx, template)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, elapsed, deployedVMLookupInterval, "the cancellation was noticed only at the next attempt")
}

// instance_cores and instance_memory are written to the clone before it is started, in their
// own config call; unset, no such call is made (see TestDeployInstanceAutoresize).
func TestDeployInstanceSetsCoresAndMemory(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)
	ig, counts := newDeployTestGroup(t, log, deployTestOptions{})

	cores, memory := 8, 12288
	ig.InstanceCores = &cores
	ig.InstanceMemory = &memory

	template := deployTestTemplate(t, ig)

	_, err := ig.deployInstance(context.Background(), template)
	require.NoError(t, err)

	configs := counts.allConfigs()
	require.Len(t, configs, 3)
	require.Equal(t, map[string]any{vmOptCores: float64(cores), vmOptMemory: float64(memory)}, configs[1])
	require.Equal(t, DefaultInstanceNameRunning, configs[2][vmOptName])
}
