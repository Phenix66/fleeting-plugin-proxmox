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

	hclog "github.com/hashicorp/go-hclog"
	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
)

const (
	deployTestNode       = "pve-node"
	deployTestTemplateID = 200
	deployTestCloneVMID  = 100
)

// deployTestOptions parameterises newDeployTestGroup.
type deployTestOptions struct {
	// failStep names the deploy step the fake makes fail: "clone" (the clone POST is
	// refused), "clone-task" (the clone task reports a failure), "autoresize" (the resize
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
}

// deployTestCounts records what a deploy asked the fake to do, so a test can assert on the
// config writes and resizes a deploy performed.
type deployTestCounts struct {
	mu      sync.Mutex
	configs []map[string]any
	resizes []map[string]any
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
		return func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }
	}

	vmRoute := func(suffix string) string {
		return fmt.Sprintf("/nodes/%s/qemu/{vmid}/%s", deployTestNode, suffix)
	}

	templateRoute := func(suffix string) string {
		return fmt.Sprintf("/nodes/%s/qemu/%d/%s", deployTestNode, deployTestTemplateID, suffix)
	}

	decodeBody := func(t *testing.T, r *http.Request) map[string]any {
		t.Helper()

		body := map[string]any{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("request body: %v", err)
		}

		return body
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /pools/", constant(poolBody))
	mux.HandleFunc("GET /nodes/"+deployTestNode+"/status", constant(`{"data":{}}`))

	mux.HandleFunc("GET /cluster/status", constant(`{"data":[]}`))
	mux.HandleFunc("GET /cluster/nextid", constant(fmt.Sprintf(`{"data":"%d"}`, deployTestCloneVMID)))

	mux.HandleFunc("GET "+templateRoute("status/current"),
		constant(fmt.Sprintf(`{"data":{"vmid":%d,"name":"template","status":"stopped","template":1}}`, deployTestTemplateID)))

	mux.HandleFunc("GET "+vmRoute("status/current"),
		constant(fmt.Sprintf(`{"data":{"vmid":%d,"name":"fleeting-creating","status":"running"}}`, deployTestCloneVMID)))

	mux.HandleFunc("GET "+vmRoute("config"), constant(`{"data":{"tags":"template-tag"}}`))

	mux.HandleFunc("POST "+vmRoute("config"), func(w http.ResponseWriter, r *http.Request) {
		call := int(opts.configCalls.Add(1)) - 1
		if opts.configFailOnCall != nil && call == *opts.configFailOnCall {
			http.Error(w, "config refused", http.StatusInternalServerError)

			return
		}

		counts.mu.Lock()
		counts.configs = append(counts.configs, decodeBody(t, r))
		counts.mu.Unlock()

		fmt.Fprintf(w, `{"data":%q}`, increaseTestUPID("qmconfig", deployTestCloneVMID))
	})

	mux.HandleFunc("PUT "+vmRoute("resize"), func(w http.ResponseWriter, r *http.Request) {
		if opts.failStep == "autoresize" {
			http.Error(w, "resize refused", http.StatusInternalServerError)

			return
		}

		counts.mu.Lock()
		counts.resizes = append(counts.resizes, decodeBody(t, r))
		counts.mu.Unlock()

		fmt.Fprintf(w, `{"data":%q}`, increaseTestUPID("qmresize", deployTestCloneVMID))
	})

	mux.HandleFunc("POST "+templateRoute("clone"), func(w http.ResponseWriter, _ *http.Request) {
		if opts.failStep == "clone" {
			http.Error(w, "clone refused", http.StatusInternalServerError)

			return
		}

		fmt.Fprintf(w, `{"data":%q}`, increaseTestUPID("qmclone", deployTestCloneVMID))
	})

	mux.HandleFunc("POST "+vmRoute("status/start"), func(w http.ResponseWriter, _ *http.Request) {
		if opts.failStep == "start" {
			http.Error(w, "start refused", http.StatusInternalServerError)

			return
		}

		fmt.Fprintf(w, `{"data":%q}`, increaseTestUPID("qmstart", deployTestCloneVMID))
	})

	mux.HandleFunc("GET "+vmRoute("agent/get-osinfo"), func(w http.ResponseWriter, _ *http.Request) {
		if opts.failStep == "agent" {
			http.Error(w, "agent unavailable", http.StatusInternalServerError)

			return
		}

		fmt.Fprint(w, `{"data":{"result":{}}}`)
	})

	mux.HandleFunc("GET /nodes/"+deployTestNode+"/tasks/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/log") {
			fmt.Fprint(w, `{"data":[]}`)

			return
		}

		upid := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/nodes/"+deployTestNode+"/tasks/"), "/status")

		fields := strings.Split(upid, ":")
		if len(fields) < 8 {
			t.Errorf("task route asked for a malformed UPID: %q", upid)
			http.Error(w, "malformed upid", http.StatusBadRequest)

			return
		}

		exitStatus := taskExitStatusOK
		if fields[5] == "qmclone" && opts.failStep == "clone-task" {
			exitStatus = "TASK ERROR: out of disk space"
		}

		fmt.Fprintf(w, `{"data":{"upid":%q,"node":%q,"type":%q,"id":%q,"user":"root@pam","status":%q,"exitstatus":%q}}`,
			upid, deployTestNode, fields[5], fields[6], taskStatusStopped, exitStatus)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	templateID := deployTestTemplateID

	ig := newWaitTestGroup()
	ig.Pool = "test-pool"
	ig.Storage = "local"
	ig.TemplateID = &templateID
	ig.InstanceNameCreating = "fleeting-creating"
	ig.InstanceNameRunning = "fleeting-running"
	ig.InstanceNameRemoving = "fleeting-removing"
	ig.InstanceTagsCreating = "fleeting-creating"
	ig.InstanceTagsRunning = "fleeting-running"
	ig.InstanceTagsRemoving = "fleeting-removing"
	if opts.autoresize {
		ig.InstanceAutoresizeDisk = "scsi1"
		ig.InstanceAutoresizeSize = "10G"
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
	steps := []struct {
		name        string
		failStep    string
		wantErrPart string
	}{
		{name: "resize fails", failStep: "autoresize", wantErrPart: "failed to resize disk"},
		{name: "start fails", failStep: "start", wantErrPart: "failed to start newly deployed instance"},
		{name: "agent never comes up", failStep: "agent", wantErrPart: "failed when waiting for qemu agent"},
	}

	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			log, _ := newLogBuffer(t)

			options := deployTestOptions{failStep: step.failStep}
			if step.failStep == "autoresize" {
				options.autoresize = true
			}

			ig, counts := newDeployTestGroup(t, log, options)

			template := deployTestTemplate(t, ig)

			vmid, err := ig.deployInstance(context.Background(), template, new(sync.Mutex))
			require.Error(t, err)
			require.Contains(t, err.Error(), step.wantErrPart)
			require.Contains(t, err.Error(), "marked for removal")
			require.Equal(t, deployTestCloneVMID, vmid)

			lastConfig, ok := counts.lastConfig()
			require.True(t, ok, "the failed instance was never renamed")
			require.Equal(t, map[string]any{
				vmOptName: "fleeting-removing",
				vmOptTags: "template-tag,fleeting-removing",
			}, lastConfig)
		})
	}
}

// A failure before the VM can be configured -- the clone refused, or its task failing --
// returns without renaming anything, because there is nothing to mark for removal.
func TestDeployInstanceCloneFailureLeavesNoInstanceBehind(t *testing.T) {
	t.Run("clone refused", func(t *testing.T) {
		log, _ := newLogBuffer(t)
		ig, counts := newDeployTestGroup(t, log, deployTestOptions{failStep: "clone"})

		template := deployTestTemplate(t, ig)

		vmid, err := ig.deployInstance(context.Background(), template, new(sync.Mutex))
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to clone the template")
		require.Equal(t, -1, vmid)
		require.Empty(t, counts.allConfigs())
	})

	t.Run("clone task fails", func(t *testing.T) {
		log, _ := newLogBuffer(t)
		ig, counts := newDeployTestGroup(t, log, deployTestOptions{failStep: "clone-task"})

		template := deployTestTemplate(t, ig)

		vmid, err := ig.deployInstance(context.Background(), template, new(sync.Mutex))
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
	log, logBuf := newLogBuffer(t)

	failCall := 1

	ig, counts := newDeployTestGroup(t, log, deployTestOptions{configFailOnCall: &failCall})

	template := deployTestTemplate(t, ig)

	vmid, err := ig.deployInstance(context.Background(), template, new(sync.Mutex))
	require.NoError(t, err)
	require.Equal(t, deployTestCloneVMID, vmid)

	configs := counts.allConfigs()
	require.Len(t, configs, 1, "the failed rename must not be recorded: %v", configs)
	require.Equal(t, map[string]any{
		vmOptTags: "template-tag,fleeting-creating",
	}, configs[0])
	require.Regexp(t, `\[ERROR\].*failed to rename instance`, logBuf.String())
}

// A failure writing the creating tags is reported before the rename, so no rename happens:
// the instance keeps the creating name the clone was given.
func TestDeployInstanceConfigFailureLeavesNoRenameBehind(t *testing.T) {
	log, _ := newLogBuffer(t)

	failCall := 0

	ig, counts := newDeployTestGroup(t, log, deployTestOptions{configFailOnCall: &failCall})

	template := deployTestTemplate(t, ig)

	vmid, err := ig.deployInstance(context.Background(), template, new(sync.Mutex))
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to get instance config")
	require.Equal(t, deployTestCloneVMID, vmid)
	require.Empty(t, counts.allConfigs(), "a config failure must not be renamed")
}

func TestDeployInstanceAutoresize(t *testing.T) {
	log, _ := newLogBuffer(t)
	ig, counts := newDeployTestGroup(t, log, deployTestOptions{autoresize: true})

	template := deployTestTemplate(t, ig)

	vmid, err := ig.deployInstance(context.Background(), template, new(sync.Mutex))
	require.NoError(t, err)
	require.Equal(t, deployTestCloneVMID, vmid)

	resizes := counts.allResizes()
	require.Len(t, resizes, 1)
	require.Equal(t, map[string]any{"disk": "scsi1", "size": "10G"}, resizes[0])

	configs := counts.allConfigs()
	require.Len(t, configs, 2)
	require.Equal(t, map[string]any{
		vmOptTags: "template-tag,fleeting-creating",
	}, configs[0])
	require.Equal(t, map[string]any{
		vmOptName: "fleeting-running",
		vmOptTags: "template-tag,fleeting-running",
	}, configs[1])
}
