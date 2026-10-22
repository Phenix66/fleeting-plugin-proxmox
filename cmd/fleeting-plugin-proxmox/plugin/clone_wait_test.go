package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
)

// cloneWaitPVE is an in-memory Proxmox that models the cluster behaviour the static deploy
// fakes omit: a clone task keeps running after the plugin's wait on it has given up, and
// for the rest of its run the template refuses new clones and the half-cloned VM refuses
// configuration, start and deletion. Aborting a clone task removes its half-cloned VM, the
// way qmclone cleans up its target.
type cloneWaitPVE struct {
	mu       sync.Mutex
	nextID   int
	cloneDur time.Duration
	vms      map[int]*cloneWaitVM
	tasks    map[string]*cloneWaitTask
	clones   int
	aborts   int
}

type cloneWaitVM struct {
	vmid   int
	name   string
	tags   string
	status string
}

type cloneWaitTask struct {
	upid      string
	vmid      int
	taskType  string
	startedAt time.Time
	duration  time.Duration
	abortedAt time.Time
}

// stoppedAt is when the task settles: on its own duration, or shortly after an abort.
func (task *cloneWaitTask) stoppedAt() time.Time {
	if !task.abortedAt.IsZero() {
		return task.abortedAt.Add(100 * time.Millisecond)
	}

	return task.startedAt.Add(task.duration)
}

// status reports the task's outcome as of now.
func (task *cloneWaitTask) status(now time.Time) (string, string) {
	if now.Before(task.stoppedAt()) {
		return taskStatusRunning, ""
	}

	if !task.abortedAt.IsZero() {
		return taskStatusStopped, "TASK ERROR: task was aborted"
	}

	return taskStatusStopped, taskExitStatusOK
}

func newCloneWaitPVE(cloneDur time.Duration) *cloneWaitPVE {
	return &cloneWaitPVE{
		nextID:   100,
		cloneDur: cloneDur,
		vms:      map[int]*cloneWaitVM{},
		tasks:    map[string]*cloneWaitTask{},
	}
}

// setCloneDuration changes how long the next clone task runs, so a test can slow the first
// deployment down and let the next one finish quickly.
func (p *cloneWaitPVE) setCloneDuration(dur time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.cloneDur = dur
}

type cloneWaitCounts struct {
	clones int
	aborts int
}

func (p *cloneWaitPVE) counts() cloneWaitCounts {
	p.mu.Lock()
	defer p.mu.Unlock()

	return cloneWaitCounts{clones: p.clones, aborts: p.aborts}
}

// names lists the non-template pool members' names, sorted.
func (p *cloneWaitPVE) names() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	names := make([]string, 0, len(p.vms))
	for _, vm := range p.vms {
		names = append(names, vm.name)
	}

	sort.Strings(names)

	return names
}

// locked reports whether a task is still running on vmid. Callers hold p.mu.
func (p *cloneWaitPVE) locked(vmid int) bool {
	now := time.Now()

	for _, task := range p.tasks {
		if task.vmid == vmid && now.Before(task.stoppedAt()) {
			return true
		}
	}

	return false
}

// clone allocates a VMID and starts a clone task against the template, refusing the request
// while another clone task is in flight, the way the locked template answers it. It returns
// the task's UPID, or the refusal the locked template answers with. Callers hold p.mu.
func (p *cloneWaitPVE) clone() (string, string) {
	now := time.Now()

	for _, task := range p.tasks {
		if task.taskType == testTaskTypeClone && now.Before(task.stoppedAt()) {
			return "", fmt.Sprintf("VM %d is locked by task %s", deployTestTemplateID, task.upid)
		}
	}

	vmid := p.nextID
	p.nextID++

	upid := fmt.Sprintf("UPID:%s:%08X:%08X:%08X:%s:%d:root@pam:", deployTestNode, 0x0001A2B, 0, 0, testTaskTypeClone, vmid)

	p.tasks[upid] = &cloneWaitTask{upid: upid, vmid: vmid, taskType: testTaskTypeClone, startedAt: now, duration: p.cloneDur}
	p.vms[vmid] = &cloneWaitVM{vmid: vmid, name: DefaultInstanceNameCreating, status: taskStatusStopped}
	p.clones++

	return upid, ""
}

// newTask records a short-lived task on vmid that settles immediately. Callers hold p.mu.
func (p *cloneWaitPVE) newTask(vmid int, taskType string) string {
	upid := fmt.Sprintf("UPID:%s:%08X:%08X:%08X:%s:%d:root@pam:", deployTestNode, 0x0001A2B, 0, 0, taskType, vmid)

	p.tasks[upid] = &cloneWaitTask{upid: upid, vmid: vmid, taskType: taskType, startedAt: time.Now()}

	return upid
}

// handler assembles the fake's routes. Each route answers from its own method, so the
// registration list stays a table of the fake's behaviour and every handler fits the
// complexity limit on its own.
func (p *cloneWaitPVE) handler() http.HandlerFunc {
	vmPath := "/nodes/" + deployTestNode + "/qemu/{vmid}"

	mux := http.NewServeMux()

	mux.HandleFunc("GET /pools/", p.handlePool)
	mux.HandleFunc("GET /cluster/status", p.handleClusterStatus)
	mux.HandleFunc("GET /cluster/nextid", p.handleNextID)
	mux.HandleFunc("GET /nodes/"+deployTestNode+"/status", p.handleNodeStatus)
	mux.HandleFunc("POST /nodes/"+deployTestNode+"/qemu/"+strconv.Itoa(deployTestTemplateID)+"/clone", p.handleClone)
	mux.HandleFunc("GET /nodes/"+deployTestNode+"/tasks/", p.handleTaskStatus)
	mux.HandleFunc("DELETE /nodes/"+deployTestNode+"/tasks/{upid}", p.handleAbortTask)
	mux.HandleFunc("GET "+vmPath+"/status/current", p.handleVMStatus)
	mux.HandleFunc("GET "+vmPath+"/config", p.handleVMGetConfig)
	mux.HandleFunc("POST "+vmPath+"/config", p.handleVMSetConfig)
	mux.HandleFunc("POST "+vmPath+"/status/start", p.handleVMStart)
	mux.HandleFunc("GET "+vmPath+"/agent/get-osinfo", p.handleAgentOSInfo)
	mux.HandleFunc("DELETE "+vmPath, p.handleVMDelete)

	return mux.ServeHTTP
}

// handlePool lists the pool members: the template and every cloned VM, ordered by VMID.
func (p *cloneWaitPVE) handlePool(writer http.ResponseWriter, _ *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()

	vmidToBody := map[int]string{
		deployTestTemplateID: fmt.Sprintf(`{"vmid":%d,"type":"qemu","name":"template","node":%q}`, deployTestTemplateID, deployTestNode),
	}

	for _, vm := range p.vms {
		vmidToBody[vm.vmid] = fmt.Sprintf(`{"vmid":%d,"type":"qemu","name":%q,"node":%q}`, vm.vmid, vm.name, deployTestNode)
	}

	vmid := make([]int, 0, len(vmidToBody))
	for id := range vmidToBody {
		vmid = append(vmid, id)
	}

	sort.Ints(vmid)

	bodies := make([]string, 0, len(vmid))
	for _, id := range vmid {
		bodies = append(bodies, vmidToBody[id])
	}

	fmt.Fprintf(writer, `{"data":[{"poolid":%q,"members":[%s]}]}`, deployTestPool, strings.Join(bodies, ","))
}

// handleClusterStatus answers the cluster's task list with an empty one.
func (p *cloneWaitPVE) handleClusterStatus(writer http.ResponseWriter, _ *http.Request) {
	fmt.Fprint(writer, `{"data":[]}`)
}

// handleNextID answers the cluster's next free VMID.
func (p *cloneWaitPVE) handleNextID(writer http.ResponseWriter, _ *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()

	fmt.Fprintf(writer, `{"data":"%d"}`, p.nextID)
}

// handleNodeStatus answers the node's status with an empty body.
func (p *cloneWaitPVE) handleNodeStatus(writer http.ResponseWriter, _ *http.Request) {
	fmt.Fprint(writer, `{"data":{}}`)
}

// handleClone starts a clone task, or refuses it with the locked template's answer while a
// clone is in flight.
func (p *cloneWaitPVE) handleClone(writer http.ResponseWriter, _ *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()

	upid, refused := p.clone()
	if refused != "" {
		http.Error(writer, refused, http.StatusInternalServerError)

		return
	}

	fmt.Fprintf(writer, `{"data":%q}`, upid)
}

// handleTaskStatus answers a task's status and log routes.
func (p *cloneWaitPVE) handleTaskStatus(writer http.ResponseWriter, request *http.Request) {
	path := request.URL.Path

	if strings.HasSuffix(path, "/log") {
		fmt.Fprint(writer, `{"data":[]}`)

		return
	}

	upid := strings.TrimSuffix(strings.TrimPrefix(path, "/nodes/"+deployTestNode+"/tasks/"), "/status")

	p.mu.Lock()
	defer p.mu.Unlock()

	task, ok := p.tasks[upid]
	if !ok {
		http.Error(writer, "task not found", http.StatusNotFound)

		return
	}

	status, exitStatus := task.status(time.Now())

	fmt.Fprintf(writer, `{"data":{"upid":%q,"node":%q,"type":%q,"id":"%d","user":"root@pam","status":%q,"exitstatus":%q}}`, //nolint:gosec//G705 // test fake rendering the task's status as JSON
		upid, deployTestNode, task.taskType, task.vmid, status, exitStatus)
}

// handleAbortTask aborts a running clone, whose half-cloned target is removed the way
// qmclone removes it.
func (p *cloneWaitPVE) handleAbortTask(writer http.ResponseWriter, request *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()

	task, ok := p.tasks[request.PathValue("upid")]
	if !ok {
		http.Error(writer, "task not found", http.StatusNotFound)

		return
	}

	if task.taskType == testTaskTypeClone && time.Now().Before(task.stoppedAt()) {
		task.abortedAt = time.Now()

		// qmclone removes its half-cloned target when it is aborted.
		delete(p.vms, task.vmid)
		p.aborts++
	}

	fmt.Fprint(writer, `{"data":null}`)
}

// handleVMStatus answers a VM's status/current route; the template answers for itself.
func (p *cloneWaitPVE) handleVMStatus(writer http.ResponseWriter, request *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()

	vmid, _ := strconv.Atoi(request.PathValue("vmid"))

	if vmid == deployTestTemplateID {
		fmt.Fprintf(writer, `{"data":{"vmid":%d,"name":"template","status":"stopped","template":1}}`, deployTestTemplateID)

		return
	}

	vm, ok := p.vms[vmid]
	if !ok {
		http.Error(writer, "VM not found", http.StatusNotFound)

		return
	}

	fmt.Fprintf(writer, `{"data":{"vmid":%d,"name":%q,"status":%q}}`, vm.vmid, vm.name, vm.status)
}

// handleVMGetConfig answers a VM's config route; the template answers for itself.
func (p *cloneWaitPVE) handleVMGetConfig(writer http.ResponseWriter, request *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()

	vmid, _ := strconv.Atoi(request.PathValue("vmid"))

	if vmid == deployTestTemplateID {
		fmt.Fprint(writer, `{"data":{"vmid":200,"name":"template","tags":"template-tag"}}`)

		return
	}

	vm, ok := p.vms[vmid]
	if !ok {
		http.Error(writer, "VM not found", http.StatusNotFound)

		return
	}

	fmt.Fprintf(writer, `{"data":{"vmid":%d,"name":%q,"tags":%q}}`, vm.vmid, vm.name, vm.tags)
}

// handleVMSetConfig applies a config change, refused while a task still runs on the VM.
func (p *cloneWaitPVE) handleVMSetConfig(writer http.ResponseWriter, request *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()

	vmid, _ := strconv.Atoi(request.PathValue("vmid"))

	vm, ok := p.vms[vmid]
	if !ok {
		http.Error(writer, "VM not found", http.StatusNotFound)

		return
	}

	if p.locked(vmid) {
		http.Error(writer, fmt.Sprintf("VM %d is locked", vmid), http.StatusInternalServerError)

		return
	}

	body := map[string]any{}

	err := json.NewDecoder(request.Body).Decode(&body)
	if err != nil {
		http.Error(writer, "bad request body", http.StatusBadRequest)

		return
	}

	if name, ok := body["name"].(string); ok {
		vm.name = name
	}

	if tags, ok := body["tags"].(string); ok {
		vm.tags = tags
	}

	// A config change on a stopped VM applies synchronously and carries no task.
	fmt.Fprint(writer, `{"data":null}`)
}

// handleVMStart starts the VM, refused while a task still runs on it.
func (p *cloneWaitPVE) handleVMStart(writer http.ResponseWriter, request *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()

	vmid, _ := strconv.Atoi(request.PathValue("vmid"))

	vm, ok := p.vms[vmid]
	if !ok {
		http.Error(writer, "VM not found", http.StatusNotFound)

		return
	}

	if p.locked(vmid) {
		http.Error(writer, fmt.Sprintf("VM %d is locked", vmid), http.StatusInternalServerError)

		return
	}

	vm.status = taskStatusRunning

	fmt.Fprintf(writer, `{"data":%q}`, p.newTask(vmid, "qmstart"))
}

// handleAgentOSInfo answers the guest agent's presence probe.
func (p *cloneWaitPVE) handleAgentOSInfo(writer http.ResponseWriter, request *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()

	vmid, _ := strconv.Atoi(request.PathValue("vmid"))

	vm, ok := p.vms[vmid]
	if ok && vm.status == taskStatusRunning {
		fmt.Fprint(writer, `{"data":{"result":{}}}`)

		return
	}

	http.Error(writer, "QEMU guest agent is not running", http.StatusInternalServerError)
}

// handleVMDelete destroys the VM, refused while a task still runs on it.
func (p *cloneWaitPVE) handleVMDelete(writer http.ResponseWriter, request *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()

	vmid, _ := strconv.Atoi(request.PathValue("vmid"))

	vm, ok := p.vms[vmid]
	if !ok {
		http.Error(writer, "VM not found", http.StatusNotFound)

		return
	}

	if p.locked(vmid) {
		http.Error(writer, fmt.Sprintf("VM %d is locked", vmid), http.StatusInternalServerError)

		return
	}

	delete(p.vms, vm.vmid)

	fmt.Fprintf(writer, `{"data":%q}`, p.newTask(vmid, "qmdestroy"))
}

// newCloneWaitGroup wires an InstanceGroup to the fake. The poll interval is passed coarser
// than the clone wait timeout on purpose: a timed-out clone is then resolved by the
// post-timeout settlement check, not by a poll that would have missed the task's outcome.
func newCloneWaitGroup(t *testing.T, pve *cloneWaitPVE, log hclog.Logger, intervalSec, taskTimeoutSec, cloneTimeoutSec int) *InstanceGroup {
	t.Helper()

	server := httptest.NewServer(pve.handler())
	t.Cleanup(server.Close)

	ig := newWaitTestGroup()
	ig.Pool = deployTestPool
	ig.Storage = deployTestStorage

	templateID := deployTestTemplateID
	ig.TemplateID = &templateID
	ig.InstanceNameCreating = DefaultInstanceNameCreating
	ig.InstanceNameRunning = DefaultInstanceNameRunning
	ig.InstanceNameRemoving = DefaultInstanceNameRemoving
	ig.InstanceTagsCreating = DefaultInstanceNameCreating
	ig.InstanceTagsRunning = DefaultInstanceNameRunning
	ig.InstanceTagsRemoving = DefaultInstanceNameRemoving

	ig.ProxmoxTaskWaitInterval = &intervalSec
	ig.ProxmoxTaskWaitTimeout = &taskTimeoutSec
	ig.ProxmoxCloneWaitTimeout = &cloneTimeoutSec

	ig.log = log
	ig.proxmox = proxmox.NewClient(server.URL)

	return ig
}

// A clone task that outlives the clone wait timeout must not be abandoned: the deployment
// aborts it, so it leaves no locked, unpowered creating instance behind, and the template
// is free for the next clone as soon as the deployment returns.
func TestSlowCloneIsAbortedWhenTheCloneWaitTimesOut(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)

	pve := newCloneWaitPVE(6 * time.Second)
	ig := newCloneWaitGroup(t, pve, log, 5, 5, 1)

	ctx := context.Background()

	start := time.Now()
	succeeded, err := ig.Increase(ctx, 1)

	require.Error(t, err, "a clone that outlives the wait timeout must fail the deployment")
	require.Zero(t, succeeded)
	require.Less(t, time.Since(start), 4*time.Second, "the deployment outlived the timed-out clone")

	require.Equal(t, 1, pve.counts().aborts, "the in-flight clone task must be aborted")
	require.Empty(t, pve.names(), "the half-cloned VM must be gone, not left creating and unpowered")

	// The template is free: the next deployment clones again and, with a fast clone, succeeds.
	pve.setCloneDuration(100 * time.Millisecond)

	succeeded, err = ig.Increase(ctx, 1)

	require.NoError(t, err)
	require.Equal(t, 1, succeeded)
	require.Equal(t, []string{DefaultInstanceNameRunning}, pve.names())
}

// A clone task that finishes after the last poll but before the wait timeout elapses must
// not be abandoned: the settlement check observes it stopped and the deployment proceeds,
// so a finished clone is never aborted or deleted.
func TestCloneFinishingAtTheWaitTimeoutIsNotAbandoned(t *testing.T) {
	t.Parallel()

	log, _ := newLogBuffer(t)

	pve := newCloneWaitPVE(300 * time.Millisecond)
	ig := newCloneWaitGroup(t, pve, log, 5, 5, 1)

	ctx := context.Background()

	start := time.Now()
	succeeded, err := ig.Increase(ctx, 1)

	require.NoError(t, err, "a clone that finished before the timeout must not fail the deployment")
	require.Equal(t, 1, succeeded)
	require.Less(t, time.Since(start), 4*time.Second)

	require.Zero(t, pve.counts().aborts, "a finished clone must never be aborted")
	require.Equal(t, []string{DefaultInstanceNameRunning}, pve.names())
}
