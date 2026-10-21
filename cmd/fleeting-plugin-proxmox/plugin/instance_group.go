package plugin

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/luthermonson/go-proxmox"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
)

var _ provider.InstanceGroup = (*InstanceGroup)(nil)

var (
	ErrInstanceConnectionTimeout = errors.New("timed out getting connection info")
	ErrResumeFailed              = errors.New("one or more instances did not resume successfully")
	ErrSuspendFailed             = errors.New("one or more instances did not suspend successfully")

	// ErrPluginPanic reports a panic in one of the plugin's goroutines -- a parallel instance
	// operation or a background worker -- recovered so it fails or logs instead of taking the
	// plugin process down.
	ErrPluginPanic = errors.New("plugin goroutine panicked")
)

const (
	networkCheckTimeout = 5 * time.Second
	networkCheckRetries = 12

	// defaultWorkerRestartPause bounds how long a panicked worker waits before its loop
	// restarts: long enough for a transient wedge -- a stuck API call, a wedged listing -- to
	// clear, and a ceiling on the rate a deterministic panic can log.
	defaultWorkerRestartPause = 1 * time.Minute

	// maxParallelInstanceOperations bounds how many operations one batch runs at once. A batch
	// is as large as the pool -- every Increase, every mark-for-removal sweep, every resume
	// or suspend -- and an unbounded fan-out would open one HTTP connection and one task
	// poller per instance, whatever the pool's size.
	maxParallelInstanceOperations = 16
)

type InstanceGroup struct {
	Settings `json:",inline"`

	FleetingSettings provider.Settings `json:"-"`

	log     hclog.Logger    `json:"-"`
	proxmox *proxmox.Client `json:"-"`

	// True when the client authenticates with a PVE API token instead of a ticket
	// session. Token auth carries no session state, so there is no ticket to refresh
	// and the session ticket refresher is not started.
	usingTokenAuth bool `json:"-"`

	// Held for the duration of one template.Clone call. It serialises clones across every
	// Increase: Proxmox refuses parallel clone requests against the same template, and the
	// vendor pairs the next-VMID fetch with the clone POST in one call, so that pair must be
	// atomic across calls. Collection, Update and Decrease take their pool listings under it:
	// while a clone is in flight, a recycled VMID is reported with its predecessor's stale
	// name (e.g. InstanceNameRemoving), and a listing taken in that window would classify a
	// just-created VM from the old one's name. Deliberately NOT held for the clone's task
	// wait or anything after it: that is minutes long, and holding it would stall the
	// provisioner's Update and Decrease for the whole deployment.
	instanceCloningMu sync.Mutex `json:"-"`

	// Trigger for collector to start removed instances collection.
	instanceCollectionTrigger chan struct{} `json:"-"`

	// collectionLockWait caps how long a collection round waits for the cloning lock before
	// skipping the round; zero means defaultCollectionLockWait. Per-instance so a test can
	// shorten it on its own group without touching shared state.
	collectionLockWait time.Duration `json:"-"`

	// collectionTriggerWait paces a triggered round so a rename has time to propagate; zero
	// means defaultCollectionTriggerWait. Per-instance for the same test reason.
	collectionTriggerWait time.Duration `json:"-"`

	// collectionMemberTimeout bounds one member's stop and delete in a collection round;
	// zero means collectionTimeout. Per-instance for the same test reason.
	collectionMemberTimeout time.Duration `json:"-"`

	// deployedVMLookupWindow caps how long the post-clone lookup retries while Proxmox's
	// listings lag the finished clone task; zero means proxmoxDeployedVMLookupWindow.
	// Per-instance so a test can shorten it on its own group without touching shared state.
	deployedVMLookupWindow time.Duration `json:"-"`

	// deployingMu guards deploying: the VMIDs a live deployInstance is working on, from the
	// clone's VMID allocation until the deployment renames the instance away from creating.
	// The collector's stale sweep skips them: a deploying instance is creating by design, and
	// only the deployment may rename it.
	deployingMu sync.Mutex       `json:"-"`
	deploying   map[int]struct{} `json:"-"`

	// workerRestartPause bounds the pause before a panicked worker loop restarts; zero means
	// defaultWorkerRestartPause. Per-instance so a test can shorten it on its own group
	// without touching shared state.
	workerRestartPause time.Duration `json:"-"`

	// sessionTicketRefreshInterval is how often the session ticket is renewed; zero means
	// defaultSessionTicketRefreshInterval. Per-instance for the same test reason.
	sessionTicketRefreshInterval time.Duration `json:"-"`

	// sessionTicketRefreshRetryInterval is how soon a failed session ticket refresh is
	// retried; zero means defaultSessionTicketRefreshRetryInterval. Per-instance for the
	// same test reason.
	sessionTicketRefreshRetryInterval time.Duration `json:"-"`

	// Trigger to shutdown collector.
	collectorShutdownTrigger chan struct{} `json:"-"`

	// Wait group for the collector.
	collectorWaitGroup sync.WaitGroup `json:"-"`

	// Trigger to shutdown session ticket refresher.
	sessionTicketRefresherShutdownTrigger chan struct{} `json:"-"`

	// Wait group for session ticket refresher.
	sessionTicketRefresherWaitGroup sync.WaitGroup `json:"-"`

	// poolCacheMu guards poolCache and poolCacheAt: the short-lived pool listing the
	// per-instance lookups (ConnectInfo, Heartbeat, resume, suspend) share. resetLifecycle
	// drops it, because a re-Init replaces the client the listing was fetched with.
	poolCacheMu sync.Mutex    `json:"-"`
	poolCache   *proxmox.Pool `json:"-"`
	poolCacheAt time.Time     `json:"-"`

	// Guards the channel closes; re-armed by resetLifecycle.
	shutdownOnce sync.Once `json:"-"`

	// storageFullLogMu guards storageFullLoggedAt, which throttles the warning logged when
	// Increase clones fewer instances than requested for lack of storage space.
	storageFullLogMu    sync.Mutex `json:"-"`
	storageFullLoggedAt time.Time  `json:"-"`
}

// Init implements provider.InstanceGroup.
func (ig *InstanceGroup) Init(ctx context.Context, logger hclog.Logger, settings provider.Settings) (provider.ProviderInfo, error) {
	ig.log = logger
	ig.FleetingSettings = settings
	ig.resetLifecycle()

	err := ig.CheckRequiredFields()
	if err != nil {
		return provider.ProviderInfo{}, err
	}

	ig.FillWithDefaults()

	if ig.InsecureSkipTLSVerify {
		ig.log.Warn("TLS verification for Proxmox client is disabled, connections will be insecure")
	}

	ig.proxmox, err = ig.getProxmoxClient()
	if err != nil {
		return provider.ProviderInfo{}, err
	}

	err = ig.markStaleInstancesForRemoval(ctx)
	if err != nil {
		return provider.ProviderInfo{}, err
	}

	// Sleep for a bit to give Proxmox a chance to propagate renames for stale instances
	// Without this sleep these instances would be reported as creating during first Update
	<-time.After(ig.triggerWait())

	//nolint:contextcheck
	ig.startRemovedInstanceCollector()

	// A ticket session needs periodic renewal; an API token is a static secret and
	// holds no session, so the refresher is only started for ticket auth.
	if !ig.usingTokenAuth {
		//nolint:contextcheck
		ig.startSessionTicketRefresher()
	}

	return provider.ProviderInfo{
		ID:      ig.Pool,
		MaxSize: *ig.MaxInstances,
	}, nil
}

// Shutdown implements provider.InstanceGroup. It is safe to call repeatedly, and safe to call
// before Init. It returns ctx.Err() if ctx expires before the workers have drained; the
// workers keep running and drain on their own bounded contexts, so a worker blocked on a stuck
// Proxmox API call cannot hold the shutdown hostage.
func (ig *InstanceGroup) Shutdown(ctx context.Context) error {
	// Before Init there is nothing to close and nothing to wait for, and closing a nil channel
	// would panic and take the plugin process down. Both channels are only ever created
	// together in resetLifecycle, so one check covers them. The check stays outside the guard as
	// defence in depth rather than out of necessity: inside it, a pre-Init call would consume
	// shutdownOnce without closing anything, and the Shutdown after the next Init would then be
	// relying on resetLifecycle having re-armed it. resetLifecycle does re-arm it on every Init,
	// so the placement is not what makes the second cycle work -- it just means Shutdown does not
	// have to depend on that.
	if ig.collectorShutdownTrigger == nil {
		return nil
	}

	ig.shutdownOnce.Do(func() {
		close(ig.collectorShutdownTrigger)
		close(ig.sessionTicketRefresherShutdownTrigger)
	})

	err := waitGroups(ctx, &ig.collectorWaitGroup, &ig.sessionTicketRefresherWaitGroup)
	if err != nil {
		return fmt.Errorf("timed out waiting for workers to drain: %w", err)
	}

	return nil
}

// waitGroups waits for every wait group to drain or for ctx to expire, whichever comes first.
// On ctx expiry the drain is left to the workers' own bounded contexts; the goroutine left
// behind exits as soon as they do.
func waitGroups(ctx context.Context, groups ...*sync.WaitGroup) error {
	done := make(chan struct{})

	go func() {
		for _, group := range groups {
			group.Wait()
		}

		close(done)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("shutdown context expired before workers drained: %w", ctx.Err())
	case <-done:
		return nil
	}
}

// runParallel calls run once for every index in [0, count), returning each call's error in
// its own slot so a failure stays matched to the item that caused it without a lock. A panic
// in one call becomes that slot's error, so a single bad item fails its own attempt instead
// of taking the plugin process down.
func runParallel(count int, run func(index int) error) []error {
	if count <= 0 {
		return nil
	}

	var waitGroup sync.WaitGroup

	errs := make([]error, count)

	// A bounded pool of workers instead of one goroutine per slot: the batch can be as
	// large as the pool, and the cap keeps its fan-out -- and the HTTP connections and
	// task pollers behind it -- at a constant size.
	workers := min(count, maxParallelInstanceOperations)

	indexes := make(chan int)

	for range workers {
		waitGroup.Go(func() {
			for index := range indexes {
				errs[index] = runRecovering(index, run)
			}
		})
	}

	for index := range count {
		indexes <- index
	}

	close(indexes)

	waitGroup.Wait()

	return errs
}

// runRecovering runs run(index) and turns a panic into the slot's error, carrying the panic
// value and stack so the failure stays diagnosable wherever the error is logged.
func runRecovering(index int, run func(index int) error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %v\n%s", ErrPluginPanic, recovered, debug.Stack())
		}
	}()

	return run(index)
}

// panicError renders a recovered panic with the stack it was captured at, so the log entry
// stays diagnosable: the value says what happened, the stack says where.
func panicError(recovered any) error {
	return fmt.Errorf("%w: %v\n%s", ErrPluginPanic, recovered, debug.Stack())
}

// Increase implements provider.InstanceGroup.
// Increase implements provider.InstanceGroup. The cloning lock is held only for each
// individual clone call, inside cloneTemplate, so Update, Decrease and the collector are
// blocked for the duration of a clone POST -- not for the whole batch's task waits and agent
// polls, which can run for minutes per instance.
func (ig *InstanceGroup) Increase(ctx context.Context, count int) (int, error) {
	template, err := ig.getProxmoxVM(ctx, *ig.TemplateID)
	if err != nil {
		return 0, fmt.Errorf("failed to find template with id='%d': %w", *ig.TemplateID, err)
	}

	// Clone only what fits on the storage. A clone or resize that runs out of space fails
	// anyway, but only after Proxmox has started on it; refusing up front keeps the demand
	// pending in taskscaler, which retries it once instances are removed and space frees up.
	if ig.StorageReserve != "" {
		count, err = ig.instancesThatFit(ctx, template, count)
		if err != nil {
			return 0, fmt.Errorf("failed to check free space on storage='%s': %w", ig.Storage, err)
		}

		if count == 0 {
			return 0, nil
		}
	}

	succeeded := 0

	errs := runParallel(count, func(_ int) error {
		vmid, err := ig.deployInstance(ctx, template)
		if err != nil {
			ig.log.Error("failed to deploy an instance", "vmid", vmid, "err", err)

			return err
		}

		ig.log.Info("successfully deployed instance", "vmid", vmid)

		return nil
	})

	for _, err := range errs {
		if err == nil {
			succeeded++
		}
	}

	return succeeded, ig.batchError("failed to deploy some instances", errs)
}

// Update implements provider.InstanceGroup.
func (ig *InstanceGroup) Update(ctx context.Context, update func(instance string, state provider.State)) error {
	// List the pool under the cloning mutex: while a clone is in flight, a recycled VMID is
	// reported with its predecessor's stale name, and classifying such a member would report a
	// just-created VM under the old one's state. The mutex is dropped before the callback
	// runs: it can round-trip to the provisioner per instance, and holding the mutex through
	// it would stall Increase, Decrease and the collector for as long as the reconcile takes
	// -- the same split as the collector and Decrease.
	ig.instanceCloningMu.Lock()

	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		ig.instanceCloningMu.Unlock()

		return err
	}

	ig.instanceCloningMu.Unlock()

	for _, member := range pool.Members {
		if !ig.isProxmoxResourceAnInstance(member) {
			continue
		}

		if member.Name == "" {
			// No name in the listing is not a foreign name: Proxmox leaves it unset while
			// its status cache holds no fresh entry for the VM. Ask the VM itself, the way
			// Decrease does, or the instance drops out of the report for as long as the
			// cache stays stale.
			nameErr := ig.nameFromNode(ctx, &member)
			if nameErr != nil {
				ig.log.Warn("cannot read the name of an instance, skipping it this cycle", "vmid", member.VMID, "err", nameErr)

				continue
			}
		}

		state, ok := ig.stateForName(member.Name)
		if !ok {
			continue // Unknown name, skipping...
		}

		update(strconv.FormatUint(member.VMID, 10), state)
	}

	return nil
}

// ConnectInfo implements provider.InstanceGroup.
func (ig *InstanceGroup) ConnectInfo(ctx context.Context, instance string) (provider.ConnectInfo, error) {
	VMID, err := strconv.Atoi(instance)
	if err != nil {
		return provider.ConnectInfo{}, fmt.Errorf("failed to parse instance name '%s': %w", instance, err)
	}

	vm, err := ig.ownedInstance(ctx, VMID)
	if err != nil {
		return provider.ConnectInfo{}, fmt.Errorf("failed to retrieve instance vmid='%d': %w", VMID, err)
	}

	return ig.getConnectInfoFromVM(ctx, instance, vm)
}

// Decrease implements provider.InstanceGroup.
func (ig *InstanceGroup) Decrease(ctx context.Context, instancesToRemove []string) ([]string, error) {
	// List the pool under the cloning mutex: while a clone is in flight, a recycled VMID is
	// reported with its predecessor's stale name, and classifying such a member would report a
	// just-created VM as removed or queue a rename for it. The mutex is dropped before the
	// per-member work below, which can take minutes -- the same split as the collector -- so a
	// slow rename cannot stall a clone behind it.
	ig.instanceCloningMu.Lock()

	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		ig.instanceCloningMu.Unlock()

		return []string{}, err
	}

	ig.instanceCloningMu.Unlock()

	var (
		succeeded = make([]string, 0, len(instancesToRemove))
		toRemove  = make([]*proxmox.ClusterResource, 0, len(instancesToRemove))
	)

	for _, member := range pool.Members {
		if !ig.isProxmoxResourceAnInstance(member) {
			continue
		}

		vmid := strconv.FormatUint(member.VMID, 10)

		if !slices.Contains(instancesToRemove, vmid) {
			continue
		}

		if member.Name == "" {
			// No name in the listing is not a foreign name: ask the VM itself, or the default
			// arm would report our own VM removed and leave it running.
			nameErr := ig.nameFromNode(ctx, &member)
			if nameErr != nil {
				ig.log.Warn("cannot read the name of an instance to remove, will retry", "vmid", member.VMID, "err", nameErr)

				continue
			}
		}

		state, owned := ig.stateForName(member.Name)

		switch {
		case !owned:
			// Another manager's VM, or a VMID reused since fleeting last saw it. Never touch it.
			// Report it removed so the provisioner stops asking; Update no longer lists it, so
			// it is pruned on the next cycle either way. Not an attempt: it must not count
			// toward batchError.
			ig.log.Warn("refusing to remove instance not owned by this group", "vmid", member.VMID, "name", member.Name)

			succeeded = append(succeeded, vmid)
		case state == provider.StateCreating:
			// Must be running to start the deletion; provisioner retries.
			continue
		case state == provider.StateDeleting:
			// Already deleting; count as succeeded without a new request.
			succeeded = append(succeeded, vmid)
		case state == provider.StateRunning:
			ig.log.Info("removing instance", "vmid", member.VMID)
			toRemove = append(toRemove, &member)
		}
	}

	errs := ig.markInstancesForRemoval(ctx, toRemove)

	for index, member := range toRemove {
		if errs[index] == nil {
			succeeded = append(succeeded, strconv.FormatUint(member.VMID, 10))
		}
	}

	return succeeded, ig.batchError("failed to mark some instances for removal", errs)
}

func (ig *InstanceGroup) Heartbeat(ctx context.Context, instance string) error {
	vmid, err := strconv.Atoi(instance)
	if err != nil {
		return fmt.Errorf("invalid vm id '%s': %w", instance, err)
	}

	vm, err := ig.ownedInstance(ctx, vmid)
	if err != nil {
		return fmt.Errorf("failed to retrieve instance vmid='%d': %w", vmid, err)
	}

	// Returns an error if the QEMU agent is not communicating due to an empty result
	_, err = vm.AgentOsInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to qemu agent '%s': %w", instance, err)
	}

	return nil
}

// Resume implements provider.InstanceGroup. The batch shares one pool listing, and the
// per-instance work runs in parallel under the batch cap: resume is a fire-and-forget
// request, so nothing waits on a task.
func (ig *InstanceGroup) Resume(ctx context.Context, instances []string) ([]string, error) {
	vms, errs := ig.resolveOwnedInstances(ctx, instances)

	errs = runParallel(len(instances), func(index int) error {
		if errs[index] != nil {
			return errs[index]
		}

		_, err := vms[index].Resume(ctx)
		if err != nil {
			return fmt.Errorf("resume api call failed for vm id '%d': %w", vms[index].VMID, err)
		}

		return nil
	})

	return suspendResumeOutcome(instances, errs, ErrResumeFailed)
}

// Suspend implements provider.InstanceGroup, with the same shape as Resume.
func (ig *InstanceGroup) Suspend(ctx context.Context, instances []string) ([]string, error) {
	vms, errs := ig.resolveOwnedInstances(ctx, instances)

	errs = runParallel(len(instances), func(index int) error {
		if errs[index] != nil {
			return errs[index]
		}

		_, err := vms[index].Pause(ctx)
		if err != nil {
			return fmt.Errorf("pause api call failed for vm id '%d': %w", vms[index].VMID, err)
		}

		return nil
	})

	return suspendResumeOutcome(instances, errs, ErrSuspendFailed)
}

// suspendResumeOutcome reports a batch of resume or suspend attempts the way the provisioner
// expects: the ids whose attempt was accepted, and, when anything failed, an error wrapping
// the operation's sentinel with every attempt's message.
func suspendResumeOutcome(instances []string, errs []error, sentinel error) ([]string, error) {
	var (
		succeeded []string
		failed    []string
	)

	for index, instance := range instances {
		if errs[index] == nil {
			succeeded = append(succeeded, instance)

			continue
		}

		failed = append(failed, errs[index].Error())
	}

	if len(failed) > 0 {
		return succeeded, fmt.Errorf("%w: %s", sentinel, strings.Join(failed, ", "))
	}

	return succeeded, nil
}

// runRecoveringLoop runs a background worker loop for the life of the lifecycle. A panic in
// the loop -- the vendor client can index past the end of a malformed UPID -- is logged and
// the loop is restarted after a bounded pause, so one panic cannot leave the worker dead for
// the rest of the run: a dead collector stops every removal and every stale sweep, and a dead
// refresher every ticket refresh. A deterministic panic then repeats at the pause's rate --
// the log keeps saying why -- and the pause waits out the stop trigger first, so a shutdown
// is never held by it.
func (ig *InstanceGroup) runRecoveringLoop(worker string, stop <-chan struct{}, loop func()) {
	for {
		panicked := false

		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					panicked = true

					ig.log.Error(worker+" loop panicked, restarting", "err", panicError(recovered))
				}
			}()

			loop()
		}()

		if !panicked {
			return
		}

		select {
		case <-stop:
			return
		case <-time.After(ig.restartPause()):
		}
	}
}

// restartPause bounds the pause before a panicked worker loop restarts; a per-group override
// wins over the default so a test can shorten it on its own group.
func (ig *InstanceGroup) restartPause() time.Duration {
	if ig.workerRestartPause > 0 {
		return ig.workerRestartPause
	}

	return defaultWorkerRestartPause
}

// refreshInterval is how often the session ticket is renewed; a per-group override wins over
// the default so a test can shorten it on its own group.
func (ig *InstanceGroup) refreshInterval() time.Duration {
	if ig.sessionTicketRefreshInterval > 0 {
		return ig.sessionTicketRefreshInterval
	}

	return defaultSessionTicketRefreshInterval
}

// refreshRetryInterval is how soon a failed session ticket refresh is retried; a per-group
// override wins over the default so a test can shorten it on its own group.
func (ig *InstanceGroup) refreshRetryInterval() time.Duration {
	if ig.sessionTicketRefreshRetryInterval > 0 {
		return ig.sessionTicketRefreshRetryInterval
	}

	return defaultSessionTicketRefreshRetryInterval
}

// resetLifecycle creates the worker channels and re-arms the shutdown guard. A reset with a
// live previous cycle -- an Init without a Shutdown in between -- first stops that cycle's
// workers: without it they would block forever on the channels about to be replaced, and
// their wait group counts would make the next Shutdown hang. The previous cycle's
// shutdownOnce is the per-cycle guard, so exactly one of Shutdown or this reset closes its
// triggers, never both. The wait groups themselves need no reset -- a sync.WaitGroup is
// reusable once Wait has returned -- but everything Shutdown closes is created here, so a
// group can be initialised again after a Shutdown without the previous cycle's guard blocking
// the next one.
func (ig *InstanceGroup) resetLifecycle() {
	// A re-Init replaces the Proxmox client, and a listing fetched with the previous one
	// is no evidence about the new one.
	ig.poolCacheMu.Lock()
	ig.poolCache = nil
	ig.poolCacheAt = time.Time{}
	ig.poolCacheMu.Unlock()

	// A re-Init starts a fresh lifecycle: no deployment of the previous cycle belongs to it,
	// and any creating instance the previous cycle left, the stale sweep at Init -- which
	// ignores the map -- marks for removal.
	ig.deployingMu.Lock()
	ig.deploying = nil
	ig.deployingMu.Unlock()

	// Channels only exist after the first reset, and they are created together, so one
	// check covers both.
	if ig.collectorShutdownTrigger != nil {
		ig.shutdownOnce.Do(func() {
			close(ig.collectorShutdownTrigger)
			close(ig.sessionTicketRefresherShutdownTrigger)
		})

		// The workers drain on their own bounded contexts, so this returns once the
		// previous cycle's in-flight work finishes.
		ig.collectorWaitGroup.Wait()
		ig.sessionTicketRefresherWaitGroup.Wait()
	}

	ig.instanceCollectionTrigger = make(chan struct{}, 1)
	ig.collectorShutdownTrigger = make(chan struct{})
	ig.sessionTicketRefresherShutdownTrigger = make(chan struct{})
	ig.shutdownOnce = sync.Once{}
}

// batchError reports a batch of instance operations as failed only when nothing attempted
// succeeded, and logs the aggregate trace that decision hides. It is the one place that rule
// lives, for both Increase and Decrease.
//
// A partial success is reported with a NIL error. fleeting's gRPC shim returns the response
// alongside the error and grpc-go discards the response whenever the error is non-nil, so a
// partial success reported as an error reaches the provisioner as no instances at all --
// leaving instances that were in fact created or removed untracked and classified
// CauseUnexpected. Each individual failure is already logged where it happened, so the warning
// here only has to record how much the nil error is hiding.
func (ig *InstanceGroup) batchError(msg string, errs []error) error {
	err := errors.Join(errs...)
	if err == nil {
		return nil
	}

	failed := 0

	for _, attemptErr := range errs {
		if attemptErr != nil {
			failed++
		}
	}

	if failed == len(errs) {
		return err
	}

	ig.log.Warn(msg, "attempted", len(errs), "failed", failed, "err", err)

	return nil
}

func (ig *InstanceGroup) getConnectInfoFromVM(ctx context.Context, instance string, vm *proxmox.VirtualMachine) (provider.ConnectInfo, error) {
	for retry := range networkCheckRetries {
		networkInterfaces, err := vm.AgentGetNetworkIFaces(ctx)
		if err != nil {
			return provider.ConnectInfo{}, fmt.Errorf("failed to retrieve instance vmid='%d' interfaces: %w", vm.VMID, err)
		}

		internalAddress, externalAddress, err := determineAddresses(networkInterfaces, ig.InstanceNetworkInterface, ig.InstanceNetworkProtocol)
		if err != nil {
			ig.log.Error("failed to get network interface", "retry", retry, "vmid", vm.VMID, "err", err)

			// A canceled ConnectInfo must not sit out the retry sleep: the caller is gone, so
			// report the cancellation instead of burning up to networkCheckTimeout per retry.
			select {
			case <-ctx.Done():
				return provider.ConnectInfo{}, fmt.Errorf("waiting for instance vmid='%d' network interfaces: %w", vm.VMID, ctx.Err())
			case <-time.After(networkCheckTimeout):
			}

			continue
		}

		return provider.ConnectInfo{
			ID:              instance,
			InternalAddr:    internalAddress,
			ExternalAddr:    externalAddress,
			ConnectorConfig: ig.FleetingSettings.ConnectorConfig,
		}, nil
	}

	return provider.ConnectInfo{}, fmt.Errorf("%w vmid='%d'", ErrInstanceConnectionTimeout, vm.VMID)
}
