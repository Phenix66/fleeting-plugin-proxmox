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

	// ErrInstanceOperationPanic reports a panic in one parallel instance operation, recovered
	// so it fails that attempt instead of taking the plugin process down.
	ErrInstanceOperationPanic = errors.New("instance operation panicked")
)

const (
	networkCheckTimeout = 5 * time.Second
	networkCheckRetries = 12
)

type InstanceGroup struct {
	Settings `json:",inline"`

	FleetingSettings provider.Settings `json:"-"`

	log     hclog.Logger    `json:"-"`
	proxmox *proxmox.Client `json:"-"`

	// This mutex is used when cloning template for new instances. It is required for blocking other
	// operations like collection or update, because when new instance is created with recycled ID then for
	// a brief period it will be reported from Proxmox with old name (e.g. InstanceNameRemoving).
	instanceCloningMu sync.Mutex `json:"-"`

	// Trigger for collector to start removed instances collection.
	instanceCollectionTrigger chan struct{} `json:"-"`

	// Trigger to shutdown collector.
	collectorShutdownTrigger chan struct{} `json:"-"`

	// Wait group for the collector.
	collectorWaitGroup sync.WaitGroup `json:"-"`

	// Trigger to shutdown session ticket refresher.
	sessionTicketRefresherShutdownTrigger chan struct{} `json:"-"`

	// Wait group for session ticket refresher.
	sessionTicketRefresherWaitGroup sync.WaitGroup `json:"-"`

	// Guards the channel closes; re-armed by resetLifecycle.
	shutdownOnce sync.Once `json:"-"`
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
	<-time.After(collectionWaitAfterTrigger)

	//nolint:contextcheck
	ig.startRemovedInstanceCollector()

	//nolint:contextcheck
	ig.startSessionTicketRefresher()

	return provider.ProviderInfo{
		ID:      ig.Pool,
		MaxSize: *ig.MaxInstances,
	}, nil
}

// Shutdown implements provider.InstanceGroup. It is safe to call repeatedly, and safe to call
// before Init.
func (ig *InstanceGroup) Shutdown(_ context.Context) error {
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

	ig.collectorWaitGroup.Wait()
	ig.sessionTicketRefresherWaitGroup.Wait()

	return nil
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

	for index := range count {
		waitGroup.Go(func() {
			errs[index] = runRecovering(index, run)
		})
	}

	waitGroup.Wait()

	return errs
}

// runRecovering runs run(index) and turns a panic into the slot's error, carrying the panic
// value and stack so the failure stays diagnosable wherever the error is logged.
func runRecovering(index int, run func(index int) error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %v\n%s", ErrInstanceOperationPanic, recovered, debug.Stack())
		}
	}()

	return run(index)
}

// Increase implements provider.InstanceGroup.
func (ig *InstanceGroup) Increase(ctx context.Context, count int) (int, error) {
	template, err := ig.getProxmoxVM(ctx, *ig.TemplateID)
	if err != nil {
		return 0, fmt.Errorf("failed to find template with id='%d': %w", *ig.TemplateID, err)
	}

	// We need to mutex cloning as Proxmox will fail multiple requests in parallel
	cloneMu := new(sync.Mutex)

	ig.instanceCloningMu.Lock()
	defer ig.instanceCloningMu.Unlock()

	succeeded := 0

	errs := runParallel(count, func(_ int) error {
		vmid, err := ig.deployInstance(ctx, template, cloneMu)
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
	ig.instanceCloningMu.Lock()
	defer ig.instanceCloningMu.Unlock()

	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		return err
	}

	for _, member := range pool.Members {
		if !ig.isProxmoxResourceAnInstance(member) {
			continue
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
	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		return []string{}, err
	}

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

func (ig *InstanceGroup) Resume(ctx context.Context, instances []string) ([]string, error) {
	var succeeded []string

	var errs []string

	for _, instance := range instances {
		vmid, err := strconv.Atoi(instance)
		if err != nil {
			errs = append(errs, fmt.Sprintf("invalid vm id '%s'", instance))

			continue
		}

		vm, err := ig.ownedInstance(ctx, vmid)
		if err != nil {
			errs = append(errs, fmt.Sprintf("vm id '%d': %v", vmid, err))

			continue
		}

		_, err = vm.Resume(ctx)
		if err != nil {
			errs = append(errs, fmt.Sprintf("resume api call failed for vm id '%d'", vmid))

			continue
		}

		succeeded = append(succeeded, instance)
	}

	if len(errs) > 0 {
		return succeeded, fmt.Errorf("%w: %s", ErrResumeFailed, strings.Join(errs, ", "))
	}

	return succeeded, nil
}

func (ig *InstanceGroup) Suspend(ctx context.Context, instances []string) ([]string, error) {
	var succeeded []string

	var errs []string

	for _, instance := range instances {
		vmid, err := strconv.Atoi(instance)
		if err != nil {
			errs = append(errs, fmt.Sprintf("invalid vm id '%s'", instance))

			continue
		}

		vm, err := ig.ownedInstance(ctx, vmid)
		if err != nil {
			errs = append(errs, fmt.Sprintf("vm id '%d': %v", vmid, err))

			continue
		}

		_, err = vm.Pause(ctx)
		if err != nil {
			errs = append(errs, fmt.Sprintf("pause api call failed for vm id '%d'", vmid))

			continue
		}

		succeeded = append(succeeded, instance)
	}

	if len(errs) > 0 {
		return succeeded, fmt.Errorf("%w: %s", ErrSuspendFailed, strings.Join(errs, ", "))
	}

	return succeeded, nil
}

// resetLifecycle creates the worker channels and re-arms the shutdown guard. The wait groups
// Shutdown waits on need no reset -- a sync.WaitGroup is reusable once Wait has returned -- but
// everything Shutdown closes is created here, so a group can be initialised again after a
// Shutdown without the previous cycle's guard blocking the next one.
func (ig *InstanceGroup) resetLifecycle() {
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
			time.Sleep(networkCheckTimeout)

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
