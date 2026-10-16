package plugin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/luthermonson/go-proxmox"
)

const (
	proxmoxTaskWaitTimeout   = 5 * time.Minute
	proxmoxAgentStartTimeout = 2 * time.Minute

	// proxmoxDeployedVMLookupWindow bounds the post-clone lookup retries: the clone task can
	// finish before Proxmox's pool or node listings include the new VM.
	proxmoxDeployedVMLookupWindow = 30 * time.Second

	// deployedVMLookupInterval is the pause between post-clone lookup attempts.
	deployedVMLookupInterval = 2 * time.Second

	vmOptName   = "name"
	vmOptTags   = "tags"
	vmOptDigest = "digest"
	vmOptCores  = "cores"
	vmOptMemory = "memory"

	vmTypeQEMU = "qemu"
)

var ErrCloneVMWithoutConfiguredStorage = errors.New("attempted to clone a VM without configured storage")

func (ig *InstanceGroup) deployInstance(ctx context.Context, template *proxmox.VirtualMachine) (int, error) {
	VMID, task, err := ig.cloneTemplate(ctx, template)
	if err == nil {
		ig.log.Info("Deploying new instance", "vmid", VMID)

		err = ig.waitTask(ctx, task, proxmoxTaskWaitTimeout)
	}

	if err != nil {
		return VMID, fmt.Errorf("failed to deploy instance: %w", err)
	}

	vm, err := ig.getDeployedVM(ctx, VMID)
	if err != nil {
		return VMID, fmt.Errorf("failed to find newly deployed instance vmid='%d': %w", VMID, err)
	}

	// The clone carries the template's tags. The final rename below re-derives its tags from
	// them rather than from whatever the creating-tag config call above wrote, so the
	// template's tags survive whichever transition the instance ends in.
	baseTags := ""
	if vm.VirtualMachineConfig != nil {
		baseTags = vm.VirtualMachineConfig.Tags
	}

	_, err = vm.Config(ctx, proxmox.VirtualMachineOption{
		Name:  vmOptTags,
		Value: ig.mergedInstanceTags(baseTags, ig.InstanceTagsCreating),
	})
	if err != nil {
		return VMID, fmt.Errorf("failed to set creating tags on instance vmid='%d': %w", VMID, err)
	}

	// Start, configure etc.
	err = ig.prepareInstance(ctx, vm)

	newInstanceName := ig.InstanceNameRunning
	newInstanceTags := ig.InstanceTagsRunning

	if err != nil {
		ig.log.Error("instance deployment failed, marking for removal", "vmid", VMID, "err", err)
		newInstanceName = ig.InstanceNameRemoving
		newInstanceTags = ig.InstanceTagsRemoving
	}

	_, renameErr := vm.Config(ctx,
		proxmox.VirtualMachineOption{
			Name:  vmOptName,
			Value: newInstanceName,
		},
		proxmox.VirtualMachineOption{
			Name:  vmOptTags,
			Value: ig.mergedInstanceTags(baseTags, newInstanceTags),
		},
	)
	if renameErr != nil {
		ig.log.Error("failed to rename instance", "vmid", VMID, "err", renameErr)
	}

	if err != nil {
		return VMID, fmt.Errorf("failed to configure instance, marked for removal due to: %w", err)
	}

	return VMID, nil
}

// prepareInstance applies the configured hardware and disk size to a freshly cloned instance,
// then starts it and waits for its agent. Any error leaves the instance to be marked for removal.
func (ig *InstanceGroup) prepareInstance(ctx context.Context, vm *proxmox.VirtualMachine) error {
	err := ig.configureHardware(ctx, vm)
	if err != nil {
		return fmt.Errorf("failed to set cores and memory: %w", err)
	}

	if ig.InstanceAutoresizeSize != "" {
		err := ig.autoresizeDisk(ctx, vm)
		if err != nil {
			return fmt.Errorf("failed to resize disk: %w", err)
		}
	}

	// Start the VM
	task, err := vm.Start(ctx)
	if err == nil {
		err = ig.waitTask(ctx, task, proxmoxTaskWaitTimeout)
	}

	if err != nil {
		return fmt.Errorf("failed to start newly deployed instance: %w", err)
	}

	// Wait for agent to start
	err = vm.WaitForAgent(ctx, int(proxmoxAgentStartTimeout/time.Second))
	if err != nil {
		return fmt.Errorf("failed when waiting for qemu agent to start on newly deployed instance: %w", err)
	}

	return nil
}

// configureHardware applies instance_cores and instance_memory to a freshly cloned instance,
// before it is started. It waits for the change to be applied, so the instance never boots with
// the template's values instead.
func (ig *InstanceGroup) configureHardware(ctx context.Context, vm *proxmox.VirtualMachine) error {
	var options []proxmox.VirtualMachineOption

	if ig.InstanceCores != nil {
		options = append(options, proxmox.VirtualMachineOption{Name: vmOptCores, Value: *ig.InstanceCores})
	}

	if ig.InstanceMemory != nil {
		options = append(options, proxmox.VirtualMachineOption{Name: vmOptMemory, Value: *ig.InstanceMemory})
	}

	if len(options) == 0 {
		return nil
	}

	task, err := vm.Config(ctx, options...)
	if err != nil {
		return fmt.Errorf("config api call failed for vm id '%d': %w", vm.VMID, err)
	}

	return ig.waitTask(ctx, task, proxmoxTaskWaitTimeout)
}

// getDeployedVM fetches the VM a finished clone task produced, retrying while Proxmox's
// listings lag the task: the task can report stopped before the new VM is a member of the
// pool or of its node's list, and a single lookup failing in that window would fail a
// deployment whose clone actually succeeded, leaving the instance behind. The retries are
// bounded by the lookup window and by the caller's context, so a VM that is genuinely
// missing fails the deployment once the window elapses instead of blocking the batch.
func (ig *InstanceGroup) getDeployedVM(ctx context.Context, vmid int) (*proxmox.VirtualMachine, error) {
	window := proxmoxDeployedVMLookupWindow
	if ig.deployedVMLookupWindow > 0 {
		window = ig.deployedVMLookupWindow
	}

	deadline := time.Now().Add(window)

	for attempt := 1; ; attempt++ {
		vm, err := ig.getProxmoxVM(ctx, vmid)
		if err == nil {
			return vm, nil
		}

		if time.Now().Add(deployedVMLookupInterval).After(deadline) {
			return nil, err
		}

		ig.log.Warn("newly cloned instance not visible in the listings yet, retrying the lookup",
			"vmid", vmid, "attempt", attempt, "err", err)

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context finished while looking up instance vmid='%d': %w", vmid, ctx.Err())
		case <-time.After(deployedVMLookupInterval):
		}
	}
}

// cloneTemplate clones the template while holding the cloning lock for the duration of the
// clone call only. The lock serialises clones across every Increase: Proxmox refuses
// parallel clone requests against the same template, and the vendor pairs the next-VMID
// fetch with the clone POST in one call, so that pair must be atomic across calls, not just
// within one batch. It is released before the caller waits on the clone task: the wait is
// minutes long, and holding the lock through it would stall Update, Decrease and the
// collector for the whole deployment.
func (ig *InstanceGroup) cloneTemplate(ctx context.Context, template *proxmox.VirtualMachine) (int, *proxmox.Task, error) {
	cloneOptions, err := ig.getTemplateCloneOptions(template)
	if err != nil {
		return -1, nil, err
	}

	ig.instanceCloningMu.Lock()
	defer ig.instanceCloningMu.Unlock()

	VMID, task, err := template.Clone(ctx, cloneOptions)
	if err != nil {
		return -1, nil, fmt.Errorf("failed to clone the template: %w", err)
	}

	return VMID, task, nil
}

func (ig *InstanceGroup) getTemplateCloneOptions(template *proxmox.VirtualMachine) (*proxmox.VirtualMachineCloneOptions, error) {
	cloneOptions := &proxmox.VirtualMachineCloneOptions{
		Name:    ig.InstanceNameCreating,
		Pool:    ig.Pool,
		Storage: ig.Storage,
		Target:  ig.InstanceTargetNode,
		Full:    true,
	}

	if !template.Template && ig.Storage == "" {
		return nil, ErrCloneVMWithoutConfiguredStorage
	}

	if template.Template && ig.Storage == "" {
		cloneOptions.Full = false
	}

	return cloneOptions, nil
}

// parseTagList splits a tag list into its tags. The tag settings delimit tags with
// semicolons, Proxmox delimits them with commas, and both are accepted; whitespace around
// the tags is dropped.
func parseTagList(value string) []string {
	return strings.FieldsFunc(value, func(separator rune) bool {
		return separator == ';' || separator == ',' || unicode.IsSpace(separator)
	})
}

// managedTagSet is the set of every tag this group sets in any instance state. They are the
// only tags the group ever adds or removes; any other tag on a VM -- the template's, or
// applied manually -- is left alone.
func (ig *InstanceGroup) managedTagSet() map[string]bool {
	managed := make(map[string]bool)

	for _, value := range []string{ig.InstanceTagsCreating, ig.InstanceTagsRunning, ig.InstanceTagsRemoving} {
		for _, tag := range parseTagList(value) {
			managed[tag] = true
		}
	}

	return managed
}

// mergedInstanceTags returns the tags an instance should carry while in the state named by
// stateTags: its current tags with every tag the group manages in any state removed and the
// state's tags added. Tags the group does not manage are preserved, so the template's tags
// survive the creating, running and removing transitions.
func (ig *InstanceGroup) mergedInstanceTags(current string, stateTags string) string {
	managed := ig.managedTagSet()

	merged := make([]string, 0)
	seen := make(map[string]bool)

	addTag := func(tag string) {
		if !seen[tag] {
			seen[tag] = true
			merged = append(merged, tag)
		}
	}

	for _, tag := range parseTagList(current) {
		if !managed[tag] {
			addTag(tag)
		}
	}

	for _, tag := range parseTagList(stateTags) {
		addTag(tag)
	}

	return strings.Join(merged, ",")
}

func (ig *InstanceGroup) markStaleInstancesForRemoval(ctx context.Context) error {
	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		return err
	}

	instancesToMarkForRemoval := make([]*proxmox.ClusterResource, 0, len(pool.Members))

	for _, member := range pool.Members {
		if !ig.isProxmoxResourceAnInstance(member) {
			continue
		}

		if member.Name != ig.InstanceNameCreating {
			continue
		}

		ig.log.Info("Found stale instance, marking for removal", "name", member.Name, "vmid", member.VMID, "node", member.Node)
		instancesToMarkForRemoval = append(instancesToMarkForRemoval, &member)
	}

	if len(instancesToMarkForRemoval) < 1 {
		return nil
	}

	// Best effort: sweeping instances left stale by a previous run is cleanup, not a
	// precondition for serving, so a VM that cannot be marked -- still locked by a clone task
	// from the previous process, held by a backup, a storage error -- must not stop the plugin
	// from starting. Each failure is logged with its vmid where it happened; the next Init
	// retries.
	err = errors.Join(ig.markInstancesForRemoval(ctx, instancesToMarkForRemoval)...)
	if err != nil {
		ig.log.Error("failed to mark some stale instances for removal, continuing startup",
			"attempted", len(instancesToMarkForRemoval), "err", err)
	}

	return nil
}

// markInstanceForRemoval renames and retags one instance so the collector picks it up. The
// caller owns the batch and triggers the collector once for all of them.
func (ig *InstanceGroup) markInstanceForRemoval(ctx context.Context, instance *proxmox.ClusterResource) error {
	log := ig.log.With("name", instance.Name, "vmid", instance.VMID, "node", instance.Node)

	vm, err := ig.getListedVM(ctx, instance)
	if err == nil && fetchedName(vm) == ig.InstanceNameRemoving {
		// Already marked, by an earlier attempt whose rename the listing has not caught up
		// with. Renaming it again would change nothing.
		return nil
	}

	if err == nil {
		var task *proxmox.Task

		currentTags := ""
		if vm.VirtualMachineConfig != nil {
			currentTags = vm.VirtualMachineConfig.Tags
		}

		options := []proxmox.VirtualMachineOption{
			{
				Name:  vmOptName,
				Value: ig.InstanceNameRemoving,
			},
			{
				Name:  vmOptTags,
				Value: ig.mergedInstanceTags(currentTags, ig.InstanceTagsRemoving),
			},
		}

		if vm.VirtualMachineConfig != nil && vm.VirtualMachineConfig.Digest != "" {
			// getListedVM checked the name on this fetch; the digest makes Proxmox refuse the
			// rename if the config has changed since, so another manager renaming the VM in
			// between fails the rename instead of having it overwritten.
			options = append(options, proxmox.VirtualMachineOption{
				Name:  vmOptDigest,
				Value: vm.VirtualMachineConfig.Digest,
			})
		}

		task, err = vm.Config(ctx, options...)

		if err == nil && task == nil {
			// The rename is the only evidence the collector will ever see that this instance
			// is to be removed; with no task there is no evidence it happened.
			err = ErrNoTask
		}

		if err == nil {
			err = ig.waitTask(ctx, task, proxmoxTaskWaitTimeout)
		}
	}

	if err != nil {
		log.Error("Failed to mark instance for removal", "err", err)

		return fmt.Errorf("failed to mark instance for removal: %w", err)
	}

	return nil
}

// markInstancesForRemoval marks every instance in parallel and wakes the collector once,
// returning each instance's error in its own slot. An empty batch marks nothing and wakes
// nothing: the wake costs the collector a trigger wait and a pool listing, and with nothing
// renamed to removing the round would collect nothing.
func (ig *InstanceGroup) markInstancesForRemoval(ctx context.Context, instances []*proxmox.ClusterResource) []error {
	if len(instances) == 0 {
		return nil
	}

	defer ig.triggerCollection()

	return runParallel(len(instances), func(index int) error {
		return ig.markInstanceForRemoval(ctx, instances[index])
	})
}

func (ig *InstanceGroup) isProxmoxResourceAnInstance(member proxmox.ClusterResource) bool {
	//nolint:gosec//G115 // TemplateID is a Proxmox VMID, which always fits in an int
	return member.Type == vmTypeQEMU && member.VMID != uint64(*ig.TemplateID)
}
