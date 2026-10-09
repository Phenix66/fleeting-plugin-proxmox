package plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/luthermonson/go-proxmox"
)

const (
	proxmoxTaskWaitTimeout   = 5 * time.Minute
	proxmoxAgentStartTimeout = 2 * time.Minute

	vmOptName   = "name"
	vmOptTags   = "tags"
	vmOptDigest = "digest"

	vmTypeQEMU = "qemu"
)

var ErrCloneVMWithoutConfiguredStorage = errors.New("attempted to clone a VM without configured storage")

func (ig *InstanceGroup) deployInstance(ctx context.Context, template *proxmox.VirtualMachine, cloneMu *sync.Mutex) (int, error) {
	VMID, task, err := ig.cloneTemplate(ctx, template, cloneMu)
	if err == nil {
		ig.log.Info("Deploying new instance", "vmid", VMID)

		err = ig.waitTask(ctx, task, proxmoxTaskWaitTimeout)
	}

	if err != nil {
		return VMID, fmt.Errorf("failed to deploy instance: %w", err)
	}

	vm, err := ig.getProxmoxVM(ctx, VMID)
	if err != nil {
		return VMID, fmt.Errorf("failed to find newly deployed instance vmid='%d': %w", VMID, err)
	}

	_, err = vm.Config(ctx, proxmox.VirtualMachineOption{
		Name:  vmOptTags,
		Value: ig.InstanceTagsCreating,
	})
	if err != nil {
		return VMID, fmt.Errorf("failed to get instance config vmid='%d': %w", VMID, err)
	}

	// Start, configure etc.
	err = func() error {
		if ig.InstanceAutoresizeSize != "" {
			task, err := vm.ResizeDisk(ctx, ig.InstanceAutoresizeDisk, ig.InstanceAutoresizeSize)
			if err == nil {
				err = ig.waitTask(ctx, task, proxmoxTaskWaitTimeout)
			}

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
	}()

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
			Value: newInstanceTags,
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

func (ig *InstanceGroup) cloneTemplate(ctx context.Context, template *proxmox.VirtualMachine, cloneMu *sync.Mutex) (int, *proxmox.Task, error) {
	cloneOptions, err := ig.getTemplateCloneOptions(template)
	if err != nil {
		return -1, nil, err
	}

	cloneMu.Lock()
	defer cloneMu.Unlock()

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

		options := []proxmox.VirtualMachineOption{
			{
				Name:  vmOptName,
				Value: ig.InstanceNameRemoving,
			},
			{
				Name:  vmOptTags,
				Value: ig.InstanceTagsRemoving,
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
// returning each instance's error in its own slot.
func (ig *InstanceGroup) markInstancesForRemoval(ctx context.Context, instances []*proxmox.ClusterResource) []error {
	defer ig.triggerCollection()

	return runParallel(len(instances), func(index int) error {
		return ig.markInstanceForRemoval(ctx, instances[index])
	})
}

func (ig *InstanceGroup) isProxmoxResourceAnInstance(member proxmox.ClusterResource) bool {
	return member.Type == vmTypeQEMU && member.VMID != uint64(*ig.TemplateID)
}
