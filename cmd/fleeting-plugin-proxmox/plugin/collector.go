package plugin

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/luthermonson/go-proxmox"
)

const (
	collectionInterval         = 1 * time.Minute
	collectionTimeout          = 5 * time.Minute
	collectionWaitAfterTrigger = 10 * time.Second
)

func (ig *InstanceGroup) startRemovedInstanceCollector() {
	ig.collectorWaitGroup.Go(func() {
		ig.runRemovedInstanceCollector()
	})
}

func (ig *InstanceGroup) runRemovedInstanceCollector() {
	ig.collectRemovedInstances()

	for {
		select {
		case <-ig.collectorShutdownTrigger:
			return
		case <-time.After(collectionInterval):
			ig.collectRemovedInstances()
		case <-ig.instanceCollectionTrigger:
			// Sleep for a bit to give Proxmox a chance to propagate renames that happened before trigger
			<-time.After(collectionWaitAfterTrigger)

			ig.collectRemovedInstances()
		}
	}
}

func (ig *InstanceGroup) collectRemovedInstances() {
	ctx, cancel := context.WithTimeout(context.Background(), collectionTimeout)
	defer cancel()

	ig.instanceCloningMu.Lock()

	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		ig.log.Error("collector failed to list instances", "err", err)
		ig.instanceCloningMu.Unlock()

		return
	}

	ig.instanceCloningMu.Unlock()

	var wg sync.WaitGroup
	defer wg.Wait()

	for _, member := range pool.Members {
		if !ig.isProxmoxResourceAnInstance(member) {
			continue
		}

		if member.Name != ig.InstanceNameRemoving {
			continue
		}

		ig.log.Info("collector found instance to remove", "vmid", member.VMID, "name", member.Name)

		wg.Add(1)

		go func(member proxmox.ClusterResource) {
			defer wg.Done()

			ig.collectInstance(ctx, member)
		}(member)
	}
}

func (ig *InstanceGroup) collectInstance(ctx context.Context, member proxmox.ClusterResource) {
	vm, usable := ig.fetchToCollect(ctx, &member)
	if !usable {
		return
	}

	if vm.Status == "running" {
		task, err := vm.Stop(ctx)
		if err == nil {
			err = ig.waitTask(ctx, task, collectionTimeout)
		}

		if err != nil {
			ig.log.Error("collector failed to stop instance", "vmid", member.VMID, "err", err)
			return
		}

		// The stop took at least one task poll; check the name again right before the delete,
		// which cannot be undone.
		vm, usable = ig.fetchToCollect(ctx, &member)
		if !usable {
			return
		}
	}

	task, err := vm.Delete(ctx, nil)
	if err == nil {
		err = ig.waitTask(ctx, task, collectionTimeout)
	}

	if err != nil {
		ig.log.Error("collector failed to delete instance", "vmid", member.VMID, "err", err)
	}
}

// fetchToCollect fetches a listed member through getListedVM, logging a failed fetch; false
// means leave the VM alone.
func (ig *InstanceGroup) fetchToCollect(ctx context.Context, member *proxmox.ClusterResource) (*proxmox.VirtualMachine, bool) {
	vm, err := ig.getListedVM(ctx, member)
	if err != nil {
		// getListedVM has already logged the refusal; log it otherwise.
		if !errors.Is(err, ErrNotOwned) {
			ig.log.Error("collector failed to fetch instance info", "vmid", member.VMID, "err", err)
		}

		return nil, false
	}

	return vm, true
}

// triggerCollection wakes up the collector without blocking. The channel is a
// wake-up signal, not a queue: a full channel already means a run is pending.
func (ig *InstanceGroup) triggerCollection() {
	select {
	case ig.instanceCollectionTrigger <- struct{}{}:
	default:
	}
}
