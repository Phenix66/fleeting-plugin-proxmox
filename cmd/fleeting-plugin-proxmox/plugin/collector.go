package plugin

import (
	"context"
	"errors"
	"time"

	"github.com/luthermonson/go-proxmox"
)

const (
	collectionInterval = 1 * time.Minute

	// collectionTimeout bounds one member's stop and delete within a collection round. The
	// round keeps no shared deadline of its own (see collectRemovedInstances); memberTimeout
	// exposes the bound per group so a test can shorten it.
	collectionTimeout = 5 * time.Minute

	collectionLockPoll           = 500 * time.Millisecond
	defaultCollectionLockWait    = 10 * time.Second
	defaultCollectionTriggerWait = 10 * time.Second
)

func (ig *InstanceGroup) startRemovedInstanceCollector() {
	ig.collectorWaitGroup.Go(func() {
		ig.runRemovedInstanceCollector()
	})
}

func (ig *InstanceGroup) runRemovedInstanceCollector() {
	ig.runRecoveringLoop("removed instance collector", ig.collectorShutdownTrigger, func() {
		ig.collectRemovedInstances()

		for {
			select {
			case <-ig.collectorShutdownTrigger:
				return
			case <-time.After(collectionInterval):
				ig.collectRemovedInstances()
			case <-ig.instanceCollectionTrigger:
				// A wake-up is a nudge, not a commitment. If shutdown was already closed when the
				// outer select picked this branch over a closed shutdown trigger, or it closes while
				// we wait for the rename to propagate, exit now. Without this inner check a
				// self-refilling trigger -- every round that marks an instance re-queues a wake-up --
				// keeps winning the outer select's random pick, and Shutdown is delayed by a whole
				// trigger wait per round.
				select {
				case <-ig.collectorShutdownTrigger:
					return
				case <-time.After(ig.triggerWait()):
				}

				ig.collectRemovedInstances()
			}
		}
	})
}

func (ig *InstanceGroup) collectRemovedInstances() {
	// The round keeps no deadline of its own: one clock shared by every member would starve
	// the later waves of the capped batch, whose members would inherit a budget the earlier
	// waves had already spent. Each member gets the full memberTimeout on a context of its
	// own instead, and the round is bounded by the batch cap times that budget.
	ctx := context.Background()

	// The listing must not race a clone: while one is in flight, a recycled VMID is reported
	// with its predecessor's stale name, and collecting from such a listing could stop or
	// delete a just-created instance. A held lock means the listing would be stale by
	// definition, so the round is skipped and its wake-up re-queued for the next attempt
	// instead of waiting the clone out.
	unlock, ok := ig.lockInstanceCloning(ctx)
	if !ok {
		ig.log.Info("collector skipping round: an instance clone is in progress")

		ig.triggerCollection()

		return
	}

	pool, err := ig.getProxmoxPool(ctx)

	unlock()

	if err != nil {
		ig.log.Error("collector failed to list instances", "err", err)

		return
	}

	members := make([]proxmox.ClusterResource, 0, len(pool.Members))

	stale := make([]*proxmox.ClusterResource, 0)

	for _, member := range pool.Members {
		if !ig.isProxmoxResourceAnInstance(member) {
			continue
		}

		if member.Name == "" {
			// An empty listed name is no evidence either way: Proxmox leaves it unset while
			// its status cache holds no fresh entry for the VM. Ask the VM itself, the way
			// Update and Decrease do, or the round would skip a removing instance -- possibly
			// still powered on -- until the node reports again, and a restart would not
			// change that: the Init sweep sees the same listing.
			nameErr := ig.nameFromNode(ctx, &member)
			if nameErr != nil {
				ig.log.Warn("collector cannot read the name of an instance, skipping it this round",
					"vmid", member.VMID, "err", nameErr)

				continue
			}
		}

		switch member.Name {
		case ig.InstanceNameRemoving:
			ig.log.Info("collector found instance to remove", "vmid", member.VMID, "name", member.Name, "node", member.Node)

			members = append(members, member)
		case ig.InstanceNameCreating:
			// A creating instance with no deployment in flight is an orphan a failed
			// deployment left: a failed clone, or a failed deployment whose rename to
			// removing also failed, keeps the creating name. The stale sweep at Init covers
			// orphans left by a previous run; this covers the ones this run's deployments
			// leave. Mark it for removal; the round the mark triggers collects it, and a
			// failed mark is retried on the next round.
			//nolint:gosec//G115 // member.VMID is a Proxmox VMID, which always fits in an int
			if ig.isDeploying(int(member.VMID)) {
				continue
			}

			ig.log.Info("collector found orphaned creating instance, marking for removal", "vmid", member.VMID, "name", member.Name, "node", member.Node)

			stale = append(stale, &member)
		}
	}

	// The orphans are renamed, not deleted here: deletion is the next round's job, and it
	// applies to them the same name re-check and the same per-member budget as to any
	// removing member.
	if len(stale) > 0 {
		// Each failure is already logged with its vmid where it happened; the aggregate
		// says how many of the round's orphans will retry on the next round.
		errs := ig.markInstancesForRemoval(ctx, stale)

		err := errors.Join(errs...)
		if err != nil {
			ig.log.Error("collector failed to mark some orphaned creating instances for removal",
				"attempted", len(stale), "err", err)
		}
	}

	// The per-instance work runs under the same batch cap as every other batch: a round can
	// name as many members as the pool holds, and unbounded fan-out would open one HTTP
	// connection and one task poller per member whatever the pool's size. collectInstance
	// logs each member's own failure, so the batch's slots stay nil.
	runParallel(len(members), func(index int) error {
		// A fresh budget per member: a member that starts in a later wave gets the same
		// memberTimeout the first wave did, and no other member can spend it.
		mctx, cancel := context.WithTimeout(ctx, ig.memberTimeout())
		defer cancel()

		ig.collectInstance(mctx, members[index])

		return nil
	})
}

// lockWait caps how long a collection round waits for the cloning lock before skipping the
// round: a held lock means a clone is in flight, and the listing the lock guards is stale
// for exactly as long as it is held, so waiting it out buys nothing -- the round is
// re-queued and retried once the lock is free. A per-group override wins over the default
// so a test can shorten it on its own group.
func (ig *InstanceGroup) lockWait() time.Duration {
	if ig.collectionLockWait > 0 {
		return ig.collectionLockWait
	}

	return defaultCollectionLockWait
}

// triggerWait paces a triggered round so a rename has time to propagate before it
// collects; a per-group override wins over the default for the same test reason.
func (ig *InstanceGroup) triggerWait() time.Duration {
	if ig.collectionTriggerWait > 0 {
		return ig.collectionTriggerWait
	}

	return defaultCollectionTriggerWait
}

// memberTimeout bounds one member's stop and delete within a collection round. A member that
// starts in a later wave of the batch cap gets the full budget: the round keeps no shared
// deadline, so the earlier waves cannot spend it. A per-group override wins over the default
// so a test can shorten it on its own group.
func (ig *InstanceGroup) memberTimeout() time.Duration {
	if ig.collectionMemberTimeout > 0 {
		return ig.collectionMemberTimeout
	}

	return collectionTimeout
}

// lockInstanceCloning acquires the cloning lock for the duration of one pool listing, giving
// up after the shorter of lockWait and the caller's context deadline, if any. A false second
// return means a clone is in flight and the caller must skip its round: a listing taken under
// a held lock is precisely the stale one the lock exists to prevent.
func (ig *InstanceGroup) lockInstanceCloning(ctx context.Context) (func(), bool) {
	waitFor := ig.lockWait()

	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < waitFor {
			waitFor = remaining
		}
	}

	deadline := time.Now().Add(waitFor)

	for {
		if ig.instanceCloningMu.TryLock() {
			return ig.instanceCloningMu.Unlock, true
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, false
		}

		poll := min(collectionLockPoll, remaining)

		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(poll):
		}
	}
}

func (ig *InstanceGroup) collectInstance(ctx context.Context, member proxmox.ClusterResource) {
	// A panic here (the vendor client can index past the end of a malformed UPID) takes
	// down only this worker: it is logged, and the rest of the collection run proceeds.
	defer func() {
		if recovered := recover(); recovered != nil {
			ig.log.Error("collector panicked while removing an instance", "vmid", member.VMID, "err", panicError(recovered))
		}
	}()

	vm, usable := ig.fetchToCollect(ctx, &member)
	if !usable {
		return
	}

	if vm.Status == taskStatusRunning {
		task, err := vm.Stop(ctx)
		if err == nil {
			err = ig.waitTask(ctx, uint64(vm.VMID), task, ig.memberTimeout())
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
		err = ig.waitTask(ctx, uint64(vm.VMID), task, ig.memberTimeout())
	}

	if err != nil {
		ig.log.Error("collector failed to delete instance", "vmid", member.VMID, "err", err)

		return
	}

	ig.log.Info("collector removed instance", "vmid", member.VMID, "node", member.Node)
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
