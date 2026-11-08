package plugin

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// freedVMIDQuarantine is how long a VMID the collector has deleted is kept from new clones.
	// Instance ids are VMIDs, and the provisioner matches the instances Update reports to the
	// ones it knows by id alone: a clone that reuses a VMID before the provisioner has seen it
	// disappear inherits the deleted instance's record, marked for removal, and is never used.
	// The provisioner forgets a removed instance on the first Update that no longer lists it,
	// once a minute by default, so this leaves it many updates to do so.
	freedVMIDQuarantine = 15 * time.Minute

	// maxVMIDProbes bounds how many VMIDs past the next free one cloneTemplate probes while
	// skipping quarantined ones.
	maxVMIDProbes = 1000

	// maxCloneAttempts bounds how many VMIDs cloneTemplate tries when Proxmox refuses a clone
	// because its VMID was taken in the meantime. Proxmox has no way to reserve a VMID: it is
	// read as free first and only taken by the clone, so another client cloning at the same
	// time, such as another runner using the same cluster, can take it in between.
	maxCloneAttempts = 5
)

var (
	// ErrNoFreeVMID reports that no VMID outside the quarantine was found within maxVMIDProbes.
	ErrNoFreeVMID = errors.New("no free VMID outside the reuse quarantine")

	// ErrVMIDRangeFull reports that every VMID in vmid_range is taken.
	ErrVMIDRangeFull = errors.New("no free VMID in vmid_range")
)

// trackFreed records that the collector has deleted vmid, so cloneTemplate keeps it from new
// clones for freedVMIDQuarantine. Expired entries are dropped on the way.
func (ig *InstanceGroup) trackFreed(vmid int) {
	ig.freedMu.Lock()
	defer ig.freedMu.Unlock()

	if ig.freed == nil {
		ig.freed = make(map[int]time.Time)
	}

	for freedVMID, freedAt := range ig.freed {
		if time.Since(freedAt) >= freedVMIDQuarantine {
			delete(ig.freed, freedVMID)
		}
	}

	ig.freed[vmid] = time.Now()
}

// isQuarantined reports whether vmid was freed by the collector within freedVMIDQuarantine.
func (ig *InstanceGroup) isQuarantined(vmid int) bool {
	_, quarantined := ig.quarantinedAt(vmid)

	return quarantined
}

// quarantinedAt returns when the collector freed vmid, if it is still quarantined.
func (ig *InstanceGroup) quarantinedAt(vmid int) (time.Time, bool) {
	ig.freedMu.Lock()
	defer ig.freedMu.Unlock()

	freedAt, ok := ig.freed[vmid]
	if !ok || time.Since(freedAt) >= freedVMIDQuarantine {
		return time.Time{}, false
	}

	return freedAt, true
}

// hasQuarantine reports whether any VMID is currently quarantined.
func (ig *InstanceGroup) hasQuarantine() bool {
	ig.freedMu.Lock()
	defer ig.freedMu.Unlock()

	for _, freedAt := range ig.freed {
		if time.Since(freedAt) < freedVMIDQuarantine {
			return true
		}
	}

	return false
}

// allocateVMID returns the VMID the next clone must use, or zero to let the vendor pick
// Proxmox's next free one. The caller holds the cloning lock.
func (ig *InstanceGroup) allocateVMID(ctx context.Context) (int, error) {
	lower, upper, ok := ig.vmidRange()
	if ok {
		return ig.allocateVMIDInRange(ctx, lower, upper)
	}

	return ig.nextUnquarantinedVMID(ctx)
}

// allocateVMIDInRange returns the lowest free VMID of vmid_range that is not quarantined.
// Proxmox reserves nothing: whether a VMID is free is only known by asking, and another
// manager can take one at any time, so every candidate is probed. The pool listing only
// spares the probes of the VMIDs this group's own instances hold.
//
// When every free VMID of the range is quarantined, the one freed the longest ago is reused
// rather than failing the clone: a range sized close to max_instances would otherwise refuse
// new instances for the whole quarantine after a burst of removals, and the provisioner has
// most likely forgotten the oldest one by then.
func (ig *InstanceGroup) allocateVMIDInRange(ctx context.Context, lower, upper int) (int, error) {
	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		return 0, err
	}

	used := make(map[int]bool, len(pool.Members))

	for _, member := range pool.Members {
		//nolint:gosec//G115 // member.VMID is a Proxmox VMID, which always fits in an int
		used[int(member.VMID)] = true
	}

	fallback := 0

	var fallbackFreedAt time.Time

	for candidate := lower; candidate <= upper; candidate++ {
		if used[candidate] {
			continue
		}

		freedAt, quarantined := ig.quarantinedAt(candidate)
		if quarantined && fallback != 0 && !freedAt.Before(fallbackFreedAt) {
			continue
		}

		free, err := ig.isVMIDFree(ctx, candidate)
		if err != nil {
			return 0, err
		}

		if !free {
			continue
		}

		if !quarantined {
			return candidate, nil
		}

		fallback, fallbackFreedAt = candidate, freedAt
	}

	if fallback != 0 {
		ig.log.Warn("every free VMID in vmid_range was freed recently, reusing the oldest one",
			"vmid", fallback, "freed_at", fallbackFreedAt, "vmid_range", ig.VMIDRange)

		return fallback, nil
	}

	return 0, fmt.Errorf("%w %q", ErrVMIDRangeFull, ig.VMIDRange)
}

// nextUnquarantinedVMID returns the VMID the next clone must use, or zero to let the vendor
// pick Proxmox's next free one as it always has. Proxmox hands out the lowest free VMID, which
// right after a removal is the one just deleted: when that one is quarantined, the VMIDs above
// it are probed for one that is both free and not quarantined.
func (ig *InstanceGroup) nextUnquarantinedVMID(ctx context.Context) (int, error) {
	if !ig.hasQuarantine() {
		return 0, nil
	}

	var next string

	err := ig.proxmox.Get(ctx, "/cluster/nextid", &next)
	if err != nil {
		return 0, fmt.Errorf("failed to get the next free VMID: %w", err)
	}

	candidate, err := strconv.Atoi(next)
	if err != nil {
		return 0, fmt.Errorf("failed to parse the next free VMID %q: %w", next, err)
	}

	for range maxVMIDProbes {
		if ig.isQuarantined(candidate) {
			ig.log.Debug("skipping recently freed VMID for a new clone", "vmid", candidate)

			candidate++

			continue
		}

		free, err := ig.isVMIDFree(ctx, candidate)
		if err != nil {
			return 0, err
		}

		if free {
			return candidate, nil
		}

		candidate++
	}

	return 0, fmt.Errorf("%w: probed %d VMIDs", ErrNoFreeVMID, maxVMIDProbes)
}

// isVMIDFree asks Proxmox whether vmid is free. The vendor's CheckID cannot be used: it passes
// its result by value, so every answer for a free VMID fails to decode.
func (ig *InstanceGroup) isVMIDFree(ctx context.Context, vmid int) (bool, error) {
	var answer string

	err := ig.proxmox.Get(ctx, fmt.Sprintf("/cluster/nextid?vmid=%d", vmid), &answer)
	if err == nil {
		return true, nil
	}

	if strings.Contains(err.Error(), "already exists") {
		return false, nil
	}

	return false, fmt.Errorf("failed to check whether VMID %d is free: %w", vmid, err)
}

// isVMIDTakenError reports whether err is Proxmox refusing a clone because vmid was taken
// since it was read as free. The clone handler checks for the new VMID's config file before
// it starts the clone task and answers "unable to create VM <vmid>: config file already
// exists".
func isVMIDTakenError(err error, vmid int) bool {
	if err == nil {
		return false
	}

	message := err.Error()

	return strings.Contains(message, "already exists") && strings.Contains(message, strconv.Itoa(vmid))
}
