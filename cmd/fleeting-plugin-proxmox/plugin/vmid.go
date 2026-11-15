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

	// takenVMIDCacheTTL is how long a VMID probed as occupied by a VM the plugin cannot see is
	// remembered as occupied, so a burst of clones skips re-probing the same foreign VMIDs. It
	// expires quickly enough that a foreign VMID freed within it is reconsidered on the next
	// allocation; see rememberTaken.
	takenVMIDCacheTTL = 60 * time.Second

	// maxVMIDProbes bounds how many VMIDs cloneTemplate probes for a free one, past the next
	// free one while skipping quarantined ones, or between vmid_min and vmid_max.
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

	// ErrVMIDRangeFull reports that no free VMID was found between vmid_min and vmid_max.
	ErrVMIDRangeFull = errors.New("no free VMID")
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

// isRecentlyTaken reports whether vmid was probed as occupied by a VM the plugin cannot see
// within takenVMIDCacheTTL, so allocateVMIDInRange skips re-probing it.
func (ig *InstanceGroup) isRecentlyTaken(vmid int) bool {
	ig.takenMu.Lock()
	defer ig.takenMu.Unlock()

	takenAt, ok := ig.taken[vmid]

	return ok && time.Since(takenAt) < takenVMIDCacheTTL
}

// rememberTaken records that vmid was probed as occupied by a VM the plugin cannot see, so a
// burst of clones skips re-probing it for takenVMIDCacheTTL. Only "occupied" is cached, never
// "free": a free VMID is unreserved and must be re-checked right before every clone. Expired
// entries are dropped on the way.
func (ig *InstanceGroup) rememberTaken(vmid int) {
	ig.takenMu.Lock()
	defer ig.takenMu.Unlock()

	if ig.taken == nil {
		ig.taken = make(map[int]time.Time)
	}

	for takenVMID, takenAt := range ig.taken {
		if time.Since(takenAt) >= takenVMIDCacheTTL {
			delete(ig.taken, takenVMID)
		}
	}

	ig.taken[vmid] = time.Now()
}

// allocateVMID returns the VMID the next clone must use, or zero to let the vendor pick
// Proxmox's next free one. The caller holds the cloning lock.
func (ig *InstanceGroup) allocateVMID(ctx context.Context) (int, error) {
	lower, upper, ok := ig.vmidBounds()
	if ok {
		return ig.allocateVMIDInRange(ctx, lower, upper)
	}

	return ig.nextUnquarantinedVMID(ctx)
}

// allocateVMIDInRange returns the lowest free VMID between vmid_min and vmid_max that is not
// quarantined. The cluster's VM listing spares the probes of the VMIDs it shows as used, but
// it only shows the VMs the plugin may see, and Proxmox reserves nothing: another manager can
// take a VMID at any time, so every candidate left is still probed, at most maxVMIDProbes.
//
// When every free VMID of the range is quarantined, the one freed the longest ago is reused
// rather than failing the clone: a range sized close to max_instances would otherwise refuse
// new instances for the whole quarantine after a burst of removals, and the provisioner has
// most likely forgotten the oldest one by then.
func (ig *InstanceGroup) allocateVMIDInRange(ctx context.Context, lower, upper int) (int, error) {
	used, err := ig.listUsedVMIDs(ctx)
	if err != nil {
		return 0, err
	}

	fallback, probes := 0, 0

	var fallbackFreedAt time.Time

	for candidate := lower; candidate <= upper && probes < maxVMIDProbes; candidate++ {
		if used[candidate] {
			continue
		}

		// A VMID probed as occupied by a VM the plugin cannot see is remembered briefly, so a
		// burst of clones does not pay the round-trip for the same foreign VMIDs again.
		if ig.isRecentlyTaken(candidate) {
			continue
		}

		freedAt, quarantined := ig.quarantinedAt(candidate)
		if quarantined && fallback != 0 && !freedAt.Before(fallbackFreedAt) {
			continue
		}

		probes++

		free, err := ig.isVMIDFree(ctx, candidate)
		if err != nil {
			return 0, err
		}

		if !free {
			ig.rememberTaken(candidate)

			continue
		}

		if !quarantined {
			return candidate, nil
		}

		fallback, fallbackFreedAt = candidate, freedAt
	}

	if fallback != 0 {
		ig.log.Warn("every free VMID between vmid_min and vmid_max was freed recently, reusing the oldest one",
			"vmid", fallback, "freed_at", fallbackFreedAt, "vmid_min", lower, "vmid_max", upper)

		return fallback, nil
	}

	return 0, fmt.Errorf("%w between %d and %d (probed %d)", ErrVMIDRangeFull, lower, upper, probes)
}

// listUsedVMIDs returns the VMIDs of the guests, VMs and containers, the cluster shows.
func (ig *InstanceGroup) listUsedVMIDs(ctx context.Context) (map[int]bool, error) {
	var guests []struct {
		VMID int `json:"vmid"`
	}

	err := ig.proxmox.Get(ctx, "/cluster/resources?type=vm", &guests)
	if err != nil {
		return nil, fmt.Errorf("failed to list the cluster's VMIDs: %w", err)
	}

	used := make(map[int]bool, len(guests))

	for _, guest := range guests {
		used[guest.VMID] = true
	}

	return used, nil
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
