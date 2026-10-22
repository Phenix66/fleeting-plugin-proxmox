package plugin

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/luthermonson/go-proxmox"
)

var (
	ErrInvalidSize = errors.New("invalid size")

	// ErrDiskNotFound reports a disk the configuration names that the VM does not have.
	ErrDiskNotFound = errors.New("disk not found")

	// ErrAutoresizeNotApplied reports a resize Proxmox accepted, or answered without a task, that
	// did not leave the disk at the size it was asked for.
	ErrAutoresizeNotApplied = errors.New("disk was not resized to the expected size")
)

// storageFullLogInterval throttles the warning about refused clones. Taskscaler retries an
// unmet demand every couple of seconds, so a full storage would otherwise warn just as often.
const storageFullLogInterval = time.Minute

// Binary size units Proxmox accepts, as shifts of a byte count.
const (
	kibShift = 10
	mibShift = 20
	gibShift = 30
	tibShift = 40
)

var proxmoxSizeRE = regexp.MustCompile(`^(\d+(?:\.\d+)?)([KMGT]?)$`)

// parseProxmoxSize converts a Proxmox size -- a disk's size= option, or an absolute resize size
// -- to bytes. Units are binary, and a size without a unit is in bytes.
func parseProxmoxSize(size string) (uint64, error) {
	matches := proxmoxSizeRE.FindStringSubmatch(size)
	if matches == nil {
		return 0, fmt.Errorf("%w: %q", ErrInvalidSize, size)
	}

	value, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q: %w", ErrInvalidSize, size, err)
	}

	multiplier := map[string]float64{
		"":  1,
		"K": 1 << kibShift,
		"M": 1 << mibShift,
		"G": 1 << gibShift,
		"T": 1 << tibShift,
	}[matches[2]]

	return uint64(math.Ceil(value * multiplier)), nil
}

// diskSpecSize returns the size a disk option such as "local-lvm:vm-100-disk-0,size=10G"
// allocates on the storage. The bool is false for an option that allocates nothing there: a
// CD-ROM drive (an ISO, a cloud-init drive) or an empty drive.
func diskSpecSize(spec string) (uint64, bool, error) {
	parts := strings.Split(spec, ",")

	if parts[0] == "none" {
		return 0, false, nil
	}

	size := ""

	for _, part := range parts[1:] {
		key, value, _ := strings.Cut(part, "=")

		switch key {
		case "media":
			if value == "cdrom" {
				return 0, false, nil
			}
		case "size":
			size = value
		}
	}

	if size == "" {
		return 0, false, nil
	}

	bytes, err := parseProxmoxSize(size)
	if err != nil {
		return 0, false, err
	}

	return bytes, true, nil
}

// vmDisks returns every disk option of a VM's configuration, keyed by its name ("scsi0",
// "efidisk0", ...).
func vmDisks(config *proxmox.VirtualMachineConfig) map[string]string {
	disks := make(map[string]string)

	if config == nil {
		return disks
	}

	for _, devices := range []map[string]string{config.IDEs, config.SCSIs, config.SATAs, config.VirtIOs} {
		maps.Copy(disks, devices)
	}

	if config.EFIDisk0 != "" {
		disks["efidisk0"] = config.EFIDisk0
	}

	if config.TPMState0 != "" {
		disks["tpmstate0"] = config.TPMState0
	}

	return disks
}

// autoresizedSize returns the size the autoresize disk must have after a resize from current.
func (ig *InstanceGroup) autoresizedSize(current uint64) (uint64, error) {
	if increment, ok := strings.CutPrefix(ig.InstanceAutoresizeSize, "+"); ok {
		bytes, err := parseProxmoxSize(increment)
		if err != nil {
			return 0, err
		}

		return current + bytes, nil
	}

	bytes, err := parseProxmoxSize(ig.InstanceAutoresizeSize)
	if err != nil {
		return 0, err
	}

	// Proxmox refuses to shrink a disk, so an absolute size below the current one fails the
	// resize; never count less than the disk already takes.
	return max(current, bytes), nil
}

// instanceFootprint returns the storage space one instance cloned from template allocates: the
// size of every disk it carries over, with the autoresize disk counted at its resized size.
// It assumes a full clone, which is what the clone performs whenever storage is set.
func (ig *InstanceGroup) instanceFootprint(template *proxmox.VirtualMachine) (uint64, error) {
	var (
		footprint      uint64
		autoresizeSeen bool
	)

	for name, spec := range vmDisks(template.VirtualMachineConfig) {
		size, allocates, err := diskSpecSize(spec)
		if err != nil {
			return 0, fmt.Errorf("disk '%s' of template: %w", name, err)
		}

		if !allocates {
			continue
		}

		if name == ig.InstanceAutoresizeDisk {
			autoresizeSeen = true

			size, err = ig.autoresizedSize(size)
			if err != nil {
				return 0, fmt.Errorf("instance_autoresize_size: %w", err)
			}
		}

		footprint += size
	}

	if ig.InstanceAutoresizeDisk != "" && !autoresizeSeen {
		return 0, fmt.Errorf("%w: template has no disk '%s' to autoresize", ErrDiskNotFound, ig.InstanceAutoresizeDisk)
	}

	return footprint, nil
}

// instancesThatFit returns how many of count instances cloned from template fit on the
// storage while leaving storage_reserve free. It reads the storage's free space once per
// Increase: the batch's own clones are accounted for by the footprint of each, and clones by
// another manager on the same storage in the meantime are what the reserve is for.
func (ig *InstanceGroup) instancesThatFit(ctx context.Context, template *proxmox.VirtualMachine, count int) (int, error) {
	footprint, err := ig.instanceFootprint(template)
	if err != nil {
		return 0, err
	}

	reserve, err := parseProxmoxSize(ig.StorageReserve)
	if err != nil {
		return 0, fmt.Errorf("storage_reserve: %w", err)
	}

	nodeName := ig.InstanceTargetNode
	if nodeName == "" {
		nodeName = template.Node
	}

	node, err := ig.proxmox.Node(ctx, nodeName)
	if err != nil {
		return 0, fmt.Errorf("failed to get node='%s': %w", nodeName, err)
	}

	storage, err := node.Storage(ctx, ig.Storage)
	if err != nil {
		return 0, fmt.Errorf("failed to get status of storage='%s' on node='%s': %w", ig.Storage, nodeName, err)
	}

	fits := 0

	if footprint == 0 {
		fits = count
	} else if storage.Avail > reserve {
		//nolint:gosec//G115 // count is a positive instance count and the result is at most count
		fits = int(min(uint64(count), (storage.Avail-reserve)/footprint))
	}

	if fits < count {
		ig.warnStorageFull("not enough free space on storage, cloning fewer instances than requested",
			"storage", ig.Storage, "node", nodeName, "requested", count, "cloning", fits,
			"avail", storage.Avail, "reserve", reserve, "instance_footprint", footprint)
	}

	return fits, nil
}

// warnStorageFull logs a refused clone at warn level at most once per storageFullLogInterval,
// and at debug level otherwise.
func (ig *InstanceGroup) warnStorageFull(msg string, args ...any) {
	ig.storageFullLogMu.Lock()

	warn := time.Since(ig.storageFullLoggedAt) >= storageFullLogInterval
	if warn {
		ig.storageFullLoggedAt = time.Now()
	}
	ig.storageFullLogMu.Unlock()

	if warn {
		ig.log.Warn(msg, args...)

		return
	}

	ig.log.Debug(msg, args...)
}

// autoresizeDisk grows the autoresize disk of a freshly cloned instance and checks the result
// on the VM itself. Neither a successful task nor the absence of one is taken as proof: the
// instance only counts as resized once its configuration reports the expected size.
func (ig *InstanceGroup) autoresizeDisk(ctx context.Context, vm *proxmox.VirtualMachine) error {
	current, err := ig.autoresizeDiskSize(vm)
	if err != nil {
		return err
	}

	expected, err := ig.autoresizedSize(current)
	if err != nil {
		return fmt.Errorf("instance_autoresize_size: %w", err)
	}

	task, err := vm.ResizeDisk(ctx, ig.InstanceAutoresizeDisk, ig.InstanceAutoresizeSize)
	if err == nil {
		err = ig.waitTask(ctx, task, ig.taskWaitTimeout())
	}

	if err != nil {
		return err
	}

	err = vm.Ping(ctx)
	if err != nil {
		return fmt.Errorf("failed to read back the instance configuration: %w", err)
	}

	actual, err := ig.autoresizeDiskSize(vm)
	if err != nil {
		return err
	}

	if actual < expected {
		return fmt.Errorf("%w: disk '%s' is %d bytes, expected %d", ErrAutoresizeNotApplied, ig.InstanceAutoresizeDisk, actual, expected)
	}

	return nil
}

// autoresizeDiskSize returns the current size of the autoresize disk on vm.
func (ig *InstanceGroup) autoresizeDiskSize(vm *proxmox.VirtualMachine) (uint64, error) {
	spec, ok := vmDisks(vm.VirtualMachineConfig)[ig.InstanceAutoresizeDisk]
	if !ok {
		return 0, fmt.Errorf("%w: instance vmid='%d' has no disk '%s'", ErrDiskNotFound, vm.VMID, ig.InstanceAutoresizeDisk)
	}

	size, allocates, err := diskSpecSize(spec)
	if err != nil {
		return 0, fmt.Errorf("disk '%s' of instance vmid='%d': %w", ig.InstanceAutoresizeDisk, vm.VMID, err)
	}

	if !allocates {
		return 0, fmt.Errorf("%w: disk '%s' of instance vmid='%d' has no size", ErrDiskNotFound, ig.InstanceAutoresizeDisk, vm.VMID)
	}

	return size, nil
}
