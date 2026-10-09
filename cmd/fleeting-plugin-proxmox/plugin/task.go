package plugin

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/luthermonson/go-proxmox"
)

const (
	// taskStatusStopped is the status Proxmox reports for a task that has finished.
	taskStatusStopped = "stopped"

	// taskExitStatusOK is the exit status Proxmox reports for a task that succeeded.
	taskExitStatusOK = "OK"

	// taskLogLineLimit is the most log lines ever fetched for a failure log entry.
	taskLogLineLimit = 50
)

var (
	ErrTaskFailed = errors.New("proxmox task failed")

	// ErrNoTask reports an operation Proxmox accepted without returning a task to wait on, so
	// its outcome was never observed.
	ErrNoTask = errors.New("proxmox returned no task")
)

// classifyTask decides a finished task's outcome from what Task.Ping already populated. It is
// pure so the branch matrix is table-testable without an HTTP server.
func classifyTask(status, exitStatus string) error {
	// Task.Wait returns nil for any reported status other than running; a poll that fails
	// returns an error and never reaches this check. The empty status guarded against here
	// is a successful poll whose response carried no status field, on a task never yet
	// populated. Only a task actually observed as stopped can be trusted, so this
	// deliberately does NOT fall through to the exit status check below: an empty status has
	// an empty exit status too, which would otherwise read as success.
	if status != taskStatusStopped {
		return fmt.Errorf("%w: never observed as stopped (status '%s')", ErrTaskFailed, status)
	}

	// Proxmox omits the exit status for some task types; on a stopped task, absent means
	// success -- which is why this is not task.IsSuccessful, as the client reports an omitted
	// exit status as failed.
	if exitStatus == "" || exitStatus == taskExitStatusOK {
		return nil
	}

	return fmt.Errorf("%w: %s", ErrTaskFailed, exitStatus)
}

// waitTask waits for a Proxmox task to finish and turns a non-OK exit status into an error.
func (ig *InstanceGroup) waitTask(ctx context.Context, task *proxmox.Task, timeout time.Duration) error {
	// go-proxmox's NewTask returns nil for an empty UPID, so an operation Proxmox answers with
	// null data yields no task. That may be a synchronous completion, so it is not an error
	// here -- but it is never silent, and a caller for which "no task" means "not verified"
	// checks for nil itself before calling this.
	if task == nil {
		ig.log.Warn("Proxmox returned no task to wait on, treating the operation as complete")

		return nil
	}

	interval := time.Duration(*ig.ProxmoxTaskWaitInterval) * time.Second

	err := task.Wait(ctx, interval, timeout)
	if err != nil {
		return fmt.Errorf("failed while waiting for task '%s': %w", task.UPID, err)
	}

	err = classifyTask(task.Status, task.ExitStatus)
	if err == nil {
		return nil
	}

	// Fetch the log only when the failure carries an exit status: a task never observed as
	// stopped has none, and no log worth fetching either. The fetch is best effort -- a
	// failure to fetch is reported alongside the task failure, never in place of it.
	if task.ExitStatus != "" {
		logLines, logErr := taskLog(ctx, task)

		ig.log.Error("Proxmox task failed", "upid", task.UPID, "exitstatus", task.ExitStatus, "log", logLines, "logerr", logErr)
	}

	return fmt.Errorf("task '%s': %w", task.UPID, err)
}

// taskLog returns the first page of a task's log in order. task.ExitStatus already carries
// Proxmox's own reason for a failure, so one page of context is enough for the log entry.
func taskLog(ctx context.Context, task *proxmox.Task) ([]string, error) {
	lines, err := task.Log(ctx, 0, taskLogLineLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch log for task '%s': %w", task.UPID, err)
	}

	// proxmox.Log is keyed by absolute line number, so sorting the keys orders the page.
	ordered := make([]string, 0, len(lines))

	for _, number := range slices.Sorted(maps.Keys(lines)) {
		ordered = append(ordered, lines[number])
	}

	return ordered, nil
}
