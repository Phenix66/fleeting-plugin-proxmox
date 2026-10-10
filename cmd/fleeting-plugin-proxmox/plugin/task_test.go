package plugin

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
)

// testTaskUPID is the UPID of the fake clone task the wait tests poll.
func testTaskUPID() proxmox.UPID {
	return testUPID("qmclone")
}

// testUPID builds the UPID of a task of the given type against the fake node.
func testUPID(taskType string) proxmox.UPID {
	return proxmox.UPID(fmt.Sprintf("UPID:pve-node:00001A2B:00000000:00000000:%s:100:root@pam:", taskType))
}

// taskStatusBody builds a /status response, echoing back upid/node/type/id/user as real
// Proxmox responses do.
func taskStatusBody(taskType, status, exitStatus string) string {
	return fmt.Sprintf(`{"data":{"upid":%q,"node":"pve-node","type":%q,"id":"100","user":"root@pam","status":%q,"exitstatus":%q}}`,
		testUPID(taskType), taskType, status, exitStatus)
}

// taskHandler serves a task's /status and /log endpoints, reporting the given outcome, and
// fails the test on any other request. Servers that need more routes fall through to it.
func taskHandler(t *testing.T, taskType, status, exitStatus, logLine string) http.HandlerFunc {
	t.Helper()

	return func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/status"):
			fmt.Fprint(writer, taskStatusBody(taskType, status, exitStatus))
		case strings.HasSuffix(request.URL.Path, "/log"):
			fmt.Fprintf(writer, `{"data":[{"n":1,"t":%q}]}`, logLine)
		default:
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
		}
	}
}

// newWaitTestGroup is the minimum InstanceGroup waitTask needs: a poll interval and a logger.
func newWaitTestGroup() *InstanceGroup {
	waitInterval := 1

	return &InstanceGroup{
		ProxmoxTaskWaitInterval: &waitInterval,
		log:                     hclog.NewNullLogger(),
	}
}

func TestClassifyTask(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		status      string
		exitStatus  string
		expectedErr error
		expectText  string
	}{
		{
			name:       "success",
			status:     taskStatusStopped,
			exitStatus: taskExitStatusOK,
		},
		{
			// Proxmox omits the exit status for some task types.
			name:   "omitted exit status",
			status: taskStatusStopped,
		},
		{
			name:        "real failure",
			status:      taskStatusStopped,
			exitStatus:  "unable to parse volume ID 'local-lvm:'",
			expectedErr: ErrTaskFailed,
			expectText:  "unable to parse volume ID 'local-lvm:'",
		},
		{
			// A successful poll whose response carries no status field leaves task.Status and
			// ExitStatus empty, which Task.Wait reports as no-longer-running. Falling through
			// to the exit status check would read that as success.
			name:        "blank status poll",
			expectedErr: ErrTaskFailed,
			expectText:  "never observed as stopped",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := classifyTask(testCase.status, testCase.exitStatus)

			if testCase.expectedErr == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, testCase.expectedErr)

			if testCase.expectText != "" {
				require.Contains(t, err.Error(), testCase.expectText)
			}
		})
	}
}

// One end-to-end case for the wiring: classifyTask's verdict reaches the caller, and a real
// failure pulls Proxmox's own explanation into the log. The branch matrix is TestClassifyTask's.
func TestInstanceGroup_waitTask(t *testing.T) {
	t.Parallel()

	var logFetched atomic.Bool

	handler := taskHandler(t, "qmclone", taskStatusStopped, "unable to parse volume ID 'local-lvm:'", "volume parse failed")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/log") {
			logFetched.Store(true)
		}

		handler(w, r)
	}))
	defer server.Close()

	task := proxmox.NewTask(testTaskUPID(), proxmox.NewClient(server.URL))

	err := newWaitTestGroup().waitTask(context.Background(), task, time.Second)

	require.ErrorIs(t, err, ErrTaskFailed)
	require.Contains(t, err.Error(), "unable to parse volume ID 'local-lvm:'")
	require.True(t, logFetched.Load(), "expected waitTask to fetch the task log")
}

func TestInstanceGroup_waitTaskNilTask(t *testing.T) {
	t.Parallel()

	// An operation Proxmox answers with null data yields no task to wait on. That is not an
	// error here -- it may be a synchronous completion -- but it is never silent.
	log, logBuffer := newLogBuffer(t)

	group := newWaitTestGroup()
	group.log = log

	require.NoError(t, group.waitTask(context.Background(), nil, time.Second))
	require.Regexp(t, `\[WARN\].*Proxmox returned no task to wait on`, logBuffer.String())
}

// A task that keeps reporting running times out instead of being trusted: Wait's timeout is
// surfaced to the caller.
func TestInstanceGroup_waitTaskTimesOutOnRunningTask(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(taskHandler(t, "qmclone", "running", "", ""))
	defer server.Close()

	task := proxmox.NewTask(testTaskUPID(), proxmox.NewClient(server.URL))

	err := newWaitTestGroup().waitTask(context.Background(), task, 100*time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed while waiting for task")
}

// A poll whose response carries no status field is never trusted as success, and a task that
// was never observed as stopped has no log to fetch.
func TestInstanceGroup_waitTaskBlankStatusDoesNotFetchLog(t *testing.T) {
	t.Parallel()

	var logFetched atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/log") {
			logFetched.Store(true)
		}

		fmt.Fprint(w, taskStatusBody("qmclone", "", ""))
	}))
	defer server.Close()

	task := proxmox.NewTask(testTaskUPID(), proxmox.NewClient(server.URL))

	err := newWaitTestGroup().waitTask(context.Background(), task, time.Second)
	require.ErrorIs(t, err, ErrTaskFailed)
	require.Contains(t, err.Error(), "never observed as stopped")
	require.False(t, logFetched.Load(), "a task never observed as stopped has no log worth fetching")
}

// A failure to fetch the task log is reported alongside the task failure, never in place of
// it: the exit status is what the caller acts on.
func TestInstanceGroup_waitTaskLogFailureDoesNotMaskTaskFailure(t *testing.T) {
	t.Parallel()

	log, logBuffer := newLogBuffer(t)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/log") {
			http.Error(writer, "log unavailable", http.StatusInternalServerError)

			return
		}

		fmt.Fprint(writer, taskStatusBody("qmclone", taskStatusStopped, "unable to parse volume ID 'local-lvm:'"))
	}))
	defer server.Close()

	group := newWaitTestGroup()
	group.log = log

	task := proxmox.NewTask(testTaskUPID(), proxmox.NewClient(server.URL))

	err := group.waitTask(context.Background(), task, time.Second)
	require.ErrorIs(t, err, ErrTaskFailed)
	require.Contains(t, err.Error(), "unable to parse volume ID 'local-lvm:'")
	require.Contains(t, logBuffer.String(), "logerr", "the failed log fetch must be logged")
}

// taskLog orders the page by line number: Proxmox's log endpoint keys its lines by number,
// and a JSON object's key order is not that order.
func TestTaskLogOrdersLinesByLineNumber(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/log") {
			http.NotFound(writer, r)

			return
		}

		fmt.Fprint(writer, `{"data":[{"n":3,"t":"third line"},{"n":1,"t":"first line"},{"n":2,"t":"second line"}]}`)
	}))
	defer server.Close()

	task := proxmox.NewTask(testTaskUPID(), proxmox.NewClient(server.URL))

	lines, err := taskLog(context.Background(), task)
	require.NoError(t, err)
	require.Equal(t, []string{"first line", "second line", "third line"}, lines)
}
