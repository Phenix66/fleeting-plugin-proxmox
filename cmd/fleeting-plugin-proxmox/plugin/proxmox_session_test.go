package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
)

// sessionTestTicketPath is the one route every ticket fake answers.
const sessionTestTicketPath = "/access/ticket"

// The refresher loop returns once the shutdown trigger fires, so Shutdown cannot hang on it.
func TestSessionTicketRefresherStopsOnShutdown(t *testing.T) {
	t.Parallel()

	ig := &InstanceGroup{
		log:                                   hclog.NewNullLogger(),
		sessionTicketRefresherShutdownTrigger: make(chan struct{}),
	}

	ig.startSessionTicketRefresher()

	requireShutdownReturns(t, ig, "with the session ticket refresher running")
}

// A failed refresh must be retried soon rather than waiting for the next renewal: the
// current ticket is at the end of its life when a renewal fails, so waiting out the normal
// interval leaves it to run out in the meantime. A success returns to the normal interval.
func TestSessionTicketRefresherRetriesAfterFailure(t *testing.T) {
	t.Parallel()

	normalInterval := 800 * time.Millisecond
	retryInterval := 200 * time.Millisecond

	failuresLeft := 2

	var lock sync.Mutex

	var requestTimes []time.Time

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != sessionTestTicketPath {
			http.NotFound(writer, request)

			return
		}

		lock.Lock()

		requestTimes = append(requestTimes, time.Now())

		failed := failuresLeft > 0

		if failed {
			failuresLeft--
		}
		lock.Unlock()

		if failed {
			http.Error(writer, "authentication failed", http.StatusUnauthorized)

			return
		}

		fmt.Fprint(writer, `{"data":{"ticket":"sample-ticket","userid":"test@pve","TTL":300}}`)
	}))
	t.Cleanup(server.Close)

	ig := newRefreshTestGroup(t, server.URL)
	ig.sessionTicketRefreshInterval = normalInterval
	ig.sessionTicketRefreshRetryInterval = retryInterval

	t.Cleanup(func() {
		close(ig.sessionTicketRefresherShutdownTrigger)
		ig.sessionTicketRefresherWaitGroup.Wait()
	})

	ig.startSessionTicketRefresher()

	// The sequence under test: a failed normal renewal, two failed retries, then a renewed
	// normal interval after the success.
	requested := func() int {
		lock.Lock()
		defer lock.Unlock()

		return len(requestTimes)
	}

	deadline := time.Now().Add(15 * time.Second)

	for requested() < 4 {
		if time.Now().After(deadline) {
			t.Fatalf("the refresher made %d ticket requests, want at least 4", requested())
		}

		time.Sleep(25 * time.Millisecond)
	}

	lock.Lock()
	first, second, third, fourth := requestTimes[0], requestTimes[1], requestTimes[2], requestTimes[3]
	lock.Unlock()

	// The retries beat the normal interval: without the retry, the second renewal would be a
	// full normal interval after the first.
	require.Less(t, second.Sub(first), normalInterval, "the first retry waited for the normal interval")
	require.Less(t, third.Sub(second), normalInterval, "the second retry waited for the normal interval")

	// The success returns to the normal interval.
	require.GreaterOrEqual(t, fourth.Sub(third), normalInterval-150*time.Millisecond, "the normal interval did not resume after the success")
	require.Less(t, fourth.Sub(third), normalInterval+500*time.Millisecond, "the normal interval ran long after the success")
}

func TestRefreshSessionTicket(t *testing.T) {
	t.Parallel()

	t.Run("exchanges the credentials for a ticket", func(t *testing.T) {
		t.Parallel()

		var ticketRequested atomic.Bool

		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.Method == http.MethodPost && request.URL.Path == sessionTestTicketPath {
				ticketRequested.Store(true)

				fmt.Fprint(writer, `{"data":{"ticket":"sample-ticket","userid":"test@pve","TTL":300}}`)

				return
			}

			http.NotFound(writer, request)
		}))
		t.Cleanup(server.Close)

		ig := newRefreshTestGroup(t, server.URL)

		err := ig.refreshSessionTicket(context.Background())
		require.NoError(t, err)
		require.True(t, ticketRequested.Load(), "no ticket was requested")
	})

	t.Run("missing credentials file", func(t *testing.T) {
		t.Parallel()

		ig := newRefreshTestGroup(t, "http://127.0.0.1:1")
		ig.CredentialsFilePath = path.Join(t.TempDir(), "does-not-exist.json")

		err := ig.refreshSessionTicket(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "could not read credentials")
	})

	t.Run("refused ticket", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "authentication failed", http.StatusUnauthorized)
		}))
		t.Cleanup(server.Close)

		ig := newRefreshTestGroup(t, server.URL)

		err := ig.refreshSessionTicket(context.Background())
		require.Error(t, err)
	})

	t.Run("renews from the current ticket without the credentials file", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPost || request.URL.Path != sessionTestTicketPath {
				http.NotFound(writer, request)

				return
			}

			body := map[string]string{}

			err := json.NewDecoder(request.Body).Decode(&body)
			if err != nil {
				http.Error(writer, "bad request", http.StatusBadRequest)

				return
			}

			if body["password"] != "current-ticket" {
				http.Error(writer, "authentication failed", http.StatusUnauthorized)

				return
			}

			fmt.Fprint(writer, `{"data":{"ticket":"renewed-ticket","userid":"test@pve","TTL":300}}`)
		}))
		t.Cleanup(server.Close)

		// The file does not exist: a renewal that needed it would fail. This is the path
		// that keeps a two-factor-protected session alive -- the login's one-time password
		// is never re-sent.
		ig := &InstanceGroup{
			CredentialsFilePath: path.Join(t.TempDir(), "does-not-exist.json"),
			log:                 hclog.NewNullLogger(),
			proxmox:             proxmox.NewClient(server.URL, proxmox.WithSession("current-ticket", "")),
		}

		err := ig.refreshSessionTicket(context.Background())
		require.NoError(t, err)
		require.Equal(t, "renewed-ticket", ig.proxmox.Session().Ticket)
	})

	t.Run("falls back to the credentials when the ticket is refused", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPost || request.URL.Path != sessionTestTicketPath {
				http.NotFound(writer, request)

				return
			}

			body := map[string]string{}

			err := json.NewDecoder(request.Body).Decode(&body)
			if err != nil {
				http.Error(writer, "bad request", http.StatusBadRequest)

				return
			}

			if body["password"] != "secret" {
				http.Error(writer, "authentication failed", http.StatusUnauthorized)

				return
			}

			fmt.Fprint(writer, `{"data":{"ticket":"restored-ticket","userid":"test@pve","TTL":300}}`)
		}))
		t.Cleanup(server.Close)

		ig := newRefreshTestGroup(t, server.URL)
		ig.proxmox = proxmox.NewClient(server.URL, proxmox.WithSession("stale-ticket", ""))

		err := ig.refreshSessionTicket(context.Background())
		require.NoError(t, err)
		require.Equal(t, "restored-ticket", ig.proxmox.Session().Ticket)
	})
}

// A one-time-password credential cannot re-authenticate: the code was consumed by the login
// that established the session. A renewal that falls through to it must report the
// credential so the refresher keeps the normal interval instead of hot-looping a failure
// that can never succeed, and log what to do instead. A plain-password failure still
// retries soon.
func TestRefreshSessionTicketOneTimePassword(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "authentication failed", http.StatusUnauthorized)
	}))
	t.Cleanup(server.Close)

	newOtpGroup := func(t *testing.T, log hclog.Logger) *InstanceGroup {
		t.Helper()

		credentialsPath := path.Join(t.TempDir(), "credentials.json")

		require.NoError(t, os.WriteFile(credentialsPath,
			[]byte(`{"realm":"pve","username":"test","password":"secret","otp":"123456"}`), 0o600))

		return &InstanceGroup{
			CredentialsFilePath: credentialsPath,
			log:                 log,
			proxmox:             proxmox.NewClient(server.URL, proxmox.WithSession("stale-ticket", "")),
		}
	}

	t.Run("one-time password keeps the normal interval", func(t *testing.T) {
		t.Parallel()

		log, logBuf := newLogBuffer(t)
		ig := newOtpGroup(t, log)

		err := ig.refreshSessionTicket(context.Background())
		require.Error(t, err)
		require.ErrorIs(t, err, ErrOneTimePasswordConsumed, "the fallback must report a spent one-time password")

		require.False(t, ig.refreshSessionTicketOnce(), "a consumed one-time password must not hot-retry")
		require.Contains(t, logBuf.String(), "one-time password")
		require.Contains(t, logBuf.String(), "API token")
	})

	t.Run("plain password still retries soon", func(t *testing.T) {
		t.Parallel()

		ig := newRefreshTestGroup(t, server.URL)
		ig.proxmox = proxmox.NewClient(server.URL, proxmox.WithSession("stale-ticket", ""))

		require.True(t, ig.refreshSessionTicketOnce())
	})
}

// newRefreshTestGroup is the minimum InstanceGroup refreshSessionTicket needs: a credentials
// file and a client pointed at the given URL.
func newRefreshTestGroup(t *testing.T, apiURL string) *InstanceGroup {
	t.Helper()

	credentialsPath := path.Join(t.TempDir(), "credentials.json")

	require.NoError(t, os.WriteFile(credentialsPath, []byte(`{"realm":"pve","username":"test","password":"secret"}`), 0o600))

	return &InstanceGroup{
		CredentialsFilePath:                   credentialsPath,
		log:                                   hclog.NewNullLogger(),
		proxmox:                               proxmox.NewClient(apiURL),
		sessionTicketRefresherShutdownTrigger: make(chan struct{}),
	}
}
