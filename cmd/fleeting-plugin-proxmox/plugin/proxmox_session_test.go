package plugin

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sync/atomic"
	"testing"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
)

// The refresher loop returns once the shutdown trigger fires, so Shutdown cannot hang on it.
func TestSessionTicketRefresherStopsOnShutdown(t *testing.T) {
	ig := &InstanceGroup{
		log:                                   hclog.NewNullLogger(),
		sessionTicketRefresherShutdownTrigger: make(chan struct{}),
	}

	ig.startSessionTicketRefresher()

	requireShutdownReturns(t, ig, "with the session ticket refresher running")
}

func TestRefreshSessionTicket(t *testing.T) {
	t.Run("exchanges the credentials for a ticket", func(t *testing.T) {
		var ticketRequested atomic.Bool

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && r.URL.Path == "/access/ticket" {
				ticketRequested.Store(true)

				fmt.Fprint(w, `{"data":{"ticket":"sample-ticket","userid":"test@pve","TTL":300}}`)

				return
			}

			http.NotFound(w, r)
		}))
		t.Cleanup(server.Close)

		ig := newRefreshTestGroup(t, server.URL)

		err := ig.refreshSessionTicket(context.Background())
		require.NoError(t, err)
		require.True(t, ticketRequested.Load(), "no ticket was requested")
	})

	t.Run("missing credentials file", func(t *testing.T) {
		ig := newRefreshTestGroup(t, "http://127.0.0.1:1")
		ig.CredentialsFilePath = path.Join(t.TempDir(), "does-not-exist.json")

		err := ig.refreshSessionTicket(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "could not read credentials")
	})

	t.Run("refused ticket", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "authentication failed", http.StatusUnauthorized)
		}))
		t.Cleanup(server.Close)

		ig := newRefreshTestGroup(t, server.URL)

		err := ig.refreshSessionTicket(context.Background())
		require.Error(t, err)
	})
}

// newRefreshTestGroup is the minimum InstanceGroup refreshSessionTicket needs: a credentials
// file and a client pointed at the given URL.
func newRefreshTestGroup(t *testing.T, apiURL string) *InstanceGroup {
	t.Helper()

	credentialsPath := path.Join(t.TempDir(), "credentials.json")

	require.NoError(t, os.WriteFile(credentialsPath, []byte(`{"realm":"pve","username":"test","password":"secret"}`), 0o600))

	return &InstanceGroup{
		Settings: Settings{CredentialsFilePath: credentialsPath},
		log:      hclog.NewNullLogger(),
		proxmox:  proxmox.NewClient(apiURL),
	}
}
