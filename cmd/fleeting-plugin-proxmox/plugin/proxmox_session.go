package plugin

import (
	"context"
	"fmt"
	"time"
)

const (
	// defaultSessionTicketRefreshInterval is how often a healthy session ticket is renewed.
	defaultSessionTicketRefreshInterval = 1 * time.Hour

	// sessionTicketRefreshTimeout bounds one refresh attempt: a read of the credentials
	// file and one request to the API.
	sessionTicketRefreshTimeout = 5 * time.Second

	// defaultSessionTicketRefreshRetryInterval is how soon a failed session ticket refresh
	// is retried: the current ticket is at the end of its life when a renewal fails, so the
	// next attempt must not wait out the whole normal interval.
	defaultSessionTicketRefreshRetryInterval = 5 * time.Minute
)

func (ig *InstanceGroup) startSessionTicketRefresher() {
	ig.sessionTicketRefresherWaitGroup.Go(func() {
		ig.runSessionTicketRefresher()
	})
}

func (ig *InstanceGroup) runSessionTicketRefresher() {
	ig.runRecoveringLoop("session ticket refresher", ig.sessionTicketRefresherShutdownTrigger, func() {
		// A failed renewal leaves the current ticket to run out before the next one, so the
		// attempt after a failure retries soon; a success returns to the normal interval.
		retryNext := false

		for {
			interval := ig.refreshInterval()

			if retryNext {
				interval = ig.refreshRetryInterval()
			}

			select {
			case <-ig.sessionTicketRefresherShutdownTrigger:
				return
			case <-time.After(interval):
				retryNext = ig.refreshSessionTicketOnce()
			}
		}
	})
}

// refreshSessionTicketOnce attempts one session ticket renewal, logging its outcome. It
// reports a failure so the refresher can retry sooner than the normal interval.
func (ig *InstanceGroup) refreshSessionTicketOnce() bool {
	ctx, cancel := context.WithTimeout(context.Background(), sessionTicketRefreshTimeout)

	err := ig.refreshSessionTicket(ctx)

	cancel()

	if err != nil {
		ig.log.Error("failed to refresh proxmox session", "err", err)

		return true
	}

	ig.log.Info("refreshed proxmox session")

	return false
}

// refreshSessionTicket re-authenticates the Proxmox client: it re-reads the credentials file
// and exchanges the credentials for a fresh ticket.
func (ig *InstanceGroup) refreshSessionTicket(ctx context.Context) error {
	credentials, err := ig.getProxmoxCredentials()
	if err != nil {
		return fmt.Errorf("could not read credentials: %w", err)
	}

	// Ticket auth only: an API-token client holds no session, so the refresher never
	// runs for it. Re-read the file so a rotated password is picked up, then exchange
	// it for a fresh ticket.
	_, err = ig.proxmox.Ticket(ctx, credentials.credentials())
	if err != nil {
		return fmt.Errorf("failed to create proxmox ticket: %w", err)
	}

	return nil
}
