package plugin

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	// defaultSessionTicketRefreshInterval is how often a healthy session ticket is renewed.
	defaultSessionTicketRefreshInterval = 1 * time.Hour

	// sessionTicketRefreshTimeout bounds one refresh attempt: a ticket renewal, and, when
	// that fails, a read of the credentials file and one more request to the API.
	sessionTicketRefreshTimeout = 5 * time.Second

	// defaultSessionTicketRefreshRetryInterval is how soon a failed session ticket refresh
	// is retried: the current ticket is at the end of its life when a renewal fails, so the
	// next attempt must not wait out the whole normal interval.
	defaultSessionTicketRefreshRetryInterval = 5 * time.Minute
)

// ErrOneTimePasswordConsumed wraps a session renewal failure whose fallback re-read a
// credentials file carrying a one-time password: the code was consumed by the login that
// established the session, so the fallback can never succeed and the refresher must not
// hot-retry it.
var ErrOneTimePasswordConsumed = errors.New("the credentials file's one-time password cannot re-authenticate the session")

func (ig *InstanceGroup) startSessionTicketRefresher() {
	ig.sessionTicketRefresherWaitGroup.Go(func() {
		ig.runSessionTicketRefresher()
	})
}

func (ig *InstanceGroup) runSessionTicketRefresher() {
	ig.runRecoveringLoop("session ticket refresher", ig.sessionTicketRefresherShutdownTrigger, func() {
		// A failed renewal leaves the current ticket to run out before the next one, so the
		// attempt after a failure retries soon; a success returns to the normal interval. A
		// one-time-password credential that cannot re-authenticate keeps the normal
		// interval instead: retrying it every few minutes can never succeed.
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
// reports a failure that must be retried soon; a one-time-password credential that cannot
// re-authenticate reports none, keeping the normal interval.
func (ig *InstanceGroup) refreshSessionTicketOnce() bool {
	ctx, cancel := context.WithTimeout(context.Background(), sessionTicketRefreshTimeout)

	err := ig.refreshSessionTicket(ctx)

	cancel()

	if err == nil {
		ig.log.Info("refreshed proxmox session")

		return false
	}

	if errors.Is(err, ErrOneTimePasswordConsumed) {
		// The one-time password the file carries was consumed by the login that established
		// the session, so the fallback can never succeed: say what to do instead, and keep
		// the normal interval rather than hot-looping a dead credential.
		ig.log.Error("failed to refresh proxmox session; the credentials file's one-time password was consumed by the login and cannot re-authenticate it -- "+
			"use an API token (token_id and token_secret in the credentials file) or restart the plugin to log in again with a fresh one-time password",
			"err", err)

		return false
	}

	ig.log.Error("failed to refresh proxmox session", "err", err)

	return true
}

// refreshSessionTicket renews the client's session ticket. It prefers the vendor's
// RefreshTicket, which re-POSTs the current ticket as the password: Proxmox accepts the
// current ticket as the password of POST /access/ticket for any account, and it is the only
// renewal a two-factor-protected account can ever do, whose one-time password the login that
// established the session already consumed. When the ticket itself can no longer be renewed
// -- it has outlived Proxmox's ticket timeout -- the credentials file is re-read and
// exchanged for a fresh ticket the way Init did: that restores a plain-password session and
// picks up a rotated password. A fallback onto a one-time-password credential wraps the
// failure with ErrOneTimePasswordConsumed: that code is spent, so the failure cannot
// succeed no matter how often it is retried.
func (ig *InstanceGroup) refreshSessionTicket(ctx context.Context) error {
	err := ig.proxmox.RefreshTicket(ctx)
	if err == nil {
		return nil
	}

	credentials, fileErr := ig.getProxmoxCredentials()
	if fileErr != nil {
		return fmt.Errorf("could not read credentials: %w", fileErr)
	}

	_, err = ig.proxmox.Ticket(ctx, credentials.credentials())
	if err != nil {
		if credentials.Otp != "" {
			return fmt.Errorf("%w: %w", ErrOneTimePasswordConsumed, err)
		}

		return fmt.Errorf("failed to create proxmox ticket: %w", err)
	}

	return nil
}
