package plugin

import (
	"context"
	"fmt"
	"time"
)

const (
	sessionTicketRefreshInterval = 1 * time.Hour
	sessionTicketRefreshTimeout  = 5 * time.Second
)

func (ig *InstanceGroup) startSessionTicketRefresher() {
	ig.sessionTicketRefresherWaitGroup.Go(func() {
		ig.runSessionTicketRefresher()
	})
}

func (ig *InstanceGroup) runSessionTicketRefresher() {
	for {
		select {
		case <-ig.sessionTicketRefresherShutdownTrigger:
			return
		case <-time.After(sessionTicketRefreshInterval):
			ctx, cancel := context.WithTimeout(context.Background(), sessionTicketRefreshTimeout)

			err := ig.refreshSessionTicket(ctx)

			cancel()

			if err != nil {
				ig.log.Error("failed to refresh proxmox session", "err", err)
			} else {
				ig.log.Info("refreshed proxmox session")
			}
		}
	}
}

// refreshSessionTicket re-authenticates the Proxmox client: it re-reads the credentials file
// and exchanges the credentials for a fresh ticket.
func (ig *InstanceGroup) refreshSessionTicket(ctx context.Context) error {
	credentials, err := ig.getProxmoxCredentials()
	if err != nil {
		return fmt.Errorf("could not read credentials: %w", err)
	}

	_, err = ig.proxmox.Ticket(ctx, credentials)
	if err != nil {
		return fmt.Errorf("failed to create proxmox ticket: %w", err)
	}

	return nil
}
