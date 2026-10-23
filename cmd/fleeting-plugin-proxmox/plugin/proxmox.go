package plugin

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/luthermonson/go-proxmox"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
)

// proxmoxRequestTimeout caps a single Proxmox API call. Every long operation
// (clone, stop, delete, resize) is asynchronous: the call returns a task
// immediately and completion is polled, so no legitimate request outlives this
// bound. It exists so a wedged Proxmox (connection accepted, no answer) fails
// the call after a bounded time instead of blocking the caller -- and, with it,
// instanceCloningMu and the collector queued behind it -- until the caller's
// context expires.
const proxmoxRequestTimeout = 30 * time.Second

const (
	// maxIdleConnsPerHost sizes the HTTP connection pool for the busiest moment: a batch
	// runs up to maxParallelInstanceOperations operations at once, each holding a request,
	// alongside the per-instance calls (connect info, heartbeat) the runners make. The
	// default pool of two idle connections per host would close every connection beyond
	// the first two and pay a TLS handshake for the rest.
	maxIdleConnsPerHost = 64

	// poolCacheTTL bounds how long the pool listing the per-instance lookups share stays
	// trusted. Those lookups only need a member's node and listed name, and the per-
	// instance name check re-verifies against the VM itself, so a listing a couple of
	// seconds old is safe evidence; a fresh listing per call would multiply the pool
	// endpoint by every connected instance. The removal and collection paths keep listing
	// fresh: a rename must be visible the moment it happens.
	poolCacheTTL = 2 * time.Second
)

var (
	ErrNotFound = errors.New("not found")
	// ErrNotOwned reports a VM that is in the pool but does not carry one of this
	// config's instance names, or, to getListedVM, no longer carries the one it was
	// listed and selected under.
	ErrNotOwned = errors.New("not owned by this instance group")

	// ErrTokenAuthMissingTokenID, ErrTokenAuthMissingTokenSecret and
	// ErrTokenAuthMissingUsername report an incomplete API-token credentials file:
	// token auth was requested but one of its required fields is absent.
	ErrTokenAuthMissingTokenID     = errors.New("token auth requires 'token_id'")
	ErrTokenAuthMissingTokenSecret = errors.New("token auth requires 'token_secret'")
	ErrTokenAuthMissingUsername    = errors.New("token auth requires 'username'")
)

func notOwned(vmid uint64, name string) error {
	return fmt.Errorf("vmid='%d' is named %q: %w", vmid, name, ErrNotOwned)
}

func (ig *InstanceGroup) getProxmoxPool(ctx context.Context) (*proxmox.Pool, error) {
	pool, err := ig.proxmox.Pool(ctx, ig.Pool)
	if err != nil {
		return nil, fmt.Errorf("failed to get pool id='%s': %w", ig.Pool, err)
	}

	return pool, nil
}

func (ig *InstanceGroup) findPoolMember(ctx context.Context, vmid int) (proxmox.ClusterResource, error) {
	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		return proxmox.ClusterResource{}, err
	}

	return findPoolMemberIn(pool, vmid)
}

// findPoolMemberIn is the listing-free half of findPoolMember: the member of an already
// fetched pool that carries vmid, or ErrNotFound when none does.
func findPoolMemberIn(pool *proxmox.Pool, vmid int) (proxmox.ClusterResource, error) {
	for _, member := range pool.Members {
		if member.Type != vmTypeQEMU {
			continue
		}

		//nolint:gosec//G115 // vmid is a Proxmox VMID, which always fits in an int
		if member.VMID == uint64(vmid) {
			return member, nil
		}
	}

	return proxmox.ClusterResource{}, ErrNotFound
}

// Where possible, use getProxmoxVMOnNode instead as it makes less calls to API. It does not
// check ownership: Increase uses it for the template and deployInstance for the VM it has just
// cloned. fleeting's per-instance RPCs go through ownedInstance, and the removal paths through
// getListedVM once they have selected a VM by its listed name.
func (ig *InstanceGroup) getProxmoxVM(ctx context.Context, vmid int) (*proxmox.VirtualMachine, error) {
	member, err := ig.findPoolMember(ctx, vmid)
	if err != nil {
		return nil, err
	}

	return ig.getProxmoxVMOnNode(ctx, vmid, member.Node)
}

func (ig *InstanceGroup) getProxmoxVMOnNode(ctx context.Context, vmid int, nodeName string) (*proxmox.VirtualMachine, error) {
	node, err := ig.proxmox.Node(ctx, nodeName)
	if err != nil {
		return nil, fmt.Errorf("failed to get node='%s': %w", nodeName, err)
	}

	vm, err := node.VirtualMachine(ctx, vmid)
	if err != nil {
		return nil, fmt.Errorf("failed to get vm='%d' on node='%s': %w", vmid, nodeName, err)
	}

	return vm, nil
}

// fetchedName returns the name the VM carries: the config name (what a rename writes) when
// present, else the status name.
func fetchedName(vm *proxmox.VirtualMachine) string {
	if vm.VirtualMachineConfig != nil && vm.VirtualMachineConfig.Name != "" {
		return vm.VirtualMachineConfig.Name
	}

	return vm.Name
}

// stateForName maps one of this group's instance names to its state; the bool is false for any
// other name.
func (ig *InstanceGroup) stateForName(name string) (provider.State, bool) {
	switch name {
	case ig.InstanceNameCreating:
		return provider.StateCreating, true
	case ig.InstanceNameRunning:
		return provider.StateRunning, true
	case ig.InstanceNameRemoving:
		return provider.StateDeleting, true
	default:
		return "", false
	}
}

func (ig *InstanceGroup) isOwnName(name string) bool {
	_, ok := ig.stateForName(name)

	return ok
}

// nameFromNode fills in the name of a pool member the listing returned without one. Proxmox
// takes a member's listed name from its status cache, which holds no entry for a VM whose node
// has not reported for five minutes (pvestatd stalled, pmxcfs just restarted). An empty name
// says nothing about who owns the VM; the name on the VM itself does.
func (ig *InstanceGroup) nameFromNode(ctx context.Context, member *proxmox.ClusterResource) error {
	//nolint:gosec//G115 // member.VMID is a Proxmox VMID, which always fits in an int
	vm, err := ig.getProxmoxVMOnNode(ctx, int(member.VMID), member.Node)
	if err != nil {
		return err
	}

	member.Name = fetchedName(vm)

	return nil
}

// poolForOwnership returns the pool listing the per-instance lookups trust: a fresh one
// when the cache is absent or older than poolCacheTTL. The fetch runs outside the guard,
// so concurrent misses duplicate the listing rather than queueing behind it.
func (ig *InstanceGroup) poolForOwnership(ctx context.Context) (*proxmox.Pool, error) {
	ig.poolCacheMu.Lock()

	if ig.poolCache != nil && time.Since(ig.poolCacheAt) < poolCacheTTL {
		cached := ig.poolCache
		ig.poolCacheMu.Unlock()

		return cached, nil
	}

	ig.poolCacheMu.Unlock()

	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		return nil, err
	}

	ig.poolCacheMu.Lock()
	ig.poolCache = pool
	ig.poolCacheAt = time.Now()
	ig.poolCacheMu.Unlock()

	return pool, nil
}

// ownedInstance resolves a pool member to the VM it names, refusing a VM that does not
// carry one of this group's instance names. The pool listing comes from the short-lived
// ownership cache: a heartbeat or connect for one instance is one member of a fleet of
// per-instance calls, and each one would otherwise re-list the whole pool.
func (ig *InstanceGroup) ownedInstance(ctx context.Context, vmid int) (*proxmox.VirtualMachine, error) {
	pool, err := ig.poolForOwnership(ctx)
	if err != nil {
		return nil, err
	}

	member, err := findPoolMemberIn(pool, vmid)
	if err != nil {
		return nil, err
	}

	// An empty listed name is no evidence either way: Proxmox leaves it unset while it has no
	// fresh status for the VM. The fetched name below decides.
	if member.Name != "" && !ig.isOwnName(member.Name) {
		return nil, notOwned(member.VMID, member.Name)
	}

	vm, err := ig.getProxmoxVMOnNode(ctx, vmid, member.Node)
	if err != nil {
		return nil, err
	}

	if name := fetchedName(vm); !ig.isOwnName(name) {
		return nil, notOwned(member.VMID, name)
	}

	return vm, nil
}

// getListedVM fetches a pool member listed under one of this group's names on its node and
// refuses it unless the VM still carries the name it was listed under: the pool listing and
// the node fetch are not atomic, and another manager may have renamed the VM between them.
// The one other name let through is InstanceNameRemoving: only this group renames a VM to
// it, and the listing trails a rename by several seconds, so a VM fetched under it is one this
// group has already marked.
func (ig *InstanceGroup) getListedVM(ctx context.Context, member *proxmox.ClusterResource) (*proxmox.VirtualMachine, error) {
	if !ig.isOwnName(member.Name) {
		return nil, notOwned(member.VMID, member.Name)
	}

	//nolint:gosec//G115 // member.VMID is a Proxmox VMID, which always fits in an int
	vm, err := ig.getProxmoxVMOnNode(ctx, int(member.VMID), member.Node)
	if err != nil {
		return nil, err
	}

	if name := fetchedName(vm); name != member.Name && name != ig.InstanceNameRemoving {
		ig.log.Warn("refusing to act on VM: renamed since pool listing",
			"vmid", member.VMID, "listed", member.Name, "found", name)

		return nil, notOwned(member.VMID, name)
	}

	return vm, nil
}

// proxmoxCredentialsFile is the on-disk Proxmox credentials file. It carries either
// ticket-auth fields (username + password, with realm) or API-token fields (token_id +
// token_secret, with username + realm naming the token's user).
//
// API-token auth is the preferred method: a token is a static secret attached to every
// request, so the client holds no session and there is no ticket to refresh. That removes
// the concurrent session state a ticket session otherwise exposes. Ticket auth remains as
// a backwards-compatible fallback.
type proxmoxCredentialsFile struct {
	Realm       string `json:"realm,omitempty"`
	Username    string `json:"username,omitempty"`
	Password    string `json:"password,omitempty"`
	Otp         string `json:"otp,omitempty"`
	TokenID     string `json:"token_id,omitempty"`
	TokenSecret string `json:"token_secret,omitempty"`
}

// tokenAuth reports whether the file requests API-token authentication.
func (c *proxmoxCredentialsFile) tokenAuth() bool {
	return c.TokenID != "" || c.TokenSecret != ""
}

// tokenAuthID builds the PVE API-token identifier "<user>@<realm>!<token_id>" that
// go-proxmox's WithAPIToken expects as its first argument. A username that already carries
// a "@realm" is used as-is, so the realm is not appended twice.
func (c *proxmoxCredentialsFile) tokenAuthID() (string, error) {
	if c.TokenID == "" {
		return "", ErrTokenAuthMissingTokenID
	}

	if c.TokenSecret == "" {
		return "", ErrTokenAuthMissingTokenSecret
	}

	user := c.Username
	if user == "" {
		return "", ErrTokenAuthMissingUsername
	}

	if c.Realm != "" && !strings.Contains(user, "@") {
		user += "@" + c.Realm
	}

	return user + "!" + c.TokenID, nil
}

// credentials converts the file's ticket-auth fields to go-proxmox credentials.
func (c *proxmoxCredentialsFile) credentials() *proxmox.Credentials {
	return &proxmox.Credentials{
		Realm:    c.Realm,
		Username: c.Username,
		Password: c.Password,
		Otp:      c.Otp,
	}
}

func (ig *InstanceGroup) getProxmoxClient() (*proxmox.Client, error) {
	url, err := url.Parse(ig.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL='%s': %w", ig.URL, err)
	}

	credentials, err := ig.getProxmoxCredentials()
	if err != nil {
		return nil, err
	}

	httpClient := http.Client{
		Timeout: proxmoxRequestTimeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: maxIdleConnsPerHost,
			TLSClientConfig: &tls.Config{
				//nolint:gosec
				InsecureSkipVerify: ig.InsecureSkipTLSVerify,
			},
		},
	}

	// WithRetry wraps the client's transport (installed in finalizeOptions, after
	// WithHTTPClient, so the order here is irrelevant) to re-issue a request on network
	// errors and 502/503/504/429, up to three attempts with backoff. Without it, a single
	// transient API failure mid-operation would abort a long clone poll or a whole deploy;
	// the client's request timeout above still bounds the exchange including retries.
	options := []proxmox.Option{
		proxmox.WithHTTPClient(&httpClient),
		proxmox.WithRetry(),
	}

	if credentials.tokenAuth() {
		// API-token auth: a static secret on every request, no session to keep alive.
		tokenID, err := credentials.tokenAuthID()
		if err != nil {
			return nil, fmt.Errorf("invalid API-token credentials: %w", err)
		}

		options = append(options, proxmox.WithAPIToken(tokenID, credentials.TokenSecret))
		ig.usingTokenAuth = true
	} else {
		// Ticket auth: username + password exchanged for a session ticket that the
		// session ticket refresher keeps alive.
		options = append(options, proxmox.WithCredentials(credentials.credentials()))
		ig.usingTokenAuth = false
	}

	return proxmox.NewClient(url.JoinPath("/api2/json").String(), options...), nil
}

func (ig *InstanceGroup) getProxmoxCredentials() (*proxmoxCredentialsFile, error) {
	credentialsFile, err := os.Open(ig.CredentialsFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open credentials file from path='%s': %w", ig.CredentialsFilePath, err)
	}
	defer credentialsFile.Close()

	credentials := proxmoxCredentialsFile{}

	err = json.NewDecoder(credentialsFile).Decode(&credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to decode credentials file from path='%s': %w", ig.CredentialsFilePath, err)
	}

	return &credentials, nil
}
