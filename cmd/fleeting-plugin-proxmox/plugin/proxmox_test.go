package plugin

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
)

func TestInstanceGroup_getProxmoxClient(t *testing.T) {
	tempDir := t.TempDir()
	ig := InstanceGroup{
		Settings: Settings{
			URL:                   "https://example.com/proxmox",
			InsecureSkipTLSVerify: false,
			CredentialsFilePath:   path.Join(tempDir, "prox_credentials.json"),
		},
	}

	err := os.WriteFile(
		ig.CredentialsFilePath,
		[]byte(`{"realm": "pve","username": "03Ewl6rENi","password": "-rx£N503o_8(%\"l+=*4,YD"}`),
		0o600,
	)
	require.NoError(t, err)

	_, err = ig.getProxmoxClient()
	require.NoError(t, err)
}

func TestOwnedInstance(t *testing.T) {
	const (
		foreignListed  = "other-running"
		foreignFetched = "other-creating"
	)

	tests := []struct {
		name        string
		vmid        uint64
		members     []removalTestMember
		wantName    string // fetched name of the VM returned
		wantErr     error
		wantMsgPart string
		wantFetch   bool // whether the VM itself was fetched
	}{
		{
			name:      "own creating",
			vmid:      100,
			members:   []removalTestMember{{vmid: 100, name: "fleeting-creating"}},
			wantName:  "fleeting-creating",
			wantFetch: true,
		},
		{
			name:      "own running",
			vmid:      100,
			members:   []removalTestMember{{vmid: 100, name: "fleeting-running"}},
			wantName:  "fleeting-running",
			wantFetch: true,
		},
		{
			name:      "own removing",
			vmid:      100,
			members:   []removalTestMember{{vmid: 100, name: "fleeting-removing"}},
			wantName:  "fleeting-removing",
			wantFetch: true,
		},
		{
			// Ownership, not state: listed under one of our names, fetched under another
			// (our own rename the listing has not caught up with) is still ours.
			name:      "own listed other own fetched",
			vmid:      100,
			members:   []removalTestMember{{vmid: 100, name: "fleeting-running", fetchedName: "fleeting-removing"}},
			wantName:  "fleeting-removing",
			wantFetch: true,
		},
		{
			name:        "foreign listed",
			vmid:        100,
			members:     []removalTestMember{{vmid: 100, name: foreignListed}},
			wantErr:     ErrNotOwned,
			wantMsgPart: `vmid='100' is named "other-running"`,
		},
		{
			name:        "own listed foreign fetched",
			vmid:        100,
			members:     []removalTestMember{{vmid: 100, name: "fleeting-running", fetchedName: foreignFetched}},
			wantErr:     ErrNotOwned,
			wantMsgPart: `vmid='100' is named "other-creating"`,
			wantFetch:   true,
		},
		{
			// No name in the listing (Proxmox has no fresh status for the VM) is no evidence
			// either way: the fetched name decides.
			name:      "unnamed listing own fetched",
			vmid:      100,
			members:   []removalTestMember{{vmid: 100, name: "", fetchedName: "fleeting-running"}},
			wantName:  "fleeting-running",
			wantFetch: true,
		},
		{
			name:        "unnamed listing foreign fetched",
			vmid:        100,
			members:     []removalTestMember{{vmid: 100, name: "", fetchedName: foreignFetched}},
			wantErr:     ErrNotOwned,
			wantMsgPart: `vmid='100' is named "other-creating"`,
			wantFetch:   true,
		},
		{
			name:        "absent from pool",
			vmid:        999,
			members:     []removalTestMember{{vmid: 100, name: "fleeting-creating"}},
			wantErr:     ErrNotFound,
			wantMsgPart: "not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			counts := &removalRequestCounts{}
			ig := newRemovalTestGroup(t, removalTestServer{
				members:  tt.members,
				requests: counts,
			})

			vm, err := ig.ownedInstance(context.Background(), int(tt.vmid))

			require.Equal(t, tt.wantFetch, len(counts.requestsFor(tt.vmid)) > 0, "requests: %v", counts.requestsFor(tt.vmid))

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, vm)
				require.Contains(t, err.Error(), tt.wantMsgPart)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantName, fetchedName(vm))
		})
	}
}

func TestStateForName(t *testing.T) {
	ig := &InstanceGroup{Settings: Settings{
		InstanceNameCreating: "fleeting-creating",
		InstanceNameRunning:  "fleeting-running",
		InstanceNameRemoving: "fleeting-removing",
	}}

	for name, want := range map[string]provider.State{
		"fleeting-creating": provider.StateCreating,
		"fleeting-running":  provider.StateRunning,
		"fleeting-removing": provider.StateDeleting,
	} {
		state, ok := ig.stateForName(name)
		require.True(t, ok, name)
		require.Equal(t, want, state, name)
	}

	for _, name := range []string{"other-running", "template", ""} {
		_, ok := ig.stateForName(name)
		require.False(t, ok, "%q must not be one of this group's names", name)
	}
}

// getListedVM must refuse a member listed under a foreign name before any request to its node,
// whatever its caller selected it by.
func TestGetListedVMRefusesForeignListing(t *testing.T) {
	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		members:  []removalTestMember{{vmid: 100, name: "other-running"}},
		requests: counts,
	})

	member := &proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: "other-running", Node: "pve-node"}

	vm, err := ig.getListedVM(context.Background(), member)
	require.ErrorIs(t, err, ErrNotOwned)
	require.Nil(t, vm)
	require.Empty(t, counts.requestsFor(100))
}

// TestGetProxmoxVMIgnoresName guards the regression: getProxmoxVM must resolve any pool member regardless
// of its name, because Increase uses it to fetch the template and deployInstance the VM it has just cloned.
func TestGetProxmoxVMIgnoresName(t *testing.T) {
	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{{vmid: 200, name: "template"}},
	})

	vm, err := ig.getProxmoxVM(context.Background(), 200)
	require.NoError(t, err)
	require.NotNil(t, vm)
}

func TestInstanceGroup_getProxmoxCredentials(t *testing.T) {
	tempDir := t.TempDir()
	ig := InstanceGroup{
		Settings: Settings{
			CredentialsFilePath: path.Join(tempDir, "sample_credentials.json"),
		},
	}

	// Missing credentials file
	_, err := ig.getProxmoxCredentials()
	require.ErrorIs(t, err, os.ErrNotExist)

	// Malformed credentials file
	err = os.WriteFile(
		ig.CredentialsFilePath,
		[]byte(`{"realm": 'pve',`),
		0o600,
	)
	require.NoError(t, err)

	_, err = ig.getProxmoxCredentials()
	require.Error(t, err)

	// Correct credentials file
	err = os.WriteFile(
		ig.CredentialsFilePath,
		[]byte(`{"realm": "pve","username": "oQcW8N246FODI6Qui","password": "88u3[kKLJ{gU7A£fhWq"}`),
		0o600,
	)
	require.NoError(t, err)

	credentials, err := ig.getProxmoxCredentials()
	require.NoError(t, err)
	require.Equal(t, "pve", credentials.Realm)
	require.Equal(t, "oQcW8N246FODI6Qui", credentials.Username)
	require.Equal(t, `88u3[kKLJ{gU7A£fhWq`, credentials.Password)
}
