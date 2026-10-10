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

const (
	proxmoxTestNode            = "pve-node"
	proxmoxTestForeignRunning  = "other-running"
	proxmoxTestForeignCreating = "other-creating"
	proxmoxTestMemberTypeLXC   = "lxc"
	proxmoxTestTemplate        = "template"
	proxmoxTestPools           = "/pools"
	proxmoxTestStatusCurrent   = "/status/current"
)

func TestInstanceGroup_getProxmoxClient(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	ig := InstanceGroup{
		URL:                   "https://example.com/proxmox",
		InsecureSkipTLSVerify: false,
		CredentialsFilePath:   path.Join(tempDir, "prox_credentials.json"),
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
	t.Parallel()

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
			members:   []removalTestMember{{vmid: 100, name: DefaultInstanceNameCreating}},
			wantName:  DefaultInstanceNameCreating,
			wantFetch: true,
		},
		{
			name:      "own running",
			vmid:      100,
			members:   []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning}},
			wantName:  DefaultInstanceNameRunning,
			wantFetch: true,
		},
		{
			name:      "own removing",
			vmid:      100,
			members:   []removalTestMember{{vmid: 100, name: DefaultInstanceNameRemoving}},
			wantName:  DefaultInstanceNameRemoving,
			wantFetch: true,
		},
		{
			// Ownership, not state: listed under one of our names, fetched under another
			// (our own rename the listing has not caught up with) is still ours.
			name:      "own listed other own fetched",
			vmid:      100,
			members:   []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning, fetchedName: DefaultInstanceNameRemoving}},
			wantName:  DefaultInstanceNameRemoving,
			wantFetch: true,
		},
		{
			name:        "foreign listed",
			vmid:        100,
			members:     []removalTestMember{{vmid: 100, name: proxmoxTestForeignRunning}},
			wantErr:     ErrNotOwned,
			wantMsgPart: `vmid='100' is named "other-running"`,
		},
		{
			name:        "own listed foreign fetched",
			vmid:        100,
			members:     []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning, fetchedName: proxmoxTestForeignCreating}},
			wantErr:     ErrNotOwned,
			wantMsgPart: `vmid='100' is named "other-creating"`,
			wantFetch:   true,
		},
		{
			// No name in the listing (Proxmox has no fresh status for the VM) is no evidence
			// either way: the fetched name decides.
			name:      "unnamed listing own fetched",
			vmid:      100,
			members:   []removalTestMember{{vmid: 100, name: "", fetchedName: DefaultInstanceNameRunning}},
			wantName:  DefaultInstanceNameRunning,
			wantFetch: true,
		},
		{
			name:        "unnamed listing foreign fetched",
			vmid:        100,
			members:     []removalTestMember{{vmid: 100, name: "", fetchedName: proxmoxTestForeignCreating}},
			wantErr:     ErrNotOwned,
			wantMsgPart: `vmid='100' is named "other-creating"`,
			wantFetch:   true,
		},
		{
			name:        "absent from pool",
			vmid:        999,
			members:     []removalTestMember{{vmid: 100, name: DefaultInstanceNameCreating}},
			wantErr:     ErrNotFound,
			wantMsgPart: "not found",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			counts := &removalRequestCounts{}
			ig := newRemovalTestGroup(t, removalTestServer{
				members:  testCase.members,
				requests: counts,
			})

			//nolint:gosec//G115 // testCase.vmid is a Proxmox VMID, which always fits in an int
			vm, err := ig.ownedInstance(context.Background(), int(testCase.vmid))

			require.Equal(t, testCase.wantFetch, len(counts.requestsFor(testCase.vmid)) > 0, "requests: %v", counts.requestsFor(testCase.vmid))

			if testCase.wantErr != nil {
				require.ErrorIs(t, err, testCase.wantErr)
				require.Nil(t, vm)
				require.Contains(t, err.Error(), testCase.wantMsgPart)

				return
			}

			require.NoError(t, err)
			require.Equal(t, testCase.wantName, fetchedName(vm))
		})
	}
}

func TestStateForName(t *testing.T) {
	t.Parallel()

	ig := &InstanceGroup{
		InstanceNameCreating: DefaultInstanceNameCreating,
		InstanceNameRunning:  DefaultInstanceNameRunning,
		InstanceNameRemoving: DefaultInstanceNameRemoving,
	}

	for name, want := range map[string]provider.State{
		DefaultInstanceNameCreating: provider.StateCreating,
		DefaultInstanceNameRunning:  provider.StateRunning,
		DefaultInstanceNameRemoving: provider.StateDeleting,
	} {
		state, ok := ig.stateForName(name)
		require.True(t, ok, name)
		require.Equal(t, want, state, name)
	}

	for _, name := range []string{proxmoxTestForeignRunning, proxmoxTestTemplate, ""} {
		_, ok := ig.stateForName(name)
		require.False(t, ok, "%q must not be one of this group's names", name)
	}
}

// getListedVM must refuse a member listed under a foreign name before any request to its node,
// whatever its caller selected it by.
func TestGetListedVMRefusesForeignListing(t *testing.T) {
	t.Parallel()

	counts := &removalRequestCounts{}
	ig := newRemovalTestGroup(t, removalTestServer{
		members:  []removalTestMember{{vmid: 100, name: proxmoxTestForeignRunning}},
		requests: counts,
	})

	member := &proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: proxmoxTestForeignRunning, Node: proxmoxTestNode}

	vm, err := ig.getListedVM(context.Background(), member)
	require.ErrorIs(t, err, ErrNotOwned)
	require.Nil(t, vm)
	require.Empty(t, counts.requestsFor(100))
}

// TestGetProxmoxVMIgnoresName guards the regression: getProxmoxVM must resolve any pool member regardless
// of its name, because Increase uses it to fetch the template and deployInstance the VM it has just cloned.
func TestGetProxmoxVMIgnoresName(t *testing.T) {
	t.Parallel()

	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{{vmid: 200, name: proxmoxTestTemplate}},
	})

	vm, err := ig.getProxmoxVM(context.Background(), 200)
	require.NoError(t, err)
	require.NotNil(t, vm)
}

func TestInstanceGroup_getProxmoxCredentials(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	ig := InstanceGroup{
		CredentialsFilePath: path.Join(tempDir, "sample_credentials.json"),
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

func TestInstanceGroup_getProxmoxClientErrors(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	credentialsPath := path.Join(tempDir, "credentials.json")

	require.NoError(t, os.WriteFile(credentialsPath, []byte(`{"realm": "pve","username": "test","password": "secret"}`), 0o600))

	t.Run("unparseable URL", func(t *testing.T) {
		t.Parallel()

		ig := InstanceGroup{
			URL:                 "http://exa mple.com",
			CredentialsFilePath: credentialsPath,
		}

		_, err := ig.getProxmoxClient()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to parse URL")
	})

	t.Run("missing credentials file", func(t *testing.T) {
		t.Parallel()

		ig := InstanceGroup{
			URL:                 "https://example.com",
			CredentialsFilePath: path.Join(tempDir, "does-not-exist.json"),
		}

		_, err := ig.getProxmoxClient()
		require.ErrorIs(t, err, os.ErrNotExist)
		require.Contains(t, err.Error(), "failed to open credentials file")
	})
}

func TestGetProxmoxVMAbsentFromPool(t *testing.T) {
	t.Parallel()

	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning}},
	})

	vm, err := ig.getProxmoxVM(context.Background(), 999)
	require.ErrorIs(t, err, ErrNotFound)
	require.Nil(t, vm)
}

func TestGetProxmoxVMPoolFetchFailure(t *testing.T) {
	t.Parallel()

	ig := newRemovalTestGroup(t, removalTestServer{failPaths: []string{proxmoxTestPools}})

	vm, err := ig.getProxmoxVM(context.Background(), 100)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to get pool")
	require.Nil(t, vm)
}

// findPoolMember only looks at VM pool members: a container that happens to hold the vmid is
// not the VM.
func TestFindPoolMemberIgnoresNonQEMUMembers(t *testing.T) {
	t.Parallel()

	ig := newRemovalTestGroup(t, removalTestServer{
		members: []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning, memberType: proxmoxTestMemberTypeLXC}},
	})

	member, err := ig.findPoolMember(context.Background(), 100)
	require.ErrorIs(t, err, ErrNotFound)
	require.Zero(t, member.VMID)
}

func TestGetProxmoxVMOnNodeErrors(t *testing.T) {
	t.Parallel()

	t.Run("node fetch fails", func(t *testing.T) {
		t.Parallel()

		ig := newRemovalTestGroup(t, removalTestServer{failPaths: []string{"/nodes/pve-node/status"}})

		vm, err := ig.getProxmoxVMOnNode(context.Background(), 100, proxmoxTestNode)
		require.Error(t, err)
		require.Nil(t, vm)
		require.Contains(t, err.Error(), "failed to get node")
	})

	t.Run("vm fetch fails", func(t *testing.T) {
		t.Parallel()

		ig := newRemovalTestGroup(t, removalTestServer{failPaths: []string{proxmoxTestStatusCurrent}})

		vm, err := ig.getProxmoxVMOnNode(context.Background(), 100, proxmoxTestNode)
		require.Error(t, err)
		require.Nil(t, vm)
		require.Contains(t, err.Error(), "failed to get vm")
	})
}

// ownedInstance and getListedVM surface a failed fetch as an error rather than acting on a
// half-read VM.
func TestOwnedInstanceFetchFailure(t *testing.T) {
	t.Parallel()

	ig := newRemovalTestGroup(t, removalTestServer{
		members:   []removalTestMember{{vmid: 100, name: DefaultInstanceNameRunning}},
		failPaths: []string{proxmoxTestStatusCurrent},
	})

	vm, err := ig.ownedInstance(context.Background(), 100)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrNotOwned)
	require.Nil(t, vm)
}

func TestGetListedVMAccessFailure(t *testing.T) {
	t.Parallel()

	ig := newRemovalTestGroup(t, removalTestServer{failPaths: []string{proxmoxTestStatusCurrent}})

	member := &proxmox.ClusterResource{VMID: 100, Type: vmTypeQEMU, Name: DefaultInstanceNameRemoving, Node: proxmoxTestNode}

	vm, err := ig.getListedVM(context.Background(), member)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrNotOwned)
	require.Nil(t, vm)
}
