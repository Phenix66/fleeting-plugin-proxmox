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
	"time"

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
	proxmoxTestTokenID         = "mytoken"
	proxmoxTestRealm           = "pam"
	proxmoxTestUsername        = "root"
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

func TestProxmoxCredentialsFile_tokenAuthID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    proxmoxCredentialsFile
		wantID  string
		wantErr bool
	}{
		{
			name:   "plain username and realm",
			file:   proxmoxCredentialsFile{Realm: proxmoxTestRealm, Username: proxmoxTestUsername, TokenID: proxmoxTestTokenID, TokenSecret: "s"},
			wantID: "root@pam!" + proxmoxTestTokenID,
		},
		{
			name:   "username already carries realm",
			file:   proxmoxCredentialsFile{Realm: proxmoxTestRealm, Username: "root@pam", TokenID: proxmoxTestTokenID, TokenSecret: "s"},
			wantID: "root@pam!" + proxmoxTestTokenID,
		},
		{
			name:   "empty realm",
			file:   proxmoxCredentialsFile{Username: "root@pve", TokenID: "ci", TokenSecret: "s"},
			wantID: "root@pve!ci",
		},
		{
			name:    "missing token id",
			file:    proxmoxCredentialsFile{Realm: proxmoxTestRealm, Username: proxmoxTestUsername, TokenSecret: "s"},
			wantErr: true,
		},
		{
			name:    "missing secret",
			file:    proxmoxCredentialsFile{Realm: proxmoxTestRealm, Username: proxmoxTestUsername, TokenID: proxmoxTestTokenID},
			wantErr: true,
		},
		{
			name:    "missing username",
			file:    proxmoxCredentialsFile{Realm: proxmoxTestRealm, TokenID: proxmoxTestTokenID, TokenSecret: "s"},
			wantErr: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			tokenID, err := testCase.file.tokenAuthID()
			if testCase.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, testCase.wantID, tokenID)
		})
	}
}

func TestProxmoxCredentialsFile_tokenAuth(t *testing.T) {
	t.Parallel()

	require.False(t, (&proxmoxCredentialsFile{Realm: proxmoxTestRealm, Username: proxmoxTestUsername, Password: "s"}).tokenAuth())
	require.True(t, (&proxmoxCredentialsFile{Realm: proxmoxTestRealm, Username: proxmoxTestUsername, TokenID: proxmoxTestTokenID, TokenSecret: "s"}).tokenAuth())
}

// A token-authenticated client must send the Authorization header and must not
// open a ticket session; a ticket-authenticated client must not send it.
func TestInstanceGroup_getProxmoxClient_authMode(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name           string
		credentials    string
		wantTokenAuth  bool
		wantAuthHeader string
	}{
		//nolint:gosec//G101 // test fixture, not a real secret
		{
			name:           "token auth sends PVEAPIToken header",
			credentials:    `{"realm": "pam","username": "root","token_id": "mytoken","token_secret": "s3cret"}`,
			wantTokenAuth:  true,
			wantAuthHeader: "PVEAPIToken=root@pam!mytoken=s3cret",
		},
		//nolint:gosec//G101 // test fixture, not a real secret
		{
			name:           "ticket auth sends no token header",
			credentials:    `{"realm": "pam","username": "root","password": "s3cret"}`,
			wantTokenAuth:  false,
			wantAuthHeader: "",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			authHeader := make(chan string, 1)

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				authHeader <- request.Header.Get("Authorization")

				fmt.Fprint(w, `{"data":{"version":"8.0","repoid":"test","supportrepo":"test","versiondate":"0"}}`)
			}))
			t.Cleanup(server.Close)

			tempDir := t.TempDir()
			ig := InstanceGroup{
				URL:                 server.URL,
				CredentialsFilePath: path.Join(tempDir, "prox_credentials.json"),
			}

			require.NoError(t, os.WriteFile(ig.CredentialsFilePath, []byte(testCase.credentials), 0o600))

			client, err := ig.getProxmoxClient()
			require.NoError(t, err)
			require.Equal(t, testCase.wantTokenAuth, ig.usingTokenAuth)

			_, _ = client.Version(context.Background())

			select {
			case got := <-authHeader:
				require.Equal(t, testCase.wantAuthHeader, got)
			case <-time.After(2 * time.Second):
				t.Fatal("no request reached the server")
			}
		})
	}
}

// A transient API failure (502/503) must be retried by the transport instead of surfaced:
// a single blip mid-operation must not abort a long clone poll or a whole deploy. A
// definitive failure (500) is not retried and must surface to the caller.
func TestInstanceGroup_getProxmoxClient_retriesTransientErrors(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name         string
		statuses     []int // status served per request, the last one repeated
		wantRequests int64
		wantErr      bool
	}{
		{
			name:         "503 is retried until clear",
			statuses:     []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusOK},
			wantRequests: 3,
		},
		{
			name:         "502 is retried until clear",
			statuses:     []int{http.StatusBadGateway, http.StatusOK},
			wantRequests: 2,
		},
		{
			name:         "500 is not retried",
			statuses:     []int{http.StatusInternalServerError, http.StatusOK},
			wantRequests: 1,
			wantErr:      true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			var requests atomic.Int64

			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				index := int(requests.Add(1)) - 1

				if index >= len(testCase.statuses) {
					index = len(testCase.statuses) - 1
				}

				if testCase.statuses[index] != http.StatusOK {
					writer.WriteHeader(testCase.statuses[index])

					return
				}

				fmt.Fprint(writer, `{"data":{"version":"8.0","repoid":"test","supportrepo":"test","versiondate":"0"}}`)
			}))
			t.Cleanup(server.Close)

			tempDir := t.TempDir()
			ig := InstanceGroup{
				URL:                 server.URL,
				CredentialsFilePath: path.Join(tempDir, "prox_credentials.json"),
			}

			require.NoError(t, os.WriteFile(
				ig.CredentialsFilePath,
				[]byte(`{"realm": "pam","username": "root","token_id": "mytoken","token_secret": "s3cret"}`),
				0o600,
			))

			client, err := ig.getProxmoxClient()
			require.NoError(t, err)

			_, err = client.Version(context.Background())

			if testCase.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, testCase.wantRequests, requests.Load(), "the retry behavior is not as expected")
		})
	}
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
