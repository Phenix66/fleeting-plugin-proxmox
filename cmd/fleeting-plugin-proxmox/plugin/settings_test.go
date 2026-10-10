package plugin

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

//nolint:gochecknoglobals
var (
	sampleURL = "https://example.com"
	//nolint:gosec
	sampleCredentialsPath      = "/tmp/proxmox_credentials.json"
	samplePool                 = "sample_pool"
	sampleStorage              = "sample_storage"
	sampleTemplateID           = 20
	sampleMaxInstances         = 7
	sampleInstanceNameCreating = "proxmox-creating"
	sampleInstanceNameRunning  = "running-prox"
	sampleInstanceNameRemoving = "proxve-removing"
	sampleAutoresizeDisk       = "ide0"
	sampleAutoresizeSize       = "10G"
)

func TestSettings_fillWithDefaults(t *testing.T) {
	t.Parallel()

	settings := Settings{}
	settings.FillWithDefaults()

	// Don't use consts here, we want to ensure they are not changed
	require.False(t, settings.InsecureSkipTLSVerify)
	require.Equal(t, "fleeting-creating", settings.InstanceNameCreating)
	require.Equal(t, "fleeting-running", settings.InstanceNameRunning)
	require.Equal(t, "fleeting-removing", settings.InstanceNameRemoving)
	require.Equal(t, "ipv4", settings.InstanceNetworkProtocol)
	require.Equal(t, 10, *settings.ProxmoxTaskWaitInterval)

	settings2 := Settings{
		InstanceNameCreating: sampleInstanceNameCreating,
		InstanceNameRunning:  sampleInstanceNameRunning,
		InstanceNameRemoving: sampleInstanceNameRemoving,
	}
	settings2.FillWithDefaults()

	require.Equal(t, sampleInstanceNameCreating, settings2.InstanceNameCreating)
	require.Equal(t, sampleInstanceNameRunning, settings2.InstanceNameRunning)
	require.Equal(t, sampleInstanceNameRemoving, settings2.InstanceNameRemoving)
}

func TestSettings_checkRequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		settings      Settings
		expectedError error
	}{
		{
			name: "Missing URL",
			settings: Settings{
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				Storage:             sampleStorage,
				TemplateID:          &sampleTemplateID,
				MaxInstances:        &sampleMaxInstances,
			},
			expectedError: ErrRequiredSettingMissing,
		},
		{
			name: "Missing credentials file path",
			settings: Settings{
				URL:          sampleURL,
				Pool:         samplePool,
				Storage:      sampleStorage,
				TemplateID:   &sampleTemplateID,
				MaxInstances: &sampleMaxInstances,
			},
			expectedError: ErrRequiredSettingMissing,
		},
		{
			name: "Missing pool",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Storage:             sampleStorage,
				TemplateID:          &sampleTemplateID,
				MaxInstances:        &sampleMaxInstances,
			},
			expectedError: ErrRequiredSettingMissing,
		},
		{
			name: "Missing storage",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				TemplateID:          &sampleTemplateID,
				MaxInstances:        &sampleMaxInstances,
			},
			expectedError: nil,
		},
		{
			name: "Missing template id",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				Storage:             sampleStorage,
				TemplateID:          nil,
				MaxInstances:        &sampleMaxInstances,
			},
			expectedError: ErrRequiredSettingMissing,
		},
		{
			name: "Missing max instances",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				Storage:             sampleStorage,
				TemplateID:          nil,
				MaxInstances:        nil,
			},
			expectedError: ErrRequiredSettingMissing,
		},
		{
			// TemplateID is set, so the failure must come from max_instances itself.
			name: "Missing max instances with template id set",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				Storage:             sampleStorage,
				TemplateID:          &sampleTemplateID,
				MaxInstances:        nil,
			},
			expectedError: ErrRequiredSettingMissing,
		},
		{
			name: "No missing parameters",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				Storage:             sampleStorage,
				TemplateID:          &sampleTemplateID,
				MaxInstances:        &sampleMaxInstances,
			},
			expectedError: nil,
		},
		{
			name: "Invalid protocol",
			settings: Settings{
				URL:                     sampleURL,
				CredentialsFilePath:     sampleCredentialsPath,
				Pool:                    samplePool,
				Storage:                 sampleStorage,
				TemplateID:              &sampleTemplateID,
				MaxInstances:            &sampleMaxInstances,
				InstanceNetworkProtocol: "invalid-protocol",
			},
			expectedError: ErrSettingInvalidParameter,
		},
		{
			name: "Valid protocol ipv6",
			settings: Settings{
				URL:                     sampleURL,
				CredentialsFilePath:     sampleCredentialsPath,
				Pool:                    samplePool,
				Storage:                 sampleStorage,
				TemplateID:              &sampleTemplateID,
				MaxInstances:            &sampleMaxInstances,
				InstanceNetworkProtocol: NetworkProtocolIPv6,
			},
			expectedError: nil,
		},
		{
			name: "Valid protocol any",
			settings: Settings{
				URL:                     sampleURL,
				CredentialsFilePath:     sampleCredentialsPath,
				Pool:                    samplePool,
				Storage:                 sampleStorage,
				TemplateID:              &sampleTemplateID,
				MaxInstances:            &sampleMaxInstances,
				InstanceNetworkProtocol: NetworkProtocolAny,
			},
			expectedError: nil,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.settings.CheckRequiredFields()
			require.ErrorIs(t, err, testCase.expectedError)
		})
	}
}

func TestSettings_validateInstanceAutoresizeDisk(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		disk          string
		expectedError error
	}{
		{
			name:          "Empty disk is valid",
			disk:          "",
			expectedError: nil,
		},
		{
			name:          "Valid ide0",
			disk:          sampleAutoresizeDisk,
			expectedError: nil,
		},
		{
			name:          "Valid ide3",
			disk:          "ide3",
			expectedError: nil,
		},
		{
			name:          "Valid scsi0",
			disk:          "scsi0",
			expectedError: nil,
		},
		{
			name:          "Valid scsi29",
			disk:          "scsi29",
			expectedError: nil,
		},
		{
			name:          "Valid sata0",
			disk:          "sata0",
			expectedError: nil,
		},
		{
			name:          "Valid sata4",
			disk:          "sata4",
			expectedError: nil,
		},
		{
			name:          "Valid virtio0",
			disk:          "virtio0",
			expectedError: nil,
		},
		{
			name:          "Valid virtio15",
			disk:          "virtio15",
			expectedError: nil,
		},
		{
			name:          "Invalid ide4 - index exceeds max",
			disk:          "ide4",
			expectedError: ErrSettingInvalidParameter,
		},
		{
			name:          "Invalid scsi31 - index exceeds max",
			disk:          "scsi31",
			expectedError: ErrSettingInvalidParameter,
		},
		{
			name:          "Invalid sata6 - index exceeds max",
			disk:          "sata6",
			expectedError: ErrSettingInvalidParameter,
		},
		{
			name:          "Invalid virtio16 - index exceeds max",
			disk:          "virtio16",
			expectedError: ErrSettingInvalidParameter,
		},
		{
			name:          "Invalid disk format",
			disk:          "nvme0",
			expectedError: ErrSettingInvalidParameter,
		},
		{
			name:          "Invalid disk format no index",
			disk:          "ide",
			expectedError: ErrSettingInvalidParameter,
		},
		{
			// The index matches the pattern but overflows an int.
			name:          "Index overflows int",
			disk:          "ide" + strings.Repeat("9", 30),
			expectedError: strconv.ErrRange,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			settings := Settings{
				InstanceAutoresizeDisk: testCase.disk,
			}
			err := settings.validateInstanceAutoresizeDisk()
			require.ErrorIs(t, err, testCase.expectedError)
		})
	}
}

func TestSettings_validateInstanceAutoresizeSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		size          string
		expectedError error
	}{
		{
			name:          "Empty size is valid",
			size:          "",
			expectedError: nil,
		},
		{
			name:          "Valid absolute size no unit",
			size:          "10",
			expectedError: nil,
		},
		{
			name:          "Valid absolute size 10G",
			size:          sampleAutoresizeSize,
			expectedError: nil,
		},
		{
			name:          "Valid absolute size 512M",
			size:          "512M",
			expectedError: nil,
		},
		{
			name:          "Valid absolute size 1T",
			size:          "1T",
			expectedError: nil,
		},
		{
			name:          "Valid absolute size 1K",
			size:          "1K",
			expectedError: nil,
		},
		{
			name:          "Valid increment +10G",
			size:          "+10G",
			expectedError: nil,
		},
		{
			name:          "Valid increment +512M",
			size:          "+512M",
			expectedError: nil,
		},
		{
			name:          "Valid decimal size 1.5G",
			size:          "1.5G",
			expectedError: nil,
		},
		{
			name:          "Valid decimal increment +1.5G",
			size:          "+1.5G",
			expectedError: nil,
		},
		{
			name:          "Invalid size negative",
			size:          "-10G",
			expectedError: ErrSettingInvalidParameter,
		},
		{
			name:          "Invalid size invalid unit",
			size:          "10P",
			expectedError: ErrSettingInvalidParameter,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			settings := Settings{
				InstanceAutoresizeSize: testCase.size,
			}
			err := settings.validateInstanceAutoresizeSize()
			require.ErrorIs(t, err, testCase.expectedError)
		})
	}
}

func TestSettings_validateInstanceAutoresizeConsistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		disk          string
		size          string
		expectedError error
	}{
		{
			name:          "Both empty is valid",
			disk:          "",
			size:          "",
			expectedError: nil,
		},
		{
			name:          "Both set is valid",
			disk:          sampleAutoresizeDisk,
			size:          sampleAutoresizeSize,
			expectedError: nil,
		},
		{
			name:          "Disk without size is invalid",
			disk:          sampleAutoresizeDisk,
			size:          "",
			expectedError: ErrSettingInvalidParameter,
		},
		{
			name:          "Size without disk is invalid",
			disk:          "",
			size:          sampleAutoresizeSize,
			expectedError: ErrSettingInvalidParameter,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			settings := Settings{
				InstanceAutoresizeDisk: testCase.disk,
				InstanceAutoresizeSize: testCase.size,
			}
			err := settings.validateInstanceAutoresizeConsistency()
			require.ErrorIs(t, err, testCase.expectedError)
		})
	}
}

func TestGetDiskMaxIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		diskType string
		expected int
	}{
		{
			name:     "ide returns maxIDEIndex",
			diskType: "ide",
			expected: maxIDEIndex,
		},
		{
			name:     "scsi returns maxSCSIIndex",
			diskType: "scsi",
			expected: maxSCSIIndex,
		},
		{
			name:     "sata returns maxSATAIndex",
			diskType: "sata",
			expected: maxSATAIndex,
		},
		{
			name:     "unknown returns 0",
			diskType: "nvme",
			expected: 0,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, testCase.expected, getDiskMaxIndex(testCase.diskType))
		})
	}
}

func TestSettings_CheckRequiredFields_fullValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		settings      Settings
		expectedError error
	}{
		{
			name: "Valid autoresize disk without size passes disk validation but fails consistency",
			settings: Settings{
				URL:                    sampleURL,
				CredentialsFilePath:    sampleCredentialsPath,
				Pool:                   samplePool,
				TemplateID:             &sampleTemplateID,
				MaxInstances:           &sampleMaxInstances,
				InstanceAutoresizeDisk: sampleAutoresizeDisk,
			},
			expectedError: ErrSettingInvalidParameter,
		},
		{
			name: "Valid autoresize with both disk and size passes",
			settings: Settings{
				URL:                    sampleURL,
				CredentialsFilePath:    sampleCredentialsPath,
				Pool:                   samplePool,
				TemplateID:             &sampleTemplateID,
				MaxInstances:           &sampleMaxInstances,
				InstanceAutoresizeDisk: sampleAutoresizeDisk,
				InstanceAutoresizeSize: sampleAutoresizeSize,
			},
			expectedError: nil,
		},
		{
			name: "Invalid autoresize disk format",
			settings: Settings{
				URL:                    sampleURL,
				CredentialsFilePath:    sampleCredentialsPath,
				Pool:                   samplePool,
				TemplateID:             &sampleTemplateID,
				MaxInstances:           &sampleMaxInstances,
				InstanceAutoresizeDisk: "nvme0",
				InstanceAutoresizeSize: sampleAutoresizeSize,
			},
			expectedError: ErrSettingInvalidParameter,
		},
		{
			name: "Invalid autoresize size format",
			settings: Settings{
				URL:                    sampleURL,
				CredentialsFilePath:    sampleCredentialsPath,
				Pool:                   samplePool,
				TemplateID:             &sampleTemplateID,
				MaxInstances:           &sampleMaxInstances,
				InstanceAutoresizeDisk: sampleAutoresizeDisk,
				InstanceAutoresizeSize: "invalid",
			},
			expectedError: ErrSettingInvalidParameter,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.settings.CheckRequiredFields()
			require.ErrorIs(t, err, testCase.expectedError)
		})
	}
}
