package plugin

import (
	"testing"

	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
)

const (
	networkTestInterface        = "ens18"
	networkTestInterfaceAlt     = "eth0"
	networkTestInterfaceAltOne  = "eth1"
	networkTestHardwareAddress  = "12:34:56:AB:CD:EF"
	networkTestHardwareAddress1 = "12:34:56:AB:CD:E1"
	networkTestIPv4Public       = "8.8.8.8"
	networkTestIPv4Private      = "192.168.0.1"
	networkTestIPv6Public       = "2001:4860:4860::8888"
	networkTestIPv6Private      = "fd3b:47fc:de09::1"
)

// networkTestIP builds an agent IP address entry of the given protocol.
func networkTestIP(protocol NetworkProtocol, address string) *proxmox.AgentNetworkIPAddress {
	return &proxmox.AgentNetworkIPAddress{IPAddressType: protocol, IPAddress: address}
}

// networkTestIface builds an agent network interface with the given name, hardware
// address and IP entries.
func networkTestIface(name, hardwareAddress string, addresses ...*proxmox.AgentNetworkIPAddress) *proxmox.AgentNetworkIface {
	return &proxmox.AgentNetworkIface{Name: name, HardwareAddress: hardwareAddress, IPAddresses: addresses}
}

// networkTestStandardAddresses is the address set most rows attach to an interface: a
// public and a private address per family.
func networkTestStandardAddresses() []*proxmox.AgentNetworkIPAddress {
	return []*proxmox.AgentNetworkIPAddress{
		networkTestIP(NetworkProtocolIPv4, networkTestIPv4Public),
		networkTestIP(NetworkProtocolIPv4, networkTestIPv4Private),
		networkTestIP(NetworkProtocolIPv6, networkTestIPv6Public),
		networkTestIP(NetworkProtocolIPv6, networkTestIPv6Private),
	}
}

// networkTestAltAddresses is the second interface's address set in the multiple-interface
// rows: the same shape as the standard set, with different values.
func networkTestAltAddresses() []*proxmox.AgentNetworkIPAddress {
	return []*proxmox.AgentNetworkIPAddress{
		networkTestIP(NetworkProtocolIPv4, "8.8.4.4"),
		networkTestIP(NetworkProtocolIPv4, "192.168.0.2"),
		networkTestIP(NetworkProtocolIPv6, "2001:4860:4860::8844"),
		networkTestIP(NetworkProtocolIPv6, "fd3b:47fc:de09::2"),
	}
}

// networkTestRow is one case of determineAddresses: what the caller requests, what the
// agent reports, and the outcome the test expects.
type networkTestRow struct {
	name               string
	requestedInterface string
	requestedProtocol  NetworkProtocol
	networkInterfaces  []*proxmox.AgentNetworkIface
	expectedError      error
	expectedInternal   string
	expectedExternal   string
}

func newNetworkTestRow(name, requestedInterface string, requestedProtocol NetworkProtocol, expectedError error,
	expectedInternal, expectedExternal string, networkInterfaces ...*proxmox.AgentNetworkIface,
) networkTestRow {
	return networkTestRow{
		name:               name,
		requestedInterface: requestedInterface,
		requestedProtocol:  requestedProtocol,
		networkInterfaces:  networkInterfaces,
		expectedError:      expectedError,
		expectedInternal:   expectedInternal,
		expectedExternal:   expectedExternal,
	}
}

func Test_determineAddresses(t *testing.T) {
	t.Parallel()

	tests := []networkTestRow{
		newNetworkTestRow("No network interfaces", networkTestInterface, NetworkProtocolAny, ErrNoIPAddress, "", ""),
		newNetworkTestRow("Any", networkTestInterface, NetworkProtocolAny, nil, networkTestIPv6Private, networkTestIPv6Public,
			networkTestIface(networkTestInterface, networkTestHardwareAddress, networkTestStandardAddresses()...)),
		newNetworkTestRow("Single interface using default behavior", "", NetworkProtocolAny, nil, networkTestIPv6Private, networkTestIPv6Public,
			networkTestIface(networkTestInterfaceAlt, networkTestHardwareAddress, networkTestStandardAddresses()...)),
		newNetworkTestRow("Multiple interfaces using default behavior", "", NetworkProtocolAny, ErrTooManyPotentials, "", "",
			networkTestIface(networkTestInterfaceAlt, networkTestHardwareAddress, networkTestStandardAddresses()...),
			networkTestIface(networkTestInterfaceAltOne, networkTestHardwareAddress1, networkTestAltAddresses()...)),
		newNetworkTestRow("Multiple interfaces with loopback using default behavior", "", NetworkProtocolAny, nil, networkTestIPv6Private, networkTestIPv6Public,
			networkTestIface("Ethernet", networkTestHardwareAddress, networkTestStandardAddresses()...),
			networkTestIface("Loopback Pseudo-Interface 1", "",
				networkTestIP(NetworkProtocolIPv4, "127.0.0.1"),
				networkTestIP(NetworkProtocolIPv6, "::1"))),
		newNetworkTestRow("Forced IPv4", networkTestInterface, NetworkProtocolIPv4, nil, networkTestIPv4Private, networkTestIPv4Public,
			networkTestIface(networkTestInterface, networkTestHardwareAddress, networkTestStandardAddresses()...)),
		newNetworkTestRow("Forced IPv6", networkTestInterface, NetworkProtocolIPv6, nil, networkTestIPv6Private, networkTestIPv6Public,
			networkTestIface(networkTestInterface, networkTestHardwareAddress, networkTestStandardAddresses()...)),
		newNetworkTestRow("Any with only internal address", networkTestInterface, NetworkProtocolAny, nil, networkTestIPv6Private, networkTestIPv6Private,
			networkTestIface(networkTestInterface, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv4, networkTestIPv4Private),
				networkTestIP(NetworkProtocolIPv6, networkTestIPv6Private))),
		newNetworkTestRow("Forced IPv4 with only internal address", networkTestInterface, NetworkProtocolIPv4, nil, networkTestIPv4Private, networkTestIPv4Private,
			networkTestIface(networkTestInterface, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv4, networkTestIPv4Private),
				networkTestIP(NetworkProtocolIPv6, networkTestIPv6Private))),
		newNetworkTestRow("Forced IPv6 with only internal address", networkTestInterface, NetworkProtocolIPv6, nil, networkTestIPv6Private, networkTestIPv6Private,
			networkTestIface(networkTestInterface, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv4, networkTestIPv4Private),
				networkTestIP(NetworkProtocolIPv6, networkTestIPv6Private))),
		newNetworkTestRow("Multiple interfaces without requested interface - should return ErrTooManyPotentials", "", NetworkProtocolAny,
			ErrTooManyPotentials, "", "",
			networkTestIface(networkTestInterface, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv4, networkTestIPv4Private)),
			networkTestIface("ens19", "12:34:56:AB:CD:EG",
				networkTestIP(NetworkProtocolIPv4, "192.168.0.2"))),
		newNetworkTestRow("No IP address found for requested protocol - should return ErrNoIPAddress", networkTestInterface, NetworkProtocolIPv6,
			ErrNoIPAddress, "", "",
			networkTestIface(networkTestInterface, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv4, networkTestIPv4Private))),
		newNetworkTestRow("Interface with no IP addresses - should skip and return empty", networkTestInterface, NetworkProtocolIPv4,
			ErrNoIPAddress, "", "",
			networkTestIface(networkTestInterface, networkTestHardwareAddress)),
		newNetworkTestRow("Only loopback addresses - should skip and return empty", networkTestInterface, NetworkProtocolIPv4,
			ErrNoIPAddress, "", "",
			networkTestIface(networkTestInterface, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv4, "127.0.0.1"))),
		newNetworkTestRow("Only unspecified addresses - should skip and return empty", networkTestInterface, NetworkProtocolIPv4,
			ErrNoIPAddress, "", "",
			networkTestIface(networkTestInterface, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv4, "0.0.0.0"))),
		newNetworkTestRow("Mixed addresses but none private/global - should return empty for that type", networkTestInterface, NetworkProtocolIPv4,
			ErrNoIPAddress, "", "",
			networkTestIface(networkTestInterface, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv4, "169.254.1.1"))), // link-local, not private or global
		newNetworkTestRow("NetworkProtocolAny with only IPv6 addresses", networkTestInterface, NetworkProtocolAny,
			nil, "", networkTestIPv6Public,
			networkTestIface(networkTestInterface, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv6, networkTestIPv6Public))),
		newNetworkTestRow("NetworkProtocolAny with only IPv4 addresses", networkTestInterface, NetworkProtocolAny,
			nil, "", networkTestIPv4Public,
			networkTestIface(networkTestInterface, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv4, networkTestIPv4Public))),
		newNetworkTestRow("Interface with empty hardware address - should be filtered out", "", NetworkProtocolIPv4,
			ErrNoIPAddress, "", "",
			networkTestIface(networkTestInterface, "",
				networkTestIP(NetworkProtocolIPv4, networkTestIPv4Private))),
		newNetworkTestRow("NetworkProtocolAny with both IPv4 and IPv6 - should prefer IPv6", networkTestInterface, NetworkProtocolAny,
			nil, "", networkTestIPv6Public,
			networkTestIface(networkTestInterface, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv4, networkTestIPv4Public),
				networkTestIP(NetworkProtocolIPv6, networkTestIPv6Public))),
		newNetworkTestRow("Multiple interfaces with a requested interface - should not be ambiguous", networkTestInterfaceAltOne, NetworkProtocolIPv4,
			nil, "10.0.0.2", "93.184.216.34",
			networkTestIface(networkTestInterfaceAlt, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv4, networkTestIPv4Private)),
			networkTestIface(networkTestInterfaceAltOne, networkTestHardwareAddress1,
				networkTestIP(NetworkProtocolIPv4, "10.0.0.2"),
				networkTestIP(NetworkProtocolIPv4, "93.184.216.34"))),
		newNetworkTestRow("Requested interface not present - should return ErrNoIPAddress", "eth9", NetworkProtocolIPv4,
			ErrNoIPAddress, "", "",
			networkTestIface(networkTestInterfaceAlt, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv4, networkTestIPv4Private))),
		newNetworkTestRow("Unparseable IP addresses - should be skipped", networkTestInterface, NetworkProtocolAny,
			ErrNoIPAddress, "", "",
			networkTestIface(networkTestInterface, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv4, "not-an-ip"),
				networkTestIP(NetworkProtocolIPv6, "also-not-an-ip"))),
		newNetworkTestRow("IPv6 link-local address - should not be used as internal or external", networkTestInterface, NetworkProtocolIPv6,
			ErrNoIPAddress, "", "",
			networkTestIface(networkTestInterface, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv6, "fe80::1"))),
		newNetworkTestRow("Only IPv6 unspecified address - should skip and return empty", networkTestInterface, NetworkProtocolIPv6,
			ErrNoIPAddress, "", "",
			networkTestIface(networkTestInterface, networkTestHardwareAddress,
				networkTestIP(NetworkProtocolIPv6, "::"))),
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			internalAddress, externalAddress, err := determineAddresses(testCase.networkInterfaces, testCase.requestedInterface, testCase.requestedProtocol)

			require.ErrorIs(t, err, testCase.expectedError)
			require.Equal(t, testCase.expectedInternal, internalAddress)
			require.Equal(t, testCase.expectedExternal, externalAddress)
		})
	}
}
