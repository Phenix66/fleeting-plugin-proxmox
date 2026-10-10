package plugin

import (
	"testing"

	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
)

func Test_determineAddresses(t *testing.T) {
	tests := []struct {
		name string

		requestedInterface string
		requestedProtocol  NetworkProtocol
		networkInterfaces  []*proxmox.AgentNetworkIface

		expectedError           error
		expectedInternalAddress string
		expectedExternalAddress string
	}{
		{
			name: "No network interfaces",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolAny,
			networkInterfaces:  []*proxmox.AgentNetworkIface{},

			expectedError:           ErrNoIPAddress,
			expectedInternalAddress: "",
			expectedExternalAddress: "",
		},
		{
			name: "Any",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolAny,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "8.8.8.8",
						},
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.1",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "2001:4860:4860::8888",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "fd3b:47fc:de09::1",
						},
					},
				},
			},

			expectedError:           nil,
			expectedInternalAddress: "fd3b:47fc:de09::1",
			expectedExternalAddress: "2001:4860:4860::8888",
		},
		{
			name: "Single interface using default behavior",

			requestedInterface: "",
			requestedProtocol:  NetworkProtocolAny,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "eth0",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "8.8.8.8",
						},
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.1",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "2001:4860:4860::8888",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "fd3b:47fc:de09::1",
						},
					},
				},
			},

			expectedError:           nil,
			expectedInternalAddress: "fd3b:47fc:de09::1",
			expectedExternalAddress: "2001:4860:4860::8888",
		},
		{
			name: "Multiple interfaces using default behavior",

			requestedInterface: "",
			requestedProtocol:  NetworkProtocolAny,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "eth0",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "8.8.8.8",
						},
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.1",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "2001:4860:4860::8888",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "fd3b:47fc:de09::1",
						},
					},
				},
				{
					Name:            "eth1",
					HardwareAddress: "12:34:56:AB:CD:E1",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "8.8.4.4",
						},
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.2",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "2001:4860:4860::8844",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "fd3b:47fc:de09::2",
						},
					},
				},
			},

			expectedError:           ErrTooManyPotentials,
			expectedInternalAddress: "",
			expectedExternalAddress: "",
		},
		{
			name: "Multiple interfaces with loopback using default behavior",

			requestedInterface: "",
			requestedProtocol:  NetworkProtocolAny,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "Ethernet",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "8.8.8.8",
						},
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.1",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "2001:4860:4860::8888",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "fd3b:47fc:de09::1",
						},
					},
				},
				{
					Name:            "Loopback Pseudo-Interface 1",
					HardwareAddress: "",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "127.0.0.1",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "::1",
						},
					},
				},
			},

			expectedError:           nil,
			expectedInternalAddress: "fd3b:47fc:de09::1",
			expectedExternalAddress: "2001:4860:4860::8888",
		},
		{
			name: "Forced IPv4",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolIPv4,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "8.8.8.8",
						},
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.1",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "2001:4860:4860::8888",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "fd3b:47fc:de09::1",
						},
					},
				},
			},

			expectedError:           nil,
			expectedInternalAddress: "192.168.0.1",
			expectedExternalAddress: "8.8.8.8",
		},
		{
			name: "Forced IPv6",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolIPv6,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "8.8.8.8",
						},
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.1",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "2001:4860:4860::8888",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "fd3b:47fc:de09::1",
						},
					},
				},
			},

			expectedError:           nil,
			expectedInternalAddress: "fd3b:47fc:de09::1",
			expectedExternalAddress: "2001:4860:4860::8888",
		},
		{
			name: "Any with only internal address",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolAny,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.1",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "fd3b:47fc:de09::1",
						},
					},
				},
			},

			expectedError:           nil,
			expectedInternalAddress: "fd3b:47fc:de09::1",
			expectedExternalAddress: "fd3b:47fc:de09::1",
		},
		{
			name: "Forced IPv4 with only internal address",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolIPv4,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.1",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "fd3b:47fc:de09::1",
						},
					},
				},
			},

			expectedError:           nil,
			expectedInternalAddress: "192.168.0.1",
			expectedExternalAddress: "192.168.0.1",
		},
		{
			name: "Forced IPv6 with only internal address",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolIPv6,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.1",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "fd3b:47fc:de09::1",
						},
					},
				},
			},

			expectedError:           nil,
			expectedInternalAddress: "fd3b:47fc:de09::1",
			expectedExternalAddress: "fd3b:47fc:de09::1",
		},
		{
			name: "Multiple interfaces without requested interface - should return ErrTooManyPotentials",

			requestedInterface: "",
			requestedProtocol:  NetworkProtocolAny,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.1",
						},
					},
				},
				{
					Name:            "ens19",
					HardwareAddress: "12:34:56:AB:CD:EG",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.2",
						},
					},
				},
			},

			expectedError:           ErrTooManyPotentials,
			expectedInternalAddress: "",
			expectedExternalAddress: "",
		},
		{
			name: "No IP address found for requested protocol - should return ErrNoIPAddress",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolIPv6,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.1",
						},
					},
				},
			},

			expectedError:           ErrNoIPAddress,
			expectedInternalAddress: "",
			expectedExternalAddress: "",
		},
		{
			name: "Interface with no IP addresses - should skip and return empty",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolIPv4,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses:     []*proxmox.AgentNetworkIPAddress{},
				},
			},

			expectedError:           ErrNoIPAddress,
			expectedInternalAddress: "",
			expectedExternalAddress: "",
		},
		{
			name: "Only loopback addresses - should skip and return empty",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolIPv4,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "127.0.0.1",
						},
					},
				},
			},

			expectedError:           ErrNoIPAddress,
			expectedInternalAddress: "",
			expectedExternalAddress: "",
		},
		{
			name: "Only unspecified addresses - should skip and return empty",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolIPv4,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "0.0.0.0",
						},
					},
				},
			},

			expectedError:           ErrNoIPAddress,
			expectedInternalAddress: "",
			expectedExternalAddress: "",
		},
		{
			name: "Mixed addresses but none private/global - should return empty for that type",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolIPv4,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "169.254.1.1", // link-local, not private or global
						},
					},
				},
			},

			expectedError:           ErrNoIPAddress,
			expectedInternalAddress: "",
			expectedExternalAddress: "",
		},
		{
			name: "NetworkProtocolAny with only IPv6 addresses",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolAny,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "2001:4860:4860::8888",
						},
					},
				},
			},

			expectedError:           nil,
			expectedInternalAddress: "",
			expectedExternalAddress: "2001:4860:4860::8888",
		},
		{
			name: "NetworkProtocolAny with only IPv4 addresses",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolAny,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "8.8.8.8",
						},
					},
				},
			},

			expectedError:           nil,
			expectedInternalAddress: "",
			expectedExternalAddress: "8.8.8.8",
		},
		{
			name: "Interface with empty hardware address - should be filtered out",

			requestedInterface: "",
			requestedProtocol:  NetworkProtocolIPv4,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.1",
						},
					},
				},
			},

			expectedError:           ErrNoIPAddress,
			expectedInternalAddress: "",
			expectedExternalAddress: "",
		},
		{
			name: "NetworkProtocolAny with both IPv4 and IPv6 - should prefer IPv6",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolAny,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "8.8.8.8",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "2001:4860:4860::8888",
						},
					},
				},
			},

			expectedError:           nil,
			expectedInternalAddress: "",
			expectedExternalAddress: "2001:4860:4860::8888",
		},
		{
			name: "Multiple interfaces with a requested interface - should not be ambiguous",

			requestedInterface: "eth1",
			requestedProtocol:  NetworkProtocolIPv4,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "eth0",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.1",
						},
					},
				},
				{
					Name:            "eth1",
					HardwareAddress: "12:34:56:AB:CD:E1",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "10.0.0.2",
						},
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "93.184.216.34",
						},
					},
				},
			},

			expectedError:           nil,
			expectedInternalAddress: "10.0.0.2",
			expectedExternalAddress: "93.184.216.34",
		},
		{
			name: "Requested interface not present - should return ErrNoIPAddress",

			requestedInterface: "eth9",
			requestedProtocol:  NetworkProtocolIPv4,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "eth0",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "192.168.0.1",
						},
					},
				},
			},

			expectedError:           ErrNoIPAddress,
			expectedInternalAddress: "",
			expectedExternalAddress: "",
		},
		{
			name: "Unparseable IP addresses - should be skipped",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolAny,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv4,
							IPAddress:     "not-an-ip",
						},
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "also-not-an-ip",
						},
					},
				},
			},

			expectedError:           ErrNoIPAddress,
			expectedInternalAddress: "",
			expectedExternalAddress: "",
		},
		{
			name: "IPv6 link-local address - should not be used as internal or external",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolIPv6,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "fe80::1",
						},
					},
				},
			},

			expectedError:           ErrNoIPAddress,
			expectedInternalAddress: "",
			expectedExternalAddress: "",
		},
		{
			name: "Only IPv6 unspecified address - should skip and return empty",

			requestedInterface: "ens18",
			requestedProtocol:  NetworkProtocolIPv6,
			networkInterfaces: []*proxmox.AgentNetworkIface{
				{
					Name:            "ens18",
					HardwareAddress: "12:34:56:AB:CD:EF",
					IPAddresses: []*proxmox.AgentNetworkIPAddress{
						{
							IPAddressType: NetworkProtocolIPv6,
							IPAddress:     "::",
						},
					},
				},
			},

			expectedError:           ErrNoIPAddress,
			expectedInternalAddress: "",
			expectedExternalAddress: "",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			internalAddress, externalAddress, err := determineAddresses(testCase.networkInterfaces, testCase.requestedInterface, testCase.requestedProtocol)

			require.ErrorIs(t, err, testCase.expectedError)
			require.Equal(t, testCase.expectedInternalAddress, internalAddress)
			require.Equal(t, testCase.expectedExternalAddress, externalAddress)
		})
	}
}
