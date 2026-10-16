# Fleeting plugin for Proxmox Virtual Environment

This is a [fleeting](https://gitlab.com/gitlab-org/fleeting/fleeting) plugin for [Proxmox Virtual Environment](https://www.proxmox.com/en/proxmox-virtual-environment/overview).

## Installation

See [GitLab fleeting documentation](https://docs.gitlab.com/runner/fleet_scaling/fleeting/#install-a-fleeting-plugin)

This plugins OCI registry and namespace is:

```
ghcr.io/phenix66/fleeting-plugin-proxmox
```

The following image tags are available:

- Official release versions (e.g. `2.1.0`)
- `latest`: points to the most recent release
- `0.0.0-bleeding`: points to the most recent build of the `main` branch.

## Configuration

### Plugin settings

| Parameter                    | Type                      | Default value                           | Description                                                                                                                                                                                                                                                                                                          |
| ---------------------------- | ------------------------- | --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                        | string                    | N/A (required)                          | Proxmox VE URL.                                                                                                                                                                                                                                                                                                      |
| `insecure_skip_tls_verify`   | bool                      | `false`                                 | If `true` then TLS certificate verification is disabled.                                                                                                                                                                                                                                                             |
| `credentials_file_path`      | string                    | N/A (required)                          | Path to Proxmox VE credentials file.                                                                                                                                                                                                                                                                                 |
| `pool`                       | string                    | N/A (required)                          | Name of the Proxmox VE pool to use.                                                                                                                                                                                                                                                                                  |
| `storage`                    | string                    | N/A (required if `template_id` is a VM) | Name of the Proxmox VE storage to use. Leave unset or empty if `template_id` refers to a template to perform linked clones. If a value is set and `template_id` is a template, full clones will be performed instead. If `template_id` is another VM, full clones are always performed and this setting is required. |
| `template_id`                | int                       | N/A (required)                          | ID of the Proxmox VE VM to create instances from.                                                                                                                                                                                                                                                                    |
| `instance_target_node`       | string                    | (None - same node as the template)      | Name of the Proxmox VE node to clone instances to. If unset, instances are cloned onto the node hosting the template. Note that this feature requires some form of cluster distributed storage (e.g. Ceph) that exists on both the node with the template and the target node.                                       |
| `max_instances`              | int                       | N/A (required)                          | Maximum instances than can be deployed.                                                                                                                                                                                                                                                                              |
| `instance_network_interface` | string                    | None                                    | Network interface to read instance's IPv4 address from. If unspecified, the plugin will attempt to automatically determine the interface to use if there is only one non-loopback/local interface attached to the instance.                                                                                          |
| `instance_network_protocol`  | `any` or `ipv4` or `ipv6` | `ipv4`                                  | Network protocol to look for when discovering instance's IP address. `any` prioritizes IPv6.                                                                                                                                                                                                                         |
| `instance_name_creating`     | string                    | `fleeting-creating`                     | Name to set for instances during creation.                                                                                                                                                                                                                                                                           |
| `instance_name_running`      | string                    | `fleeting-running`                      | Name to set for running instances.                                                                                                                                                                                                                                                                                   |
| `instance_name_removing`     | string                    | `fleeting-removing`                     | Name to set for instances during removal.                                                                                                                                                                                                                                                                            |
| `instance_tags_creating`     | string                    | None (`""`)                             | Tags to set for instances during creation. Separate multiple tags with semicolons (`;`). Tags the instance already carries (from the template or applied manually) are preserved; only the tags configured here are added or removed as the instance changes state.                                                                                       |
| `instance_tags_running`      | string                    | None (`""`)                             | Tags to set for running instances. Separate multiple tags with semicolons (`;`). Tags the instance already carries (from the template or applied manually) are preserved; only the tags configured here are added or removed as the instance changes state.                                                                                           |
| `instance_tags_removing`     | string                    | None (`""`)                             | Tags to set for instances during removal. Separate multiple tags with semicolons (`;`). Tags the instance already carries (from the template or applied manually) are preserved; only the tags configured here are added or removed as the instance changes state.                                                                                          |
| `instance_autoresize_disk`   | string                    | (None - disabled)                       | Name of disk to autoresize after cloning. After the resize, the plugin reads the disk size back from the VM configuration; an instance whose disk did not reach the expected size is marked for removal and reported as failed.                                                                                    |
| `instance_autoresize_size`   | string                    | (None - disabled)                       | Absolute or increment size to autoresize after cloning. Examples: 10G, +5.5G                                                                                                                                                                                                                                         |
| `storage_reserve`            | string                    | (None - disabled)                       | Free space to keep on `storage` (required when set). When set, every `Increase` reads the storage's free space and clones only as many instances as fit while leaving this much free; the rest of the demand stays pending until space frees up. An instance counts as the size of all its template's disks, with `instance_autoresize_disk` at its resized size. Examples: 5G, 512M, 0. See [Storage capacity](#storage-capacity). |
| `instance_cores`             | int                       | (None - template's value)               | Number of CPU cores to set on instances after cloning, before they are started. Must be greater than zero.                                                                                                                                                                                                           |
| `instance_memory`            | int                       | (None - template's value)               | Memory, in MiB, to set on instances after cloning, before they are started. Must be greater than zero. If the template uses ballooning, its minimum memory (`balloon`) must not exceed this value.                                                                                                                  |
| `proxmox_task_wait_interval` | int                       | 2                                       | How often, in seconds, to check for Proxmox task completion. Must be greater than zero.                                                                                                                                                                                                                                                                       |

### Storage capacity

Without `storage_reserve`, the plugin clones as many instances as fleeting asks for, up to
`max_instances`, and a clone or resize that runs out of space fails only once Proxmox has started
on it. With `storage_reserve`, the free space reported by Proxmox for `storage` on the target node
is checked before each batch of clones:

- On thick storage (LVM, directory with raw images), free space is what clones really allocate,
  so the check is exact for this manager.
- On thin storage (LVM-thin, ZFS, qcow2), free space is the data actually written, not the size
  promised to existing instances. The check then limits new clones but does not prevent
  instances that are already running from filling the storage as they write.
- Several managers cloning onto the same storage each check it on their own, so two of them
  can both see the same free space. Set `storage_reserve` to at least one instance's worth per
  other manager sharing the storage; a clone that still runs out of space fails as before and
  its instance is cleaned up.

The API user needs `Datastore.Audit` on the storage to read its status; `PVEDatastoreUser`
includes it.

### Credentials file

The credentials file selects the authentication method:

- **API token (recommended):** set `username`, `realm`, `token_id` and `token_secret`.
  The token is a static secret attached to every request, so the client holds no session
  and there is no ticket to refresh. Create one with
  `pveum user token add <user> --tokenid <token_id>`; the printed secret is `token_secret`.
- **Ticket (username + password):** set `username` and `password` (with `realm` and
  optional `otp`). The plugin exchanges them for a session ticket and renews it hourly.
  This remains supported for backwards compatibility.

| Parameter      | Type   | Description                          |
| -------------- | ------ | ------------------------------------ |
| `realm`        | string | Authentication realm                 |
| `username`     | string | User name                            |
| `password`     | string | User password (ticket auth)          |
| `otp`          | string | One-time password for 2FA (ticket)   |
| `token_id`     | string | API token id (token auth)            |
| `token_secret` | string | API token secret (token auth)        |

### Template VM configuration

The template must be a bootable VM with enabled DHCP and QEMU guest agent installed. See [Proxmox documentation](https://pve.proxmox.com/wiki/Qemu-guest-agent) for more details.

### Proxmox configuration

You **MUST** create a **DEDICATED** user, pool, and storage for usage with this plugin. Any other configuration is untested and unsupported.

After creating a **DEDICATED** user, pool and, storage follow procedure below to add required permissions:

1. Add template VM as a member to the pool.
2. Add storage as a member to the pool.
3. Add following roles for the user to the pool:
   * `PVEVMAdmin`,
   * `PVEPoolUser`,
   * `PVEDatastoreUser`.
4. Add following role for the user to the network that you will use for deployed VMs:
    * `PVESDNAdmin`.
5. Add following role for the user to the node with the storage, network, template etc.:
    * `PVEAuditor` without propagation.

If using an API Token with privilege separation enabled, ensure these privileges are set
as API token permissions. If privilege separation is not enabled on the token, they can
be set as either token or user permissions.

### Running multiple managers on one host

Give every runner manager its own user, pool, template, and unique `instance_name_creating`,
`instance_name_running` and `instance_name_removing` values: three different names that no
other manager uses. The user's permissions should cover only that manager's pool. Proxmox
checks every VM operation against the pool the VM is in, so a user with rights only on a
single pool cannot touch another manager's VMs even if a VM ID is reused between them. A
VM belongs to at most one pool and the plugin looks its template up in its own pool, so
managers that start from the same image each need their own copy of the template in their
own pool.

On top of that, apart from the VM it has just cloned, the plugin only renames, deletes, or
connects to VMs that carry one of its own three names, and it checks the name both in the pool
listing and on the VM itself before acting. A VM with any other name is left alone. This is what
protects managers that share a user or a pool, and it only holds once all of them run a plugin
version with this check: an older one renames any VM it is asked to remove unless the VM carries
its own creating or removing name, even when the VM ID now belongs to another manager.

Known limitation: managers that share a pool cannot tell each other's VMs apart by any
instance name they have in common, including a name left at its default.

## Development

### Integration tests

1. Create and fill out `credentials.json` file in project root (see [Credentials file](#credentials-file) for details)
    ```bash
    cp credentials.example.json credentials.json
    ```
2. Create and fill out `config.json` file in project root (see [Plugin settings](#plugin-settings) for details)
    ```bash
    cp config.example.json config.json
    ```
3. Run integration tests:
    ```bash
    make integration-test
    ```
