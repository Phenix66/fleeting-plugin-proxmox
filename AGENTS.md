# Agents

This project is a Proxmox VE plugin for the GitLab "fleeting" auto-scaling framework.
It manages "instances" of cloned VMs as instructed by a gitlab-runner process.

## Key File Structures

- `.github/workflows`: Defines GitHub Actions (CI/CD)
- `cmd/fleeting-plugin-proxmox`: Contains the plugin source code
- `config.example.json`: Provides example plugin configuration in JSON format
- `credentials.example.json`: Provides an example of a valid Proxmox credentials file
- `Makefile`: Wraps common build and test operations with the `make` command
- `README.md`: Documents plugin installation and usage

## Build & Test Commands

```shell
make build      # Builds the plugin
make lint       # Runs code style enforcement
make unit-test  # Runs the unit testing suite
```

## Making Changes

- All code must be linted and unit tested using the `make` commands above.
- All configuration must be documented in `README.md`
- All commits require DCO sign-off
- When updating the Go and/or golangci-lint version, ensure to keep the GitHub actions in lock step
