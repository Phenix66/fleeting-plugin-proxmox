# Contributing

Thank you for your interest in contributing to the GitLab fleeting plugin for Proxmox
Virtual Environment. This project follows the Linux Foundations Developer's Certificate
of Origin (DCO):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

## Signed Commits

All commits **must be signed off and signature-verified**. A continuous integration
check inspects every commit in a pull request and fails if any commit does not (1)
carry a `Signed-off-by` line that matches the commit's author name and email, and
(2) show "Verified" on GitHub. Pull requests that fail this check cannot be merged
per project settings.

To sign commits, see the [GitHub Docs](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits)
for instructions on how to configure GPG or SSH signing.

To sign off, use the `-s` option with `git commit`, or enter the message manually.
The `-s` option automatically uses the configured user.name and user.email.

```
Signed-off-by: Tux <tux@email.com>
```

The signoff name and email must match the commit's author exactly (email matching is
case-insensitive), so make sure the user.name and user.email in your git configuration
matches your GitHub identity.

## Issues and Pull Requests

- Pull requests do **not** require an associated issue. You may open a PR directly.
- Opening an issue is *encouraged* before starting work, to begin discussion on
    behavior and design. This helps align expectations and avoids rework.
