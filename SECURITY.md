# Security Policy

## Reporting

**Do not open a public GitHub issue for security vulnerabilities.**

Please use
[GitHub Private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing/privately-reporting-a-security-vulnerability)
on this repository:

https://github.com/samuelfabel/janus/security/advisories/new

You may also email **samuelfabel@hotmail.com** if private reporting is unavailable.

Include:

- Description of the issue
- Steps to reproduce
- Potential impact
- Environment (OS, Rust version, Janus revision)

Acknowledgement target: within **72 hours**.

## Supported versions

| Version | Supported |
| ------- | --------- |
| `main` (development) | Yes |
| Tagged releases | Latest only |

## Process

Acknowledge → triage → reproduce → private fix → patched release → advisory when applicable.

Please do not disclose publicly until a fix is available or maintainers agree.

## Out of scope

- Misconfiguration of deployments
- Issues only present in unmodified third-party dependencies (report upstream)
- Social engineering
