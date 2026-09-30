# Changelog

## Unreleased

### Added
- `ClusterProviderConfig` for per-account credentials, validated against B2 with account details in status.
- `B2AccessPolicy` for per-namespace allow-lists, enforced by the controllers (default deny).
- `Bucket`: bucket type, bucket info, lifecycle rules, CORS rules, default SSE-B2 encryption, Object Lock and default retention. Ownership is recorded in B2, adoption is explicit, drift is corrected, and `deletionPolicy` defaults to `Retain`.
- `ApplicationKey`: bucket, prefix and capability scoping, expiry, scheduled rotation, zero-downtime replacement with a grace period, crash-safe creation, revocation on policy violation, and S3-compatible Secret output.
- Native API v4 client with re-authorization, retries and metrics; an in-memory fake B2 for tests.
- Helm chart, multi-arch distroless image, CI (lint, envtest, kind e2e, govulncheck, Trivy), and a signed release pipeline.
