# Changelog

## Unreleased

### Added
- `ClusterProviderConfig` for per-account credentials, validated against B2 with account details in status.
- `B2AccessPolicy` for per-namespace allow-lists, enforced by the controllers (default deny).
- `Bucket`: bucket type, bucket info, lifecycle rules, CORS rules, default SSE-B2 encryption, Object Lock and default retention. Ownership is recorded in B2, adoption is explicit, drift is corrected, and `deletionPolicy` defaults to `Retain`.
- `ApplicationKey`: bucket, prefix and capability scoping, expiry, scheduled rotation, zero-downtime replacement with a grace period, crash-safe creation, revocation on policy violation, and S3-compatible Secret output.
- `B2Account`: per-customer, per-region accounts through the Partner API, with generated email addresses, durable storage of the returned key, a published provider config and access policy, and Retain/Eject deletion.
- Cloud Replication between Buckets, across accounts and regions, with managed replication keys.
- `RemoteCluster` and `ApplicationKey.spec.deliverTo`: deliver key Secrets into other clusters, so one operator can serve per-customer clusters. Delivery targets must be allowed by policy, kubeconfigs that run commands or read local files are rejected, and the operator never overwrites a Secret it did not create.
- SSE-B2 encryption by default; unencrypted buckets and replication must be allowed by policy.
- Native API v4 client with re-authorization, retries and metrics; an in-memory fake B2 for tests.
- Helm chart, multi-arch distroless image, CI (lint, envtest, kind e2e across two clusters, govulncheck, Trivy), and a signed release pipeline.
- An opt-in test suite against the live B2 API, including Partner API accounts, eject, and multi-region replication; B2 behaviour that differs from the API reference is recorded in `test/live/README.md`.
