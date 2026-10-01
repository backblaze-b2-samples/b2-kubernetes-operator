# Security model

This document describes what the operator protects against, how, and what remains the cluster operator's responsibility.

## Trust boundaries

| Actor | Trusted with | Can do |
| --- | --- | --- |
| Cluster admin | The operator's B2 key; `ClusterProviderConfig`, `B2AccessPolicy` | Anything in the B2 account |
| Operator | The operator's B2 key (read from a Secret, never cached in the informer cache) | Everything its key allows, but only acts as policies permit |
| Tenant (namespace editor) | `Bucket` and `ApplicationKey` in their namespace | Only what a `B2AccessPolicy` selecting their namespace allows |
| Workload | The Secret created for its `ApplicationKey` | Only what that key's capabilities, bucket and prefix allow |

## Threats and mitigations

| Threat | Mitigation |
| --- | --- |
| A tenant takes over another team's bucket by naming a `Bucket` after it | Buckets are named explicitly (`spec.bucketName`) and must match the policy's `namePatterns` (for example `acme-{namespace}-*`). An existing bucket is never managed unless `adoptExisting` is set **and** the policy allows adoption. A bucket owned by another resource (per its `b2operator-owner-uid` bucket info) is never adopted. |
| Name patterns overlap between namespaces (`acme-{namespace}-*` for namespace `a` also matches `acme-a-b-data` of namespace `a-b`) | Patterns only gate names. Access is gated by ownership: keys for an existing bucket (`bucketName`) are refused if a Bucket outside the requesting namespace manages it, or if another namespace released it. Adoption of a released bucket is limited to the releasing namespace. The remaining risk is name squatting (namespace `a` creating `acme-a-b-…` first), which denies service but gives no access. To rule it out, use exact patterns without a wildcard next to `{namespace}` (for example `acme-{namespace}` and `acme-{namespace}-logs`), or keep tenant namespace names from being prefixes of each other. Moving `{namespace}` to the end (`acme-*-{namespace}`) does not help, because suffixes overlap the same way. |
| A tenant makes a bucket public | `bucketType: allPublic` requires `buckets.allowPublic`. |
| A tenant deletes data by deleting a resource | `deletionPolicy` defaults to `Retain`. `Delete` requires `buckets.allowDeletion`, is re-checked at deletion time, and B2 only deletes empty buckets. |
| A tenant mints an over-privileged key | Capabilities must be in `keys.allowedCapabilities`. Account-wide keys need `keys.allowAccountWide`, keys for buckets outside the namespace need `keys.allowExternalBuckets` plus a matching name pattern, and `keys.maxValidity` forces expiry. |
| A tenant escalates to account admin via `writeKeys` | `listKeys`, `writeKeys` and `deleteKeys` are refused by the operator itself unless it runs with `--allow-key-management-capabilities`, regardless of policy. |
| Scope silently ignored (a known issue in earlier community operators) | `namePrefix` and `validFor` are always sent to B2. Integration tests assert that the created key carries them. |
| Compliance-mode Object Lock locks in cost | Compliance retention requires `buckets.allowComplianceRetention`. |
| A policy is tightened but old keys remain valid | Keys that no longer satisfy any policy are revoked and their Secrets deleted (`--revoke-on-policy-violation`, default on). Revocation is scheduled for the end of the key's grace period (`status.scheduledRevocation`), is re-confirmed against uncached data, and is cancelled if access is restored in time. |
| A key outlives its bucket | A Bucket's finalizer waits until no ApplicationKey references it. A key whose referenced Bucket disappears is revoked after its grace period. |
| A tenant removes the finalizer to keep a key alive | Every key name carries this cluster's ID and the resource UID (`b2op-<cluster>-<uid>-…`). A periodic sweep revokes keys whose resource no longer exists (`--orphan-key-sweep-interval`). Keys of other clusters sharing the account are never touched. |
| An operator crash leaves a live, untracked key | The key name is recorded in `status.pendingKeyName` before `b2_create_key`. On restart, keys with that name that were never recorded are found. A key that already reached the Secret is adopted as current; any other is revoked. Orphan cleanup re-reads the resource and Secret from the API server first, so it never acts on a stale cache. Ambiguous responses (timeouts, throttling, 5xx) keep the pending marker. |
| Credentials rotated by breaking consumers | New key → Secret updated → old key revoked after `gracePeriod`. A key is never revoked before its replacement is delivered. |
| The operator overwrites an application Secret | The operator writes only Secrets it controls (by ownerReference). A same-named Secret it does not own causes `SecretConflict`, and no key is created. |
| Admission is bypassed or webhooks are down | There are no webhooks to bypass. Policy is evaluated in the controller before every B2 mutation; CRD schema and CEL rules handle static validation. |
| A console or CLI change weakens a bucket | Drift (bucket type, info, lifecycle, CORS, and managed encryption / Object Lock settings) is reverted within `--resync-period` and recorded as a `DriftCorrected` warning event. |
| The master key is overused | The Partner API master key lives only in the partner config, which Buckets and ApplicationKeys cannot reference. Customer accounts are managed through an operator-created application key, not their master key. Provider configs report their key type, and warn when a master key is used for bucket management. |
| API traffic is intercepted | Only `https://` API URLs are accepted unless `--allow-insecure-api-url` is set, which is for tests. |

## Cluster admin responsibilities

- **Restrict RBAC.** Only cluster admins should be able to write `ClusterProviderConfig` or `B2AccessPolicy`, or read the credentials Secret. The Helm chart's aggregated roles give namespace `admin`/`edit` users access to `Bucket` and `ApplicationKey` only.
- **Protect namespace labels.** Policies select namespaces by label, so anyone who can label a namespace can opt it into a policy. Use policy selectors on labels tenants cannot set, or enforce label ownership with an admission policy.
- **Scope the operator key.** Give it only the capabilities it needs. If it only serves some buckets, use a bucket-restricted key or a separate B2 account.
- **Manage the credentials Secret** with External Secrets, Sealed Secrets or similar, rather than Helm values.
- **Watch for `PolicyDenied`, `DriftCorrected` and `KeyRevoked` events.** They signal attempted policy violations or out-of-band changes.

## Operator permissions in Kubernetes

The operator's ClusterRole grants `get`, `list`, `watch`, `create`, `update`, `patch` and `delete` on Secrets in every namespace:
- It writes key Secrets into tenant namespaces.
- It reads the credentials Secret from wherever the `ClusterProviderConfig` points.
- It watches its own Secrets so that a deleted Secret is replaced immediately.

Its informer cache only holds Secrets labelled `app.kubernetes.io/managed-by=b2-operator`. RBAC cannot express that restriction, though. **A compromised operator pod could read any Secret in the cluster.** Treat the operator namespace accordingly: restrict who can exec into it or change its Deployment, and keep the image pinned by digest.

**Tenant key capabilities that act on buckets.** If a policy lets tenant keys hold `writeBuckets`, `deleteBuckets` or `writeBucketRetentions`, a workload using such a key can change its bucket directly. It could make the bucket public, or strip the operator's ownership marker. The operator reverts managed settings within `--resync-period`, but leave these capabilities out of `allowedCapabilities` unless a workload needs them.

## Reporting a vulnerability

See [SECURITY.md](../SECURITY.md).
