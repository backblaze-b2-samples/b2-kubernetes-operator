# Architecture

## Components

```
cmd/manager            entrypoint: flags, manager, cache configuration
api/v1alpha1           CRD types (ClusterProviderConfig, B2AccessPolicy, Bucket, ApplicationKey)
internal/controller    reconcilers
internal/policy        B2AccessPolicy evaluation (pure functions + a cluster reader)
internal/provider      ClusterProviderConfig -> authorized B2 client, cached per config
internal/b2            B2 Native API v4 client (no third-party dependencies)
internal/b2/b2fake     in-memory B2 used by unit, integration and e2e tests
cmd/b2fake             the fake as a binary, for kind e2e tests only
charts/b2-kubernetes-operator     Helm chart (CRDs and manager RBAC are generated)
```

## Reconcile flow

Every reconcile follows the same order:

1. **Resolve the account.** The referenced `ClusterProviderConfig` must be `Ready`. Its client is cached and rebuilt when the config generation or credentials Secret changes.
2. **Authorize with policy.** Nothing is sent to B2 until a `B2AccessPolicy` allows the request.
3. **Observe B2.** Read the live bucket, or verify the live key.
4. **Act.** Make one revision-checked `b2_update_bucket`, or run the create → Secret → retire sequence for keys.
5. **Record.** Patch status with an optimistic lock and set the `Ready` condition. Emit events on transitions.
6. **Requeue** at the earliest of: the resync period, the next key revocation, rotation, renewal or verification.

Status-only updates do not trigger reconciles. Spec changes, annotation changes, deletion, owned Secret changes, policy changes, namespace label changes, and provider readiness changes do.

## Bucket ownership

Ownership lives in B2, in the bucket's `bucketInfo` under `b2operator-owner-uid`. This is what makes create idempotent across crashes. If the create response is lost, the next reconcile finds the bucket by name, sees its own UID, and continues. It also prevents two resources, or two clusters, from managing one bucket.

On `Retain` deletion the key is removed, so the bucket can be adopted again later.

## Key lifecycle

```
            ┌──────────────── spec change / lost Secret / rotation due / near expiry / revoked in B2
            ▼
 status.pendingKeyName = name   (persisted, optimistic lock)
            │
 b2_create_key (never retried automatically)
            │
 write Secret (owned, labelled)
            │
 old key -> status.retiringKeys (revokeAfter = now + gracePeriod)
 new key -> status.keyID; pendingKeyName cleared
            │
 ...grace period...
            ▼
 b2_delete_key(old)
```

If the operator stops between creating a key and recording it, `pendingKeyName` remains set. The next reconcile confirms its view is current with an uncached read, lists keys with that name, and revokes the ones that were never recorded.

If the operator stops after writing the Secret but before recording the key, the Secret already holds a key under the pending name. That key is adopted as current rather than revoked; the spec hash and creation time are read from the Secret's annotations.

A key that no longer exists in B2 (`b2_delete_key` returns 400 and `b2_list_keys` does not find it) is treated as already revoked.

## B2 client behavior

| Situation | Behavior |
| --- | --- |
| No token yet / token expired (`expired_auth_token`, `bad_auth_token`) | Authorize and retry once; concurrent callers share one re-authorization |
| `429`, `408`, `5xx`, transport error on an idempotent call | Up to 3 retries, jittered exponential backoff, at least `Retry-After` |
| Same on `b2_create_bucket` / `b2_create_key` | Not retried; the reconciler recovers via ownership tags or `pendingKeyName` |
| `b2_update_bucket` | Always sent with `ifRevisionIs`; a `conflict` re-reads and retries |
| Credentials rejected | `CredentialsError`; the provider config shows `InvalidCredentials` |

Reconciler handling of errors that persist: throttling is requeued after `Retry-After`. Transient errors use controller-runtime's exponential backoff. Permanent errors such as `duplicate_bucket_name` are requeued after 10 minutes with a clear condition.

## Testing layers

| Layer | What runs | Command |
| --- | --- | --- |
| Unit | B2 client against the fake; policy evaluation | `go test ./internal/b2/... ./internal/policy/...` |
| Integration | All controllers in a real API server (envtest) against the fake over HTTP, with fault injection | `make test` |
| End to end | Release image and Helm chart in kind, with the fake B2 deployed in-cluster | `make test-e2e` |
| Live | All controllers in envtest against the real B2 API, verified by reading B2 directly (opt-in, needs credentials) | `make test-live` |
