# Architecture

## Layout

```
cmd/manager          entrypoint: flags, manager, cache configuration
cmd/b2fake           the fake B2 as a binary, for kind e2e tests only
api/v1alpha1         CRD types
internal/controller  reconcilers, one file per resource, plus shared pieces:
                       common.go   Deps, options, account resolution
                       status.go   Ready condition, status commits, finalizers
                       errors.go   stageError: a failure with its condition and retry
                       watches.go  predicates and map helpers
                       events.go   event reasons
internal/policy      B2AccessPolicy evaluation
internal/provider    ClusterProviderConfig -> authorized B2 client, cached
internal/remote      RemoteCluster -> Kubernetes client, cached
internal/b2          B2 Native API v4 client (no third-party dependencies)
internal/b2/b2fake   in-memory B2 used by every test layer
test/e2e             kind end-to-end test
test/live            tests against the real B2 API (opt-in)
charts/b2-kubernetes-operator  Helm chart (CRDs and manager RBAC are generated)
```

| Resource | Scope | Reconciler | Owns in B2 |
| --- | --- | --- | --- |
| `ClusterProviderConfig` | Cluster | validates credentials | nothing |
| `B2AccessPolicy` | Cluster | none; read by the policy evaluator | nothing |
| `RemoteCluster` | Cluster | checks the connection | nothing |
| `Bucket` | Namespace | buckets, replication | a bucket; replication keys |
| `ApplicationKey` | Namespace | keys and their Secrets | one key (plus retiring keys) |
| `B2Account` | Cluster | Partner API accounts | an account; its operations key |

## Reconcile flow

The Bucket, ApplicationKey and B2Account reconcilers follow the same shape:

1. **Load** the resource. If it is being deleted, run its finalizer logic and stop.
2. **Ensure the finalizer**, so B2 resources are cleaned up before the object disappears.
3. **Resolve the account.** The referenced `ClusterProviderConfig` must be `Ready`. Partner configs are refused for buckets and keys.
4. **Check policy.** Nothing is sent to B2 until a `B2AccessPolicy` allows it.
5. **Observe B2 and act:** a revision-checked bucket update, or the create → deliver → retire sequence for keys.
6. **Commit status once,** with an optimistic lock. A failure becomes `Ready=False` with a specific reason (a `stageError`), plus an event when the condition changes.
7. **Requeue** at the earliest of the resync period and any timer the resource has (key revocation, rotation, renewal or verification).

`ClusterProviderConfig` and `RemoteCluster` are simpler: they validate and report.

Status-only updates do not trigger reconciles. These do:
- spec, annotation or deletion changes;
- changes to owned Secrets;
- policy changes and namespace label changes;
- a dependency changing readiness (provider config, bucket, remote cluster).

## Bucket ownership

Ownership lives in B2, in the bucket's `bucketInfo` under `b2operator-owner-uid`. This makes creation idempotent across crashes: if the create response is lost, the next pass finds the bucket by name, sees its own UID, and continues. It also stops two resources, or two clusters, from managing one bucket.

On `Retain` deletion, `b2operator-owner-uid` is replaced by `b2operator-released-from: <namespace>`. Only that namespace can adopt the bucket again.

A bucket cannot be deleted or released while:
- ApplicationKeys holding credentials still reference it;
- another Bucket replicates into it;
- a source is still tearing down replication into it.

## Key lifecycle

```
            ┌──────────────── spec change / lost Secret / rotation due / near expiry / revoked in B2
            ▼
 status.pendingKeyName = name   (persisted, optimistic lock)
            │
 b2_create_key (never retried automatically)
            │
 write Secret (owned)
            │
 old key -> status.retiringKeys (revokeAfter = now + gracePeriod)
 new key -> status.keyID; pendingKeyName cleared
            │
 ...grace period...
            ▼
 b2_delete_key(old)
```

If the operator stops between creating a key and recording it, `pendingKeyName` stays set. The next pass confirms its view is current with an uncached read, then looks up keys with that name:
- **Already delivered:** a key that is in the Secret becomes the current key. Its spec hash and creation time are read from the Secret's annotations.
- **Never delivered:** any other key under that name is revoked.

Deleting a key that no longer exists counts as done. B2 answers such a delete with success; an error is accepted too, once `b2_list_keys` confirms the key is gone.

Every key the operator creates is named `b2op-<cluster id>-<owner uid>-…`. The periodic sweep revokes keys of this cluster whose owner no longer exists, for example because a finalizer was removed by hand.

## Replication

Replication is configured from the source Bucket, and needs two keys:
- **One source key per bucket**, in the source account, to read files. All of the bucket's rules share it.
- **One destination key per destination bucket**, in the destination account, to write files. It is registered in the destination's key mapping.

Both have deterministic names, so a pass interrupted after creating a key finds and reuses it.

B2 replaces a bucket's whole `replicationConfiguration` on every update, which contradicts the API reference. So every update sends both sides, the untouched one copied from B2. See [test/live/README.md](../test/live/README.md) for this and other behaviour confirmed against the live API.

## B2 client

| Situation | Behavior |
| --- | --- |
| No token yet, or token expired (`expired_auth_token`, `bad_auth_token`) | Authorize and retry once; concurrent callers share one re-authorization |
| `429`, `408`, `5xx` or transport error on an idempotent call | Up to 3 retries, jittered exponential backoff, at least `Retry-After` |
| Same on `b2_create_bucket`, `b2_create_key` or `b2_create_group_member` | Not retried; the reconciler recovers through ownership marks, `pendingKeyName` or deterministic names |
| `b2_update_bucket` | Always sent with `ifRevisionIs`; `conflict` re-reads and retries |
| Credentials rejected | `CredentialsError`; the provider config reports `InvalidCredentials` |

## Testing layers

| Layer | What runs | Command |
| --- | --- | --- |
| Unit | B2 client, policy, kubeconfig handling, against the fake | `go test ./internal/b2/... ./internal/policy/... ./internal/remote/...` |
| Integration | All controllers in a real API server (envtest) against the fake over HTTP, plus a second API server as a remote cluster | `make test` |
| End to end | Release image and Helm chart in kind, with the fake deployed in-cluster | `make test-e2e` |
| Live | All controllers in envtest against the real B2 API, checked by reading B2 directly | `make test-live` |
