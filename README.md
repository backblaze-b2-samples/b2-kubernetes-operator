# Backblaze B2 Operator for Kubernetes

Manage [Backblaze B2 Cloud Storage](https://www.backblaze.com/cloud-storage) buckets and scoped application keys as Kubernetes resources, safely, on shared clusters.

```yaml
apiVersion: b2.backblaze.com/v1alpha1
kind: ApplicationKey
metadata:
  name: uploads-writer
  namespace: shop
spec:
  bucketRef: {name: uploads}
  namePrefix: images/
  capabilities: [listFiles, readFiles, writeFiles]
  rotation: {every: 720h}
```

The operator creates the key in B2, restricted to one bucket and prefix. It delivers the key to a Secret that any S3 SDK can read, and rotates it every 30 days without downtime.

## Who it's for

- **Any Kubernetes team using B2.** Point the operator at your B2 account and manage buckets and scoped keys declaratively. Guardrails keep teams on a shared cluster within their own buckets. This is the [Quick start](#quick-start).
- **Hosting providers, GPU clouds and resellers.** With the Backblaze [Partner API](docs/partner.md), the operator also creates a separate B2 account for each customer and region, which you can build self-service storage on. See [docs/integration.md](docs/integration.md). This layer is optional and needs Partner API access.

> **Status:** `v1alpha1`. The API may change before `v1`. See [docs/roadmap.md](docs/roadmap.md).

## Features

- **Buckets:** type, bucket info, lifecycle rules, CORS rules, SSE-B2 encryption (on by default), Object Lock with default retention, and Cloud Replication across accounts and regions.
- **Customer accounts:** one B2 account per customer per region through the [Partner API](docs/partner.md). The key B2 returns once is stored and never discarded, and a provider config and access policy are published for each account.
- **Application keys:** restricted to a bucket, a file-name prefix and a set of capabilities, with optional expiry. Delivered to a Secret with S3-compatible variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, `AWS_REGION`).
- **Zero-downtime rotation:** the operator creates the new key, then updates the Secret, and revokes the old key only after a grace period. Rotation can run on a schedule, before expiry, or after a spec change, a lost Secret, or a key revoked outside Kubernetes.
- **Multi-tenant guardrails:** a cluster-scoped `B2AccessPolicy` controls, per namespace, which bucket names, capabilities, key lifetimes and bucket settings are allowed. It is enforced by the operator before every B2 call, not only at admission.
- **Safe ownership:** a bucket is managed only if this resource created it, or if adoption was requested *and* allowed by policy. Deleting a resource keeps the bucket by default (`deletionPolicy: Retain`).
- **Drift correction:** live B2 state is compared with the spec on every change and every resync period. Changes made in the console or CLI are reverted and reported as events.
- **Multiple accounts:** create one `ClusterProviderConfig` per B2 account.
- **Operable:** conditions and events on every resource, Prometheus metrics for B2 API calls, leader election, a restricted pod security context, and a distroless non-root image.

## Quick start

Prerequisites: Kubernetes 1.30+, Helm 3, and a B2 application key for the operator (see [Operator key](#operator-key)).

```sh
# 1. Install the operator (CRDs are installed by the chart). Releases publish
#    the chart to oci://ghcr.io/backblaze-b2-samples/charts/b2-kubernetes-operator;
#    from a checkout:
helm install b2-operator charts/b2-kubernetes-operator \
  --namespace b2-operator-system --create-namespace

# 2. Give it credentials for your B2 account.
kubectl -n b2-operator-system create secret generic b2-credentials \
  --from-literal=applicationKeyId=<keyID> --from-literal=applicationKey=<key>
kubectl apply -f config/samples/00-provider-config.yaml
kubectl get clusterproviderconfig default   # READY should be True

# 3. Allow a namespace to use B2. Nothing is allowed until a policy says so.
kubectl apply -f config/samples/01-access-policy.yaml
kubectl create namespace shop
kubectl label namespace shop b2.backblaze.com/tenant=true

# 4. Create a bucket and a key.
kubectl apply -f config/samples/02-bucket.yaml -f config/samples/03-application-key.yaml
kubectl -n shop get b2bucket,b2key
```

Use the key from a workload:

```yaml
envFrom:
  - secretRef:
      name: uploads-writer   # AWS_* and B2_* variables
```

## Resources

| Kind | Scope | Who creates it | Purpose |
| --- | --- | --- | --- |
| `ClusterProviderConfig` | Cluster | Cluster admin | Credentials for one B2 account |
| `B2AccessPolicy` | Cluster | Cluster admin | What each namespace may do |
| `Bucket` | Namespace | App team | A B2 bucket |
| `ApplicationKey` | Namespace | App team | A scoped key, delivered to a Secret |
| `B2Account` | Cluster | Platform | A customer account created through the Partner API |

`kubectl explain bucket.spec` documents every field. More examples are in [config/samples](config/samples).

### Secret contents

| Key | Value |
| --- | --- |
| `AWS_ACCESS_KEY_ID`, `B2_APPLICATION_KEY_ID` | Application key ID |
| `AWS_SECRET_ACCESS_KEY`, `B2_APPLICATION_KEY` | Application key |
| `AWS_ENDPOINT_URL` | S3 endpoint for the account, e.g. `https://s3.us-west-004.backblazeb2.com` |
| `AWS_REGION` | e.g. `us-west-004` |
| `B2_BUCKET_NAME` | Bucket the key is restricted to, if any |
| `B2_NAME_PREFIX` | File name prefix the key is restricted to, if any |

The Secret is owned by the `ApplicationKey`. The operator never overwrites or deletes a Secret it does not own.

## Security model

Read [docs/security.md](docs/security.md) before running this on a shared cluster. In short:

- **Tenants never see the operator's credentials.** They only receive keys that a policy allows.
- **Default deny.** With no matching `B2AccessPolicy`, every Bucket and ApplicationKey is refused with reason `PolicyDenied`.
- **Key-management capabilities are blocked.** `listKeys`, `writeKeys` and `deleteKeys` would let a tenant escalate to the operator's own power, so the operator refuses them unless it runs with `--allow-key-management-capabilities`.
- **Tightening a policy revokes keys it no longer allows** after their grace period. A brief gap, such as a policy being replaced, does not break workloads. Disable with `--revoke-on-policy-violation=false`.
- **No credentials outlive their resource.** A Bucket cannot be deleted while ApplicationKeys reference it. Keys whose resource was force-deleted (finalizer removed) are revoked by a periodic sweep.
- **Ownership is recorded in B2.** Each bucket's `bucketInfo` holds its owner's resource UID (`b2operator-owner-uid`), so two resources (or two clusters) can never fight over one bucket. A retained bucket records the namespace that released it, and only that namespace can adopt it again.
- **Namespace admins and editors can manage Buckets and ApplicationKeys** through aggregated roles. `B2AccessPolicy` and `ClusterProviderConfig` are for cluster admins only.

### Operator key

The operator's key needs `listBuckets`, `readBuckets`, `writeBuckets`, `deleteBuckets`, `listKeys`, `writeKeys` and `deleteKeys`. It also needs `read/writeBucketEncryption` and `read/writeBucketRetentions` for encryption and Object Lock, Give it every capability it will grant to tenant keys, too. The provider config's `Ready` condition message warns if core capabilities are missing.

## Configuration

Operator flags (Helm values under `operator.*`):

| Flag | Default | Description |
| --- | --- | --- |
| `--resync-period` | `10m` | How often resources are compared with B2 to correct drift |
| `--key-verify-interval` | `1h` | How often each key is checked against B2 (replaced if revoked) |
| `--default-grace-period` | `15m` | How long a replaced key stays valid |
| `--revoke-on-policy-violation` | `true` | Revoke keys that policies no longer allow |
| `--allow-key-management-capabilities` | `false` | Let policies grant `listKeys`/`writeKeys`/`deleteKeys` |
| `--orphan-key-sweep-interval` | `1h` | Revoke keys this cluster created for ApplicationKeys that no longer exist |
| `--cluster-id` | from `kube-system` UID | ID embedded in B2 key names; must differ between clusters sharing an account |
| `--allow-insecure-api-url` | `false` | Allow `http://` API URLs (testing only) |

**Cost:** each resync of a Bucket is one `b2_list_buckets` call, and each key verification is one `b2_authorize_account` call. Both are Class C transactions. With the defaults, 100 buckets and 100 keys make about 16,800 calls a day. Raise the intervals if that matters to you.

## Operations

- **Status:** every resource has a `Ready` condition with a specific reason (`PolicyDenied`, `BucketNameUnavailable`, `DeletionBlocked`, `SecretConflict` and others) and a human-readable message. See [docs/troubleshooting.md](docs/troubleshooting.md).
- **Events:** creation, rotation, revocation, adoption and drift correction are recorded as Kubernetes events.
- **Metrics:** `b2_operator_api_requests_total{operation,code}` and `b2_operator_api_request_duration_seconds{operation}`, plus the standard controller-runtime metrics. They're served on an authenticated HTTPS endpoint; enable `metrics.serviceMonitor.enabled` for Prometheus Operator.
- **Forcing a resync:** change any annotation on the resource.
- **Deletion when B2 is unreachable:** the finalizer waits. To abandon the B2 resource, remove the finalizer `b2.backblaze.com/finalizer`.

## Development

```sh
make test        # unit + envtest integration tests against an in-memory fake B2
make lint
make test-e2e    # builds images, installs the chart into kind, runs end-to-end tests
make run         # run against your current kubeconfig
```

The B2 client (`internal/b2`) is a small, dependency-free client for the Native API v4. It re-authorizes when tokens expire and retries idempotent calls with backoff, honouring `Retry-After`. It never retries creates. `internal/b2/b2fake` is an in-memory B2 used by every test layer. See [docs/architecture.md](docs/architecture.md) and [CONTRIBUTING.md](CONTRIBUTING.md).

## Building a storage product on it

- [docs/partner.md](docs/partner.md): per-customer, per-region accounts through the Partner API.
- [docs/integration.md](docs/integration.md): a "new bucket" button, multi-region buckets, tenant offboarding, and where to run the operator.

## Migrating from `mgruszkiewicz/backblaze-operator`

See [docs/migration.md](docs/migration.md).

## License

Apache 2.0. See [LICENSE](LICENSE).
