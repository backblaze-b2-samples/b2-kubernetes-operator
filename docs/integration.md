# Integrating with a provider platform

This guide covers building self-service storage for a hosting or GPU cloud platform on the operator: a "new bucket" button, a "multi-region" option, and tenant offboarding.

## Creating buckets from a UI

The UI (or its backend) creates Kubernetes objects; it never calls the B2 API and never holds B2 credentials. The operator does the B2 work, enforces policy, and reports the result in status.

1. **Grant the UI's service account** `create`, `get`, `list`, `watch` and `delete` on `buckets` and `applicationkeys` in the tenant's namespace. The chart's aggregated `edit` role already covers this.
2. **"New bucket"** creates a `Bucket`:

   ```json
   {
     "apiVersion": "b2.backblaze.com/v1alpha1",
     "kind": "Bucket",
     "metadata": {"name": "datasets", "namespace": "customer-a"},
     "spec": {"providerConfigRef": {"name": "customer-a-west"}, "bucketName": "customer-a-datasets-west"}
   }
   ```

   SSE-B2 encryption, `allPrivate` and `deletionPolicy: Retain` are the defaults.
3. **Watch** `status.conditions[type=Ready]`:
   - `True`: show the bucket.
   - `False`: show the condition's `message`. It is written for humans, for example `bucket name "x" is already taken by another B2 account; bucket names are global, so choose another`.

   Bucket names are global across all of B2, so let users retry with another name on `BucketNameUnavailable`.
4. **Credentials for workloads** come from an `ApplicationKey` with `bucketRef` pointing at the bucket. Mount the resulting Secret into workloads.

## The "multi-region bucket" checkbox

When the box is ticked, create:

1. `B2Account`s for the customer in both regions, if they don't exist yet (platform side, see [partner.md](partner.md)).
2. The destination `Bucket` in the second region's account.
3. The source `Bucket` in the first region's account, with a `replication` rule pointing at the destination.

The source becomes `Ready` once replication is configured in both accounts. See [config/samples/07-multi-region-bucket.yaml](../config/samples/07-multi-region-bucket.yaml).

## Offboarding a tenant

Nothing is deleted from B2 just because a resource was removed. Every destructive step is an explicit choice.

| Step | What happens | Data |
| --- | --- | --- |
| Delete the tenant's `ApplicationKey`s (or its namespace) | Keys are revoked in B2 and their Secrets deleted. Keys whose resource was force-deleted are revoked by the periodic sweep. | Untouched |
| Delete the tenant's `Bucket`s | With `deletionPolicy: Retain` (default), the bucket stays in B2 and is released. Replication keys are revoked. Deletion waits until no keys or replication rules reference the bucket. | Kept |
| To actually delete a bucket | Set `deletionPolicy: Delete` (policy must allow it) and empty the bucket. B2 refuses to delete non-empty buckets, and the resource reports `DeletionBlocked` until it is empty. | Deleted only when you have emptied it |
| Delete the tenant's `B2Account`s | Blocked until no Buckets or ApplicationKeys use the account. `Retain` leaves the account in your Group; `Eject` hands it off as an independent account. The account key Secret is kept either way. | Kept (accounts cannot be deleted through the API) |

Deleting a whole namespace is safe: finalizers run in dependency order (keys, then buckets), and buckets are retained by default.

## Where the operator runs

The operator manages Kubernetes resources in the cluster it runs in, and writes Secrets to that cluster. There are two ways to fit it into a platform where each customer has their own cluster:

| Topology | How it works | Trade-offs |
| --- | --- | --- |
| **Central** (one operator in the provider's management cluster) | The platform creates `B2Account`, `Bucket` and `ApplicationKey` objects in per-customer namespaces of the management cluster. Credentials are then delivered into each customer cluster, either by the platform or with a secrets sync tool (External Secrets Operator `PushSecret`, or a similar tool). | Partner admin credentials live in one place. Delivering Secrets into customer clusters is a separate step. |
| **Per customer cluster** (an operator in each customer cluster) | Each customer cluster gets a `ClusterProviderConfig` for that customer's account only, and Secrets land in the customer's own cluster directly. Accounts are still created centrally (a central operator with the partner config, or the platform). | No cross-cluster delivery. The customer's account key sits in the customer's cluster, and there are more operator installs to run. |

The Partner admin key should never be installed in customer clusters. Native delivery of Secrets into remote clusters (a central operator writing directly into customer clusters) is on the [roadmap](roadmap.md). The right choice depends on who operates the customer clusters and how credentials reach them today.
