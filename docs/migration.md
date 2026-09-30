# Migrating from mgruszkiewicz/backblaze-operator

The community operator (`b2.issei.space/v1alpha2`) and this operator (`b2.backblaze.com/v1alpha1`) use different API groups, so both can run side by side during migration.

| Community | This operator |
| --- | --- |
| `Bucket.metadata.name` is the B2 bucket name | `Bucket.spec.bucketName` |
| `spec.atProvider.acl: private \| public` | `spec.bucketType: allPrivate \| allPublic` |
| `spec.atProvider.bucketLifecycle` | `spec.lifecycleRules` |
| Deleting the resource deletes the bucket | `spec.deletionPolicy`, default `Retain` |
| `Key.spec.atProvider.bucketName` | `ApplicationKey.spec.bucketRef` (a Bucket resource) or `spec.bucketName` |
| `Key.spec.atProvider.namePrefix`, `validDurationInSeconds` (ignored by the community operator) | `spec.namePrefix`, `spec.validFor` (enforced) |
| `writeConnectionSecretToRef.name`, default `b2-secret` | `spec.secretName`, default the resource name |
| Secret keys `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `bucketName`, `endpoint`, `keyName` | `AWS_*` as before, plus `AWS_ENDPOINT_URL`, `AWS_REGION`, `B2_*` |
| Credentials from env vars on the operator | `ClusterProviderConfig` |
| No tenancy controls | `B2AccessPolicy` (required) |

## Steps

1. Install this operator and create a `ClusterProviderConfig` and a `B2AccessPolicy`. The policy must allow adoption and the existing bucket names.
2. For each bucket, create a `Bucket` with `adoptExisting: true` and the same lifecycle rules. Wait for `Ready`. Adoption replaces lifecycle rules, CORS rules and bucket info with the spec, so copy them over first.
3. Before deleting the old `Bucket`, remove the community operator's finalizer (`bucket.b2.issei.space/finalizer`) from it. Otherwise deleting the old resource would try to delete the bucket.
4. For each key, create an `ApplicationKey` with a **new** Secret name, and switch workloads to it. Keys cannot be adopted: B2 only returns a key's secret at creation.
5. Delete the old `Key` resources. This revokes the old keys and deletes their Secrets.
6. Uninstall the community operator and its CRDs.
