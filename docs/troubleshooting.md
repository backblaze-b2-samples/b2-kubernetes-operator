# Troubleshooting

Start with `kubectl describe` on the resource. The `Ready` condition's reason and message say what's wrong, and events show what the operator did.

| Reason | Resource | Meaning and fix |
| --- | --- | --- |
| `ProviderConfigNotReady` | Bucket, ApplicationKey | The referenced `ClusterProviderConfig` is missing or not `Ready`. Check it with `kubectl describe clusterproviderconfig <name>`. |
| `CredentialsSecretNotFound` | ClusterProviderConfig | The Secret, or its `applicationKeyId`/`applicationKey` entries, are missing. |
| `InvalidCredentials` | ClusterProviderConfig | B2 rejected the key: it was deleted, expired, or mistyped. Update the Secret; it is re-read within `--resync-period`, or immediately if you edit the provider config. |
| `PolicyDenied` | Bucket, ApplicationKey | No `B2AccessPolicy` allows the request. The message names each policy that selects the namespace and why it refused. Check namespace labels and the policy. |
| `BucketNameUnavailable` | Bucket | Bucket names are global across all B2 accounts, and this one is taken. Choose another name. |
| `BucketAlreadyExists` | Bucket | The bucket exists in your account but is not managed by this resource. Set `spec.adoptExisting: true` if a policy allows adoption. |
| `BucketOwnedElsewhere` | Bucket | Another resource (possibly in another cluster) owns the bucket. To release it, delete that resource with `Retain`, or remove the `b2operator-owner-uid` bucket info entry in B2. |
| `DeletionBlocked` | Bucket | `deletionPolicy: Delete`, but the bucket still has file versions. Empty it, or set `deletionPolicy: Retain`. |
| `BucketNotReady` | ApplicationKey | The referenced `Bucket` is missing or not `Ready` yet. The key proceeds automatically when it is. |
| `BucketNotFound` | ApplicationKey | `spec.bucketName` names a bucket that does not exist in the account. |
| `SecretConflict` | ApplicationKey | A Secret with the target name exists and is not owned by this key. The operator will not overwrite it. Delete it or set `spec.secretName`. |
| `InvalidSpec` | ApplicationKey | For example, the key and its Bucket use different provider configs. |
| `ProviderError` | any | B2 returned an error. The message includes the B2 error code. Transient errors are retried with backoff. |

## Common questions

**A resource is stuck in Terminating.** The finalizer is waiting to clean up in B2, for example because the provider config is broken or the bucket is not empty. Fix the cause, or remove the finalizer `b2.backblaze.com/finalizer` to abandon the B2 resource.

**My application still uses an old key after rotation.** Environment variables from a Secret are read at pod start. Use a controller such as [Reloader](https://github.com/stakater/Reloader) (see `spec.secretTemplate.annotations`), or mount the Secret as a volume and re-read it. Set `rotation.gracePeriod` longer than your rollout takes.

**How do I force a resync?** Change any annotation on the resource:

```sh
kubectl annotate bucket uploads b2.backblaze.com/resync="$(date +%s)" --overwrite
```

**Debug logging.** Set the Helm value `operator.logLevel=debug`.
