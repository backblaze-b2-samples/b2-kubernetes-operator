# Provisioning customer accounts with the Partner API

Hosting providers and resellers can give each customer their own B2 account, one per region, through the [Backblaze Partner API](https://www.backblaze.com/apidocs/introduction-to-the-partner-api). Separate accounts are a stronger boundary than prefix-scoped keys in a shared account: a misconfigured key can only ever reach one customer's data.

## Prerequisites

- **Partner API access.** Backblaze sales enables the Partner API for committed-contract customers. A `ClusterProviderConfig` with `spec.partner` whose account is not enabled reports `PartnerAPINotEnabled`.
- **A Group** with B2 enabled, managed by your admin account. See [Create a Group for the Partner API](https://www.backblaze.com/docs/cloud-storage-create-a-group-for-the-partner-api).
- **The Group admin's master application key**, stored in a Secret in the operator's namespace. The Partner API requires the master key; a partner config holding an application key reports `PartnerRequiresMasterKey`. A partner config is used only to create and eject accounts: Buckets and ApplicationKeys cannot reference it.

## How it fits together

```
ClusterProviderConfig "partner"         Group admin credentials + email template
        │
        ▼
B2Account "customer-a-west"              one per customer per region
        │  b2_create_group_member
        ├─► Secret b2-account-customer-a-west   the key B2 returned (kept, unused) + the operator's application key
        ├─► ClusterProviderConfig "customer-a-west"
        └─► B2AccessPolicy "b2account-customer-a-west"  (from spec.access)
                │
                ▼
Bucket / ApplicationKey in the customer's namespaces, with providerConfigRef: customer-a-west
```

Buckets, lifecycle rules, Object Lock, encryption, replication and scoped keys all work the same in a customer account as anywhere else.

## Accounts

```yaml
apiVersion: b2.backblaze.com/v1alpha1
kind: B2Account
metadata:
  name: customer-a-west
spec:
  partnerConfigRef: {name: partner}
  customer: customer-a       # substituted for {customer}
  region: us-west            # us-east, us-west, ca-east or eu-central
  credentialsSecretRef:
    namespace: b2-operator-system
```

- **Email.** B2 requires a unique email address per account, but never mails it. The operator generates it from the partner's `memberEmailTemplate`, for example `{customer}-{region}@hosting-company.com` gives `customer-a-us-west@hosting-company.com`. The address is fixed at creation and recorded in `status.email`. Use a domain the partner owns: B2 refuses addresses on some domains, `backblaze.com` included, with `420 method_failure` and creates nothing, so the resource stays `Ready=False` with `ProviderError`. A second `B2Account` for the same customer and region is refused with `AccountConflict`.
- **Credentials.** `b2_create_group_member` returns the account's application key exactly once. The operator writes it to the credentials Secret before doing anything else. If that write fails, the key is held in memory and the write is retried until it succeeds; the key is never logged or put in status. The operator never overwrites a Secret that already holds a key it did not write, and never deletes a credentials Secret, whatever the deletion policy.
- **Account key vs. operations key.** The key B2 returns for a new account is an all-capabilities application key, not the account's master key; the master key is never handed out. It is stored as is, under `applicationKeyId`/`applicationKey`. The operator then uses it once to create its own application key in the account, stored next to it as `operationsKeyId`/`operationsKey`. The account's provider config uses the operations key for all bucket and key management, so the account key never serves day-to-day traffic. If the operations key is deleted in B2, a new one is created. On eject it is revoked; the account key is kept.
- **Existing accounts.** If a Group member with the generated email already exists, the resource reports `CredentialsMissing` until its key is stored in the Secret and `spec.adoptExisting: true` is set. The operator checks that the key belongs to that account before using it.
- **Customer key hygiene.** Workloads should get scoped `ApplicationKey`s, not the account key. The account key stays in the operator's namespace.

## Multi-region buckets

A multi-region bucket is two per-region accounts, a bucket in each, and a Cloud Replication rule:

```yaml
kind: Bucket
metadata: {name: datasets, namespace: customer-a}
spec:
  providerConfigRef: {name: customer-a-west}
  bucketName: customer-a-datasets-west
  replication:
    - name: to-east
      destinationBucketRef: {name: datasets-east}   # a Bucket using customer-a-east
      includeExistingFiles: true
```

The operator does the following:
- Creates a read key in the source account and a write key in the destination account, with the capabilities Cloud Replication requires.
- Registers the destination key on the destination bucket.
- Sets the rule on the source bucket.
- Revokes both keys when the rule is removed.

A bucket that another Bucket replicates into can't be deleted until the rule is removed. Replication must be allowed by policy (`buckets.allowReplication`). The full example is [config/samples/07-multi-region-bucket.yaml](../config/samples/07-multi-region-bucket.yaml).

## Deleting accounts

The Partner API cannot delete B2 accounts. Deleting a `B2Account` is blocked (`DeletionBlocked`) while any Bucket or ApplicationKey still uses its provider config. After that:

| `deletionPolicy` | Effect |
| --- | --- |
| `Retain` (default) | The account stays in the Group with its data. The generated provider config is removed. The credentials Secret is kept. |
| `Eject` | The account is removed from the Group (`b2_eject_group_member`). It keeps existing, with its data, billed on its own. The credentials Secret is kept and annotated `b2.backblaze.com/ejected-at`. |

See [docs/integration.md](integration.md#offboarding-a-tenant) for the full offboarding procedure.
