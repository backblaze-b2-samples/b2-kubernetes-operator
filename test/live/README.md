# Live tests against real Backblaze B2

These tests run the operator's controllers against a real Kubernetes API server (envtest) and the **real B2 API**. They check every result by reading B2 directly. They answer the questions the API docs leave open, and catch anything the in-memory fake gets wrong.

They are opt-in: without credentials every test is skipped.

```sh
export B2_LIVE_KEY_ID=...    # application key ID (not the master key)
export B2_LIVE_KEY=...
make test-live
```

## What runs

| Test | Checks in B2 |
| --- | --- |
| `TestLiveBucketSettings` | Bucket info, lifecycle, CORS and SSE-B2 (the default) are applied, and can be cleared and turned off. Out-of-band changes are reverted. B2's representation of "no encryption" is recognised as in sync. |
| `TestLiveObjectLock` | Object Lock and default retention are enabled, changed and cleared. B2's representation of "no retention" is recognised as in sync. |
| `TestLiveApplicationKeyLifecycle` | Delivered keys authorize with exactly the requested bucket, prefix, capabilities and expiry. Rotation keeps the old key valid for the grace period, then revokes it. Deletion revokes. Also records how `b2_delete_key` answers for a key that no longer exists. |
| `TestLiveReplication` | Replication rules, the source key and the destination key mapping are set up, then cleared and revoked when the rule is removed. Skipped if the account cannot use Cloud Replication (it needs a verified email and payment history). |
| `TestLivePartnerAccount` | A real Partner API account is created, managed through an operator application key, holds a bucket, and is optionally ejected. Off unless enabled (see below). Also records whether `b2_create_group_member` returns the account's master key. |

After resources become ready, most tests resync twice and assert that the bucket's revision in B2 did not change. If the operator's idea of a setting differed from how B2 reports it, it would rewrite the bucket on every resync, and this catches it.

Open questions are answered in the test log; run with `-v` (the Makefile does) and look for lines starting `B2 reports` and `b2_`.

`readBucketReplications` and `writeBucketReplications` are not listed in the v4 `b2_create_key` reference, but B2 accepts them and requires them to read and change replication settings. This was confirmed by this suite against the live API.

## B2 behaviour confirmed by this suite

These were checked against the live API (October 2026). Where they differ from the API reference, the operator and the fake in `internal/b2/b2fake` follow the live behaviour.

| Behaviour | Live API |
| --- | --- |
| Updating `replicationConfiguration` | **Replaces the whole configuration.** A side that is omitted or `null` is removed, contrary to the reference. The operator always sends both sides. |
| Clearing replication | An empty rule list (`replicationRules is empty`) and an empty key mapping (`sourceToDestinationKeyMapping is empty`) are rejected. Omit the side instead. `{}` clears everything. |
| Replication rule names | At least 6 characters. |
| Reading/changing replication settings | Needs `readBucketReplications` / `writeBucketReplications`. These are not in the v4 `b2_create_key` reference, but are accepted. |
| Default encryption turned off | Reported as `{"mode": null, "algorithm": null}`. |
| Object Lock default retention cleared | Reported as `{"mode": null, "period": null}`. |
| `b2_delete_key` for a key that no longer exists | Succeeds. |

## Credentials

Use a **dedicated test account with a spend cap**. The key needs:

```
listBuckets readBuckets writeBuckets deleteBuckets
listKeys writeKeys deleteKeys
readBucketEncryption writeBucketEncryption
readBucketRetentions writeBucketRetentions
listFiles readFiles writeFiles deleteFiles
readFileLegalHolds writeFileLegalHolds readFileRetentions writeFileRetentions
readBucketReplications writeBucketReplications
```

With the B2 CLI:

```sh
b2 key create b2-operator-live-tests listBuckets,readBuckets,writeBuckets,deleteBuckets,listKeys,writeKeys,deleteKeys,readBucketEncryption,writeBucketEncryption,readBucketRetentions,writeBucketRetentions,listFiles,readFiles,writeFiles,deleteFiles,readFileLegalHolds,writeFileLegalHolds,readFileRetentions,writeFileRetentions,readBucketReplications,writeBucketReplications
```

## What it creates and cleans up

- **Buckets.** Every bucket is named `b2op-live-<random>-…` and deleted at the end; the buckets hold no files.
- **Keys.** Every key is named `b2op-lv<random>-…` and revoked at the end.
- **Leftovers.** If a test fails part-way, a final sweep deletes anything with this run's prefix. Anything left after an interrupted run (Ctrl-C) can be found by those prefixes.

The cost is a few hundred API transactions per run.

## Partner API test

This creates a **real B2 account in your Group, which cannot be deleted**. It only runs with every variable below set:

```sh
export B2_LIVE_PARTNER_CREATE_ACCOUNTS=yes
export B2_LIVE_PARTNER_KEY_ID=...          # Group admin's MASTER key ID (equals its account ID)
export B2_LIVE_PARTNER_KEY=...
export B2_LIVE_PARTNER_GROUP_ID=...
export B2_LIVE_PARTNER_EMAIL_DOMAIN=...    # e.g. a test domain you control
export B2_LIVE_PARTNER_REGION=us-west      # optional
export B2_LIVE_PARTNER_EJECT=yes           # optional: eject the account at the end
```

The account email is `b2op-live-<random>-<region>@<domain>`. Without `B2_LIVE_PARTNER_EJECT=yes` the account stays in the Group.

## Other settings

- `B2_LIVE_API_URL` overrides the authorization endpoint.
- To dry-run the suite itself without B2, point it at the fake: `go run ./cmd/b2fake --listen 127.0.0.1:18080 --advertise-url http://127.0.0.1:18080 --master-key-id acct --master-key key --group-id grp`, then set `B2_LIVE_API_URL=http://127.0.0.1:18080` and use `acct`/`key`/`grp` as the credentials.
