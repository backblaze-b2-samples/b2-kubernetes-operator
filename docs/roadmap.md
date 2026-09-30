# Roadmap

| Milestone | Scope |
| --- | --- |
| v1alpha1 (this release) | ClusterProviderConfig, B2AccessPolicy, Bucket (type, info, lifecycle, CORS, SSE-B2, Object Lock), ApplicationKey (scoping, expiry, rotation, zero-downtime replacement) |
| Next | Event notification rules; bucket access logging; `ReplicationRule` for Cloud Replication; policy status (which namespaces a policy selects) |
| Later | COSI driver built on the same client; OLM bundle and OperatorHub listing |
| v1 | API freeze with a conversion webhook, documented support policy |

Open questions are tracked as GitHub issues.
