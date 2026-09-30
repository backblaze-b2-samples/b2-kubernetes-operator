# Contributing

## Setup

You need Go (the version in `go.mod`), Docker, kind, kubectl and Helm. Tools such as `controller-gen` and `setup-envtest` are installed into `./bin` by the Makefile.

```sh
make test       # unit + integration tests (downloads envtest binaries on first run)
make lint       # golangci-lint
make test-e2e   # kind cluster + real image + Helm chart
```

## Changing the API

1. Edit the types in `api/v1alpha1`, including kubebuilder validation markers. Prefer CEL rules for cross-field validation.
2. Run `make generate manifests`. This regenerates deepcopy code, the CRDs, the manager ClusterRole, and the chart copies of both.
3. Commit the generated files. CI fails if they are stale (`make verify`).

## Changing reconcile behavior

- Every B2 mutation must be preceded by a policy check.
- Never retry non-idempotent B2 calls blindly. Make them recoverable instead (see the ownership tag and `pendingKeyName` in [docs/architecture.md](docs/architecture.md)).
- Add an integration test in `internal/controller`. The fake B2 in `internal/b2/b2fake` supports fault injection (`InjectFault`, including `AfterApply` for lost responses) and out-of-band changes (`MutateBucket`, `DeleteKeyDirect`).
- If you rely on new B2 behavior, add it to the fake with the documented validation rules.

## Pull requests

Keep changes focused. Describe user-visible changes in `CHANGELOG.md`. All CI checks must pass.
