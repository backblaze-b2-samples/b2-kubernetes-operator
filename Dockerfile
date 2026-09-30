# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
      -ldflags "-s -w -X github.com/backblaze-b2-samples/b2-operator/internal/version.Version=${VERSION} -X github.com/backblaze-b2-samples/b2-operator/internal/version.Commit=${COMMIT}" \
      -o /out/manager ./cmd/manager && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w" -o /out/b2fake ./cmd/b2fake

# Test-only image serving the fake B2 API (used by the kind e2e suite).
FROM gcr.io/distroless/static-debian12:nonroot AS b2fake
COPY --from=build /out/b2fake /b2fake
USER 65532:65532
ENTRYPOINT ["/b2fake"]

FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.source="https://github.com/backblaze-b2-samples/b2-operator" \
      org.opencontainers.image.description="Kubernetes operator for Backblaze B2" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /out/manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
