# Pinned to the patch release of the go directive in go.mod, which is also the
# toolchain CI tests and scans with. The golang images set GOTOOLCHAIN=local,
# so this tag alone decides which standard library ships in the binary; a
# floating tag would build with whatever release is current on release day.
# Nothing bumps this pin automatically: move it together with the go directive
# on every Go patch release (cmd/controller's TestImageBuilderMatchesGoDirective
# fails when the two diverge).
FROM golang:1.26.8 AS builder
WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY pkg/ pkg/
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o karpenter-clevercloud ./cmd/controller

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/karpenter-clevercloud .
USER 65532:65532
ENTRYPOINT ["/karpenter-clevercloud"]
