FROM golang:1.26 AS builder
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
