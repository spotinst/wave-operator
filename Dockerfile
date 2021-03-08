# Build the manager binary
FROM golang:1.16 as builder

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY main.go main.go
COPY VERSION VERSION
COPY admission/ admission/
COPY api/ api/
COPY catalog/ catalog/
COPY cloudstorage/ cloudstorage/
COPY controllers/ controllers/
COPY install/ install/
COPY internal/ internal/

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build \
 -ldflags "-X github.com/spotinst/wave-operator/internal/version.BuildVersion=$(cat VERSION) -X github.com/spotinst/wave-operator/internal/version.BuildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
 -a -o manager main.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
USER nonroot:nonroot

ENTRYPOINT ["/manager"]
