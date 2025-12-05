ARG ALPINE_VERSION=3.22
ARG GOLANG_VERSION=1.25.3

# FROM alpine:${ALPINE_VERSION}
FROM golang:${GOLANG_VERSION}-alpine${ALPINE_VERSION}

ARG PROTOC_GEN_GO_VERSION=1.36.10
ARG PROTOC_GEN_GO_GRPC_VERSION=1.5.1

RUN apk add git bash protoc make curl uv
RUN apk add --no-cache python3 py3-pip
RUN uv tool install grpcio-tools
ENV PATH="$PATH:/root/.local/bin/"

# https://grpc.io/docs/languages/go/quickstart/#prerequisites
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v${PROTOC_GEN_GO_VERSION} && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v${PROTOC_GEN_GO_GRPC_VERSION}

# https://golangci-lint.run/docs/welcome/install/#binaries
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/v2.7.0/install.sh -o /tmp/install-golangci-lint.sh && \
    chmod +x /tmp/install-golangci-lint.sh && \
    /tmp/install-golangci-lint.sh -b $(go env GOPATH)/bin v2.7.0 && \
    rm /tmp/install-golangci-lint.sh

RUN rm -rf ~/.cache
