ARG ALPINE_VERSION=3.22
ARG GOLANG_VERSION=1.25.3

# FROM alpine:${ALPINE_VERSION}
FROM golang:${GOLANG_VERSION}-alpine${ALPINE_VERSION}

ARG PROTOC_GEN_GO_VERSION=1.36.10
ARG PROTOC_GEN_GO_GRPC_VERSION=1.5.1

RUN apk add git bash protoc make uv
RUN uv tool install grpcio-tools
ENV PATH="$PATH:/root/.local/bin/"

# https://grpc.io/docs/languages/go/quickstart/#prerequisites
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v${PROTOC_GEN_GO_VERSION} && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v${PROTOC_GEN_GO_GRPC_VERSION}

RUN rm -rf ~/.cache
