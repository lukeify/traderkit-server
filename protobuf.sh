#!/bin/zsh
setopt +o nomatch
PROTOC_GEN_GO="$(go env GOPATH)/bin/protoc-gen-go"
PROTOC_GEN_GO_GRPC="$(go env GOPATH)/bin/protoc-gen-go-grpc"

protoc \
  --plugin=protoc-gen-go="$PROTOC_GEN_GO" \
  --plugin=protoc-gen-go-grpc="$PROTOC_GEN_GO_GRPC" \
  --go_out=. \
  --go-grpc_out=. \
  "$@"