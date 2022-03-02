#!/usr/bin/env bash

pushd .. || exit 1

protoc --go_out=. --go-grpc_out=. -I proto/distqueue/ proto/distqueue/distqueue.proto
mockgen --source proto/distqueue/distqueue_grpc.pb.go > proto/distqueue/mock_distqueue/mock_distqueue.go
protoc --go_out=. --go-grpc_out=. -I proto/item/ proto/item/item.proto
