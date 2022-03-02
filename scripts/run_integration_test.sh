#!/usr/bin/env bash

pushd .. || exit 1

module="github.com/Cameron-Kurotori/microservices-coding-challenge"
client_pkg="${module}/client"
server_pkg="${module}/server"
proto_pkg="${module}/proto/distqueue"
go test integration_test.go --coverpkg="${client_pkg},${server_pkg},${proto_pkg}" --coverprofile=integration.cov
go tool cover -html="integration.cov"