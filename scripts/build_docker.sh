#!/usr/bin/env bash

docker build --rm  -t distqueue-cli -f ../Dockerfile .. && \
docker build --rm -t distqueue-client -f ../Dockerfile.client .. && \
docker build --rm -t distqueue-server -f ../Dockerfile.server ..