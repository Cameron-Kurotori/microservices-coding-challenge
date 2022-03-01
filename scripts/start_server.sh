#!/usr/bin/env bash

if ! docker pod exists distqueue-pod; then
    docker pod create --replace --name distqueue-pod --publish 2345:2345,6789:6789
fi

docker run --rm --pod distqueue-pod distqueue-server