#!/bin/sh

TARGET_HOST=$(printenv CONTAINER_HOST|sed 's-tcp://--g'|cut -f1 -d:)
REV=$(git describe --long --tags --match='v*' --dirty 2>/dev/null || echo dev)


docker build -f docker/build.Dockerfile -t kazimsarikaya/sibatuu:$REV . ||exit 1
docker tag kazimsarikaya/sibatuu:$REV kazimsarikaya/sibatuu:dev-latest
