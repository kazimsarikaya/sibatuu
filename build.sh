#!/bin/sh

cmd=${1:-build}

if [ "x$cmd" == "xbuild" ]; then
  REV=$(git describe --long --tags --match='v*' --dirty 2>/dev/null || git rev-list -n1 HEAD)
  NOW=$(date +'%Y-%m-%d_%T')
  GOV=$(go version)
  go mod tidy
  go mod vendor
  for proto in $(find internal -name *.proto); do
    protoc --experimental_allow_proto3_optional -I $(dirname $proto) --go_out=$(dirname $proto) $(basename $proto)
  done
  go build -ldflags "${LDFLAGS} -X main.version=$REV -X main.buildTime=$NOW -X 'main.goVersion=${GOV}'"  -o ./bin/backup ./cmd
elif [ "x$cmd" == "xtest" ]; then
  shift
  ./test.sh $@
else
  echo unknown command
fi
