#!/usr/bin/env sh

if [ -n "$1" ] && [ ${0:0:4} = "/bin" ]; then
  ROOT_DIR=$1/..
else
  ROOT_DIR="$( cd "$( dirname "$0" )" && pwd )/.."
fi

PROTOTOOL_IMAGE=p1hub/prototool
PROTOTOOL_IMAGE_TAG=latest
PROTO_GEN_PATH=${ROOT_DIR}/pkg/proto