#!/bin/sh
# Generates the Python stubs of the NRI (implementation §3) next to the library,
# with the grpcio-tools of requirements-codegen.txt.
set -eu
cd "$(dirname "$0")/../.."
rm -rf sdk/python/src/zobik/nri
python3 -m grpc_tools.protoc -I proto \
  --python_out=sdk/python/src --pyi_out=sdk/python/src --grpc_python_out=sdk/python/src \
  proto/zobik/nri/v1/nri.proto
