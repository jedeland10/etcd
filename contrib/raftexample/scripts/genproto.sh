#!/usr/bin/env bash
#
# Generate Protobuf bindings for protostore/kv.proto.
# Run from the repository root directory.
#
set -e  # Exit immediately if a command fails
shopt -s globstar  # Enable ** wildcard

if ! [[ "$0" =~ scripts/genproto.sh ]]; then
  echo "ERROR: Must be run from repository root"
  exit 255
fi

source ./scripts/test_lib.sh  # Ensure test_lib.sh exists in scripts/

# Verify correct protoc version
if [[ $(protoc --version | cut -f2 -d' ') != "3.20.3" ]]; then
  echo "ERROR: protoc 3.20.3 not found! Install and add to PATH."
  exit 255
fi

# Find required binaries and paths
GOFAST_BIN=$(tool_get_bin github.com/gogo/protobuf/protoc-gen-gofast)
GOGOPROTO_ROOT="$(tool_pkg_dir github.com/gogo/protobuf/proto)/.."
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"

echo
echo "Resolved binary and package versions:"
echo "  - protoc-gen-gofast: ${GOFAST_BIN}"
echo "  - gogoproto-root:    ${GOGOPROTO_ROOT}"

# Directories containing `.proto` files to be built
DIRS="./protostore"

log_callout -e "Running gofast (gogo) Protobuf generation for protostore..."

for dir in ${DIRS}; do
  pushd "${dir}" > /dev/null

  protoc --gofast_out=. -I=".:${GOGOPROTO_PATH}:${PWD}/.." \
    --plugin="${GOFAST_BIN}" ./**/*.proto

  # Fix incorrect import paths (if necessary)
  sed -i.bak -E 's|"protostore"|"go.etcd.io/etcd/v3/protostore"|g' ./**/*.pb.go
  sed -i.bak -E 's|"google/protobuf"|"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"|g' ./**/*.pb.go

  rm -f ./**/*.bak  # Clean up temporary files

  # Format generated Go code
  gofmt -s -w ./**/*.pb.go
  run_go_tool "golang.org/x/tools/cmd/goimports" -w ./**/*.pb.go

  popd > /dev/null
done

log_success -e "Protobuf generation for protostore SUCCESS!"

