# shellcheck shell=bash disable=SC2034 # the variables are read by the scripts sourcing this file
# The protoc release pinned for the whole project: install-protoc.sh installs
# it on every build runner, and fetch-all-in-one-inputs.sh ships it in the
# all-in-one image, where jobworkerp compiles runner schemas at run time.
#
# Kept compatible with the bash 3.2 shipped with macOS.

# Update together with every SHA-256 below.
PROTOC_VERSION=36.2
PROTOC_LICENSE_SHA256=6e5e117324afd944dcf67f36cf329843bc1a92229a8cd9bb573d7a83130fea7d
PROTOC_RELEASE_URL=https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}
PROTOC_LICENSE_URL=https://raw.githubusercontent.com/protocolbuffers/protobuf/v${PROTOC_VERSION}/LICENSE

# protoc_release <platform>: sets protoc_asset and protoc_sha256 for a
# `uname -s`-`uname -m` style platform; fails for an unpinned one.
protoc_release() {
  case "$1" in
    Linux-x86_64)
      protoc_asset=protoc-${PROTOC_VERSION}-linux-x86_64.zip
      protoc_sha256=121f6c7afe1d4d0e3ea6aab9432038599250134cbf4474cb1167d2c7decd4278
      ;;
    Linux-aarch64)
      protoc_asset=protoc-${PROTOC_VERSION}-linux-aarch_64.zip
      protoc_sha256=8b8f18bd2b30346efbc698dd5a73dd7c805f3ef8380f6dfc95c768f3f1852f6a
      ;;
    Darwin-arm64)
      protoc_asset=protoc-${PROTOC_VERSION}-osx-aarch_64.zip
      protoc_sha256=9cd98a532c5c5e0c4161314de0225de27e4c8a323917b6ea7b1b714d3ae23466
      ;;
    *)
      return 1
      ;;
  esac
}
