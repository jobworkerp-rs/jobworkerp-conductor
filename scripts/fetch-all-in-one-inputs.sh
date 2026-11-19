#!/usr/bin/env bash
# Usage: fetch-all-in-one-inputs.sh <amd64|arm64> <dest-dir>
#
# Fetches the third-party inputs of the all-in-one image for one Linux
# architecture at pinned versions, verifying each download's SHA-256 before
# anything is extracted: the jobworkerp all-in-one binary with its licenses
# (from the jobworkerp-rs release archive), the static tini binary, and the
# static protoc with its well-known types (jobworkerp compiles runner schemas
# at run time and fails without it), each with its license. <dest-dir> must
# not exist; it is assembled next to its final place and moved there only
# once complete, so a failure never leaves a partial directory behind.
#
# Output: bin/{all-in-one,protoc,tini}, include/google/protobuf/,
# licenses/jobworkerp-rs/{LICENSE-MIT,LICENSE-APACHE,THIRD-PARTY-LICENSES.txt},
# licenses/{protobuf,tini}/LICENSE and versions.env (JOBWORKERP_VERSION=,
# TINI_VERSION=, PROTOC_VERSION=).
#
# Exit status: 1 when a download or check fails, 2 on usage errors.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/lib/fetch-pinned.sh
source "${SCRIPT_DIR}/lib/fetch-pinned.sh"
# shellcheck source=scripts/lib/protoc-release.sh
source "${SCRIPT_DIR}/lib/protoc-release.sh"

# Update together with the SHA-256 values below.
JOBWORKERP_VERSION=0.25.0
TINI_VERSION=0.19.0
TINI_LICENSE_SHA256=e5f46bca81266bdd511cf08018d66866870531794569c04f9b45f50dd23c28b0

arch=${1-}
dest=${2-}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

usage_error() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 2
}

[[ -n "${arch}" && -n "${dest}" ]] || usage_error "usage: $0 <amd64|arm64> <dest-dir>"

case "${arch}" in
  amd64)
    target=x86_64-unknown-linux-gnu
    protoc_platform=Linux-x86_64
    jobworkerp_sha256=440aa1a79cacde0f34f7852f912e91d047040d29190824d845b679670bbac492
    tini_sha256=c5b0666b4cb676901f90dfcb37106783c5fe2077b04590973b885950611b30ee
    ;;
  arm64)
    target=aarch64-unknown-linux-gnu
    protoc_platform=Linux-aarch64
    jobworkerp_sha256=bef5d21ce9084ecc66c8c7083bc4c12d7606c0385191d053d552c0d46a23935f
    tini_sha256=eae1d3aa50c48fb23b8cbdf4e369d0910dfc538566bfd09df89a774aa84a48b9
    ;;
  *)
    usage_error "unsupported architecture: ${arch} (expected amd64 or arm64)"
    ;;
esac

[[ ! -e "${dest}" ]] || die "${dest} already exists"

jobworkerp_base=${AIO_INPUTS_JOBWORKERP_URL:-https://github.com/jobworkerp-rs/jobworkerp-rs/releases/download/v${JOBWORKERP_VERSION}}
tini_base=${AIO_INPUTS_TINI_URL:-https://github.com/krallin/tini/releases/download/v${TINI_VERSION}}
tini_license_url=${AIO_INPUTS_TINI_LICENSE_URL:-https://raw.githubusercontent.com/krallin/tini/v${TINI_VERSION}/LICENSE}
protoc_base=${PROTOC_BASE_URL:-${PROTOC_RELEASE_URL}}
protoc_license_url=${AIO_INPUTS_PROTOC_LICENSE_URL:-${PROTOC_LICENSE_URL}}
protoc_release "${protoc_platform}" || die "no pinned protoc for ${protoc_platform}"

mkdir -p "$(dirname "${dest}")"
work=$(mktemp -d "${dest}.XXXXXX")
trap 'rm -rf "${work}"' EXIT
mkdir -p "${work}/download" "${work}/extract" "${work}/protoc" "${work}/out/bin" \
  "${work}/out/licenses/jobworkerp-rs" "${work}/out/licenses/tini" "${work}/out/licenses/protobuf"

archive=jobworkerp-${target}.tar.gz
fetch_pinned "${jobworkerp_base}/${archive}" "${jobworkerp_sha256}" "${work}/download/${archive}"
fetch_pinned "${tini_base}/tini-static-${arch}" "${tini_sha256}" "${work}/download/tini"
fetch_pinned "${tini_license_url}" "${TINI_LICENSE_SHA256}" "${work}/download/tini-LICENSE"
fetch_pinned "${protoc_base}/${protoc_asset}" "${protoc_sha256}" "${work}/download/${protoc_asset}"
fetch_pinned "${protoc_license_url}" "${PROTOC_LICENSE_SHA256}" "${work}/download/protobuf-LICENSE"

# Only the files the image ships; tar fails when one of them is missing.
stage=jobworkerp-${target}
members=(all-in-one LICENSE-MIT LICENSE-APACHE THIRD-PARTY-LICENSES.txt)
tar -xzf "${work}/download/${archive}" -C "${work}/extract" --no-same-owner \
  "${members[@]/#/${stage}/}" || die "${archive} lacks one of: ${members[*]}"
for member in "${members[@]}"; do
  path="${work}/extract/${stage}/${member}"
  [[ -f "${path}" && ! -L "${path}" ]] || die "${archive}: ${member} is not a regular file"
done

install -m 0755 "${work}/extract/${stage}/all-in-one" "${work}/out/bin/all-in-one"
install -m 0755 "${work}/download/tini" "${work}/out/bin/tini"
for license in "${members[@]:1}"; do
  install -m 0644 "${work}/extract/${stage}/${license}" "${work}/out/licenses/jobworkerp-rs/${license}"
done
install -m 0644 "${work}/download/tini-LICENSE" "${work}/out/licenses/tini/LICENSE"

unzip -q "${work}/download/${protoc_asset}" 'bin/protoc' 'include/*' -d "${work}/protoc" \
  || die "${protoc_asset} lacks bin/protoc or include/"
[[ -f "${work}/protoc/bin/protoc" && ! -L "${work}/protoc/bin/protoc" ]] \
  || die "${protoc_asset}: bin/protoc is not a regular file"
install -m 0755 "${work}/protoc/bin/protoc" "${work}/out/bin/protoc"
cp -R "${work}/protoc/include" "${work}/out/include"
chmod -R u=rwX,go=rX "${work}/out/include"
install -m 0644 "${work}/download/protobuf-LICENSE" "${work}/out/licenses/protobuf/LICENSE"

printf 'JOBWORKERP_VERSION=%s\nTINI_VERSION=%s\nPROTOC_VERSION=%s\n' \
  "${JOBWORKERP_VERSION}" "${TINI_VERSION}" "${PROTOC_VERSION}" >"${work}/out/versions.env"

mv "${work}/out" "${dest}"
echo "Fetched all-in-one inputs for ${arch} into ${dest}"
