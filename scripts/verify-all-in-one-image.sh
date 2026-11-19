#!/usr/bin/env bash
# Usage: verify-all-in-one-image.sh <image> <conductor-version> <jobworkerp-version>
#
# Smoke check of a built all-in-one image, run by the public release workflow
# on each architecture's native runner before the image is published. It
# checks what makes the image unusable: the bundled versions and their
# labels (jobworkerp needs protoc at run time); the account owning the data
# volume (uid 100, gid 65534, as in the conductor-only image); that both
# servers start and the healthcheck passes;
# that bootstrap registers the jobworkerp server once and finds it again on
# the same volume; that jobworkerp listens on loopback only by default; and
# that `docker stop` with the documented grace period stops it cleanly.
#
# Exit status: 1 when the image fails a check, 2 on usage errors.
set -euo pipefail

image=${1-}
conductor_version=${2-}
jobworkerp_version=${3-}

# The README recommends this grace period; the entrypoint needs at most two
# ALL_IN_ONE_STOP_TIMEOUT_SEC (20 s each) of it.
readonly STOP_GRACE_SEC=60
readonly STOP_LIMIT_SEC=45
readonly HEALTHY_TIMEOUT_SEC=120
readonly JOBWORKERP_PORT_HEX=2328 # 9000
readonly JOBWORKERP_LABEL=io.jobworkerp.all-in-one.jobworkerp-version
readonly TINI_LABEL=io.jobworkerp.all-in-one.tini-version
readonly PROTOC_LABEL=io.jobworkerp.all-in-one.protoc-version

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

usage_error() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 2
}

[[ -n "${image}" && -n "${conductor_version}" && -n "${jobworkerp_version}" ]] \
  || usage_error "usage: $0 <image> <conductor-version> <jobworkerp-version>"

name=aio-verify-$$-${RANDOM}
volume=${name}-data
containers=()
cleanup() {
  local container
  for container in "${containers[@]}"; do
    docker rm -f "${container}" >/dev/null 2>&1 || true
  done
  docker volume rm -f "${volume}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

label() {
  docker image inspect -f "{{ index .Config.Labels \"$1\" }}" "${image}"
}

# check_contents: the bundled versions against the labels, and the account,
# read in one throwaway container through the entrypoint's pass-through path.
check_contents() {
  local lines conductor jobworkerp tini protoc uid gid
  lines=$(docker run --rm "${image}" bash -c \
    'conductor-main --version; all-in-one --version; tini --version; protoc --version; id -u; id -g') \
    || die "the image cannot run its bundled binaries"
  { read -r conductor; read -r jobworkerp; read -r tini; read -r protoc; read -r uid; read -r gid; } <<<"${lines}"

  [[ "${conductor}" == "conductor-main ${conductor_version}" ]] \
    || die "conductor-main reports '${conductor}', expected ${conductor_version}"
  [[ "${jobworkerp}" == "all-in-one ${jobworkerp_version}" ]] \
    || die "all-in-one reports '${jobworkerp}', expected ${jobworkerp_version}"
  [[ "$(label org.opencontainers.image.version)" == "${conductor_version}" ]] \
    || die "label org.opencontainers.image.version is not ${conductor_version}"
  [[ "$(label "${JOBWORKERP_LABEL}")" == "${jobworkerp_version}" ]] \
    || die "label ${JOBWORKERP_LABEL} is not ${jobworkerp_version}"
  local tini_label protoc_label
  tini_label=$(label "${TINI_LABEL}")
  if [[ -z "${tini_label}" || "${tini}" != "tini version ${tini_label} "* ]]; then
    die "label ${TINI_LABEL} ('${tini_label}') does not match the bundled tini ('${tini}')"
  fi
  protoc_label=$(label "${PROTOC_LABEL}")
  if [[ -z "${protoc_label}" || "${protoc}" != "libprotoc ${protoc_label}" ]]; then
    die "label ${PROTOC_LABEL} ('${protoc_label}') does not match the bundled protoc ('${protoc}')"
  fi
  echo "versions: ${conductor}, ${jobworkerp}, tini ${tini_label}, protoc ${protoc_label}"

  [[ "${uid}:${gid}" == 100:65534 ]] || die "runs as uid ${uid} gid ${gid}, expected uid 100 gid 65534"
  echo "account: uid ${uid} gid ${gid}"
}

# start <container>: runs the image on the shared volume until it is healthy.
start() {
  local container=$1 deadline=$((SECONDS + HEALTHY_TIMEOUT_SEC)) status
  containers+=("${container}")
  docker run -d --name "${container}" -v "${volume}:/home/conductor" "${image}" >/dev/null
  while :; do
    status=$(docker inspect -f '{{ .State.Status }}/{{ if .State.Health }}{{ .State.Health.Status }}{{ end }}' "${container}")
    case "${status}" in
      running/healthy) return 0 ;;
      running/*) ;;
      *) die "${container} stopped before becoming healthy (${status}):"$'\n'"$(docker logs "${container}" 2>&1 | tail -n 30)" ;;
    esac
    ((SECONDS < deadline)) \
      || die "${container} not healthy within ${HEALTHY_TIMEOUT_SEC}s:"$'\n'"$(docker logs "${container}" 2>&1 | tail -n 30)"
    sleep 1
  done
}

# expect_log <container> <text>
expect_log() {
  docker logs "$1" 2>&1 | grep -Fq -- "$2" \
    || die "$1 did not log '$2':"$'\n'"$(docker logs "$1" 2>&1 | tail -n 30)"
}

# check_loopback_only <container>: every listener on the jobworkerp port is on
# a loopback address (127.0.0.1 or ::1), and there is at least one.
check_loopback_only() {
  local listeners
  listeners=$(docker exec "$1" cat /proc/net/tcp /proc/net/tcp6 \
    | awk -v port=":${JOBWORKERP_PORT_HEX}" '$4 == "0A" && substr($2, length($2) - 4) == port { print $2 }')
  [[ -n "${listeners}" ]] || die "nothing listens on the jobworkerp port 9000"
  while IFS= read -r address; do
    case "${address%:*}" in
      0100007F | 00000000000000000000000001000000) ;;
      *) die "jobworkerp listens on ${address}, not on loopback only" ;;
    esac
  done <<<"${listeners}"
  echo "jobworkerp listens on loopback only"
}

# stop <container>: docker stop with the documented grace period.
stop() {
  local started=${SECONDS} took exit_code
  docker stop -t "${STOP_GRACE_SEC}" "$1" >/dev/null
  took=$((SECONDS - started))
  exit_code=$(docker inspect -f '{{ .State.ExitCode }}' "$1")
  [[ "${exit_code}" == 0 ]] \
    || die "$1 exited with ${exit_code} on docker stop:"$'\n'"$(docker logs "$1" 2>&1 | tail -n 30)"
  ((took < STOP_LIMIT_SEC)) || die "$1 took ${took}s to stop (limit ${STOP_LIMIT_SEC}s)"
  echo "$1 stopped in ${took}s"
}

check_contents

start "${name}-first"
expect_log "${name}-first" "JobworkerpServer local: created"
check_loopback_only "${name}-first"
stop "${name}-first"

start "${name}-second"
expect_log "${name}-second" "JobworkerpServer local: exists"
stop "${name}-second"

echo "PASS: ${image}"
