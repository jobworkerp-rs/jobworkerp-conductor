# Runtime image for the statically linked (musl) conductor-main release
# binaries. The release workflow unpacks the published Linux archives into
# dist/linux-<arch>/ (amd64, arm64) before building; see README "Docker" for a
# local build.
#
# Commands run only in the preparation stage, which executes on the build
# host's platform; the final stage just copies files, so a multi-platform
# build needs no emulation. CA certificates and account files do not depend
# on the CPU architecture. Base images are pinned by digest (multi-platform
# index) so the published image is reproducible from this file.

FROM --platform=$BUILDPLATFORM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc AS prepare

# rustls reads the system CA store. The account matches earlier images
# (system user "conductor", home owned by group daemon) so mounted volumes
# keep working.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && adduser --system conductor \
    && mkdir -p /home/conductor \
    && chown conductor:daemon /home/conductor

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc

ARG TARGETARCH

COPY --from=prepare /etc/passwd /etc/group /etc/shadow /etc/gshadow /etc/
# The per-certificate entries in /etc/ssl/certs link into
# /usr/share/ca-certificates; copy both so none of them dangles.
COPY --from=prepare /etc/ssl/certs /etc/ssl/certs
COPY --from=prepare /usr/share/ca-certificates /usr/share/ca-certificates
COPY --from=prepare --chown=conductor:daemon /home/conductor /home/conductor
COPY --chown=conductor:daemon dist/linux-${TARGETARCH}/conductor-main /home/conductor/conductor-main
COPY dist/linux-${TARGETARCH}/LICENSE dist/linux-${TARGETARCH}/THIRD-PARTY-LICENSES.txt /usr/share/doc/jobworkerp-conductor/

WORKDIR /home/conductor
USER conductor

CMD ["./conductor-main"]
