FROM debian:bookworm-slim

RUN apt update; apt install -y libssl3 ca-certificates protobuf-compiler
RUN /sbin/ldconfig

RUN adduser --system conductor

RUN mkdir -p /home/conductor && chown conductor:daemon /home/conductor

WORKDIR /home/conductor

COPY --chown=conductor:daemon ./target/x86_64-unknown-linux-musl/release/conductor-main .

USER conductor

CMD ["./conductor-main"]
