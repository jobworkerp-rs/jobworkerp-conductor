FROM debian:bookworm-slim

RUN apt update; apt install -y libssl3 ca-certificates
RUN /sbin/ldconfig

RUN adduser --system slack_handler

RUN mkdir -p /home/slack_handler && chown slack_handler:daemon /home/slack_handler

WORKDIR /home/slack_handler

COPY --chown=slack_handler:daemon ./target/x86_64-unknown-linux-musl/release/slack-handler .

USER slack_handler

CMD ["./slack-handler"]
