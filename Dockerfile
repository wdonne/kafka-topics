FROM alpine:3.23.4 AS builder
ARG TARGETPLATFORM
COPY target/aarch64-unknown-linux-musl/release/kafka-topics /target/aarch64-unknown-linux-musl/release/
COPY target/x86_64-unknown-linux-musl/release/kafka-topics /target/x86_64-unknown-linux-musl/release/
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
    cp /target/aarch64-unknown-linux-musl/release/kafka-topics /kafka-topics; \
    elif [ "$TARGETPLATFORM" = "linux/amd64" ]; then \
    cp /target/x86_64-unknown-linux-musl/release/kafka-topics /kafka-topics; \
    fi
RUN apk add ca-certificates && update-ca-certificates

FROM scratch
USER 11000:11000
COPY --from=builder /kafka-topics /app/
COPY --from=builder /etc/ssl/certs /etc/ssl/certs
ENTRYPOINT ["/app/kafka-topics"]
