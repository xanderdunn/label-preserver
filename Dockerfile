# build image
FROM rust:1-slim AS builder

RUN apt-get update && \
    apt-get install -y pkg-config openssl libssl-dev

WORKDIR /usr/local/label-preserver
COPY ./Cargo.toml ./Cargo.toml
COPY ./src ./src
RUN cargo build --release

# runtime image
FROM debian:bookworm-slim
COPY --from=builder /usr/local/label-preserver/target/release/label-preserver /usr/local/bin/label-preserver

# run as non-root user
RUN groupadd --system nonroot && useradd --system --gid nonroot nonroot
USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/label-preserver"]
