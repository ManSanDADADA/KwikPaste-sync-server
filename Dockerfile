FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM alpine:3
RUN adduser -D -H -u 10001 kwikpaste && mkdir /data && chown kwikpaste:kwikpaste /data
COPY --from=build /src/target/release/kwikpaste-sync-server /usr/local/bin/kwikpaste-sync-server
ENV KP_SYNC_DATA=/data \
    KP_SYNC_LISTEN=0.0.0.0:8686
VOLUME /data
EXPOSE 8686
USER kwikpaste
ENTRYPOINT ["kwikpaste-sync-server"]
