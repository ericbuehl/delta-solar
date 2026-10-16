FROM docker.io/library/rust:1-trixie AS build
RUN apt-get update && apt-get install -y --no-install-recommends libdbus-1-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo test --locked && cargo build --locked --release

FROM docker.io/library/debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends libdbus-1-3 ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/release/delta-solar /usr/local/bin/delta-solar
COPY THIRD_PARTY_LICENSES.md /usr/share/doc/delta-solar/THIRD_PARTY_LICENSES.md
ENV TZ=America/Los_Angeles \
    DBUS_SYSTEM_BUS_ADDRESS=unix:path=/run/dbus/system_bus_socket \
    SPOOL_DIR=/var/lib/delta-solar
ENTRYPOINT ["/usr/local/bin/delta-solar"]
