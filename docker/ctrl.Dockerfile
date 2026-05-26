# Build context is the DLSlime repo root (one level above `docker/`).
# Build with:
#   docker build -f docker/ctrl.Dockerfile -t dlslime-ctrl:local .
# Or, via compose:
#   docker compose -f docker/docker-compose.yml build ctrl

# ---- builder ----
# Use the `-slim` variant: same toolchain, no docs / locale data, saves ~700 MB.
# Needs Rust >= 1.88: clap 4.6+ requires `edition2024` (stabilized in 1.85),
# and comfy-table 7.2+ uses `let-chains` (stabilized in 1.88).
FROM rust:1.95-slim-bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src

# Prime dependency layer first, using a dummy main so the heavy
# crate graph (axum / tokio / redis / etc.) is cached independently
# of source-only changes.
COPY dlslime-ctrl/Cargo.toml dlslime-ctrl/Cargo.lock ./dlslime-ctrl/
RUN mkdir -p dlslime-ctrl/src \
    && echo 'fn main() {}' > dlslime-ctrl/src/main.rs \
    && (cd dlslime-ctrl && cargo build --release) \
    && rm -rf dlslime-ctrl/src \
              dlslime-ctrl/target/release/deps/dlslime_ctrl* \
              dlslime-ctrl/target/release/dlslime-ctrl*

# Real build.
COPY dlslime-ctrl/src ./dlslime-ctrl/src
COPY dlslime-ctrl/lua ./dlslime-ctrl/lua
RUN cd dlslime-ctrl && cargo build --release

# ---- runtime ----
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/dlslime-ctrl/target/release/dlslime-ctrl /usr/local/bin/dlslime-ctrl
COPY --from=builder /src/dlslime-ctrl/lua /opt/dlslime-ctrl/lua

# Default to the bundled-compose Redis hostname; overridable at runtime.
ENV DLSLIME_CTRL_REDIS_URL=redis://redis:6379 \
    DLSLIME_CTRL_RUST_LOG=info

EXPOSE 4479

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=6 \
    CMD curl -fsS http://localhost:4479/ >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/dlslime-ctrl"]
CMD ["server", "--host", "0.0.0.0", "--port", "4479"]
