# Shopping Sessions: Rust pipeline (v4).
# Build stage: compile the binary (network is available during the build).
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
RUN cargo build --release --locked

# Run stage: just the binary. The scorer runs it as uid 1000 with a read-only root,
# /data read-only and /output, /scratch writable.
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
RUN chmod 755 /app/shopping-sessions
WORKDIR /app
CMD ["/app/shopping-sessions"]
