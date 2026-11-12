
# ------------------------------------------
# Stage 1: Build Chronos
# ------------------------------------------
FROM golang:1.27.2-bookworm AS builder

WORKDIR /src

# Copy dependency definitions first.
# Docker can reuse this layer when source code changes
# but dependencies remain the same.
COPY go.mod go.sum ./

RUN go mod download

# Copy application source code.
COPY . .

# Compile a Linux executable without CGO dependencies.
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" \
    -o /out/chronos .

# ------------------------------------------
# Stage 2: Run Chronos
# ------------------------------------------
FROM debian:bookworm-slim AS runtime

# Install CA certificates and timezone database.
# Chronos uses timezone-aware scheduling.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
       tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only the compiled executable.
COPY --from=builder --chown=10001:10001 \
    /out/chronos /app/chronos

# Do not run the application as root.
USER 10001:10001

# Chronos HTTP API port.
EXPOSE 8080

ENTRYPOINT ["/app/chronos"]
