
# ==========================================
# Stage 1: Build Chronos
# ==========================================
FROM golang:1.27.2-bookworm AS builder

WORKDIR /app

# Copy dependency manifests first.
# Docker can cache dependency downloads.
COPY go.mod go.sum ./

RUN go mod download

# Copy the application source.
COPY . .

# Compile a Linux executable.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" \
    -o /out/chronos .

# ==========================================
# Stage 2: Runtime
# ==========================================
FROM debian:bookworm-slim AS runtime

# Install certificates and timezone database.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        tzdata && \
    rm -rf /var/lib/apt/lists/*

# Create a non-root application user.
RUN groupadd --system --gid 10001 chronos && \
    useradd --system --uid 10001 \
        --gid chronos \
        --no-create-home chronos

WORKDIR /app

# Copy only the compiled application.
COPY --from=builder /out/chronos /app/chronos

# Run without root privileges.
USER 10001:10001

EXPOSE 8080

ENTRYPOINT ["/app/chronos"]
