# Build Stage
FROM golang:1.25-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build BoltDB
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o boltDB ./cmd/boltDB

# Final Stage
FROM alpine:3.19

# Install ca-certificates and redis-cli (for testing)
RUN apk add --no-cache ca-certificates redis

# Create non-root user
RUN adduser -D -u 1000 bolt

# Set working directory
WORKDIR /data

# Copy binary from builder
COPY --from=builder /build/boltDB /usr/local/bin/bolt

# Set ownership
RUN chown -R bolt:bolt /data

# Switch to non-root user
USER bolt

# Expose default port
EXPOSE 6379

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD redis-cli -p 6379 PING || exit 1

# Default command
ENTRYPOINT ["bolt"]
CMD ["-addr", ":6379", "-dir", "/data", "-log-level", "info"]
