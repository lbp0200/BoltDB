# BoltDB Deployment Guide

This directory contains deployment guides for different platforms and package managers.

## Available Guides

| Platform | File | Description |
|----------|------|-------------|
| macOS | [brew.md](brew.md) | Install via Homebrew |
| Docker | [docker.md](docker.md) | Deploy using Docker |
| Ubuntu/Debian | [ubuntu.md](ubuntu.md) | Install via .deb package |
| CentOS/RHEL | [centos.md](centos.md) | Install via RPM package |
| Linux (systemd) | [systemd.md](systemd.md) | Configure as systemd service |

## Docker Files

Docker configuration files are located in `deploy/docker/`:

| File | Description |
|------|-------------|
| `Dockerfile` | Multi-stage build for BoltDB |
| `docker-compose.yml` | Main compose file |
| `docker-compose.standalone.yml` | Standalone mode |
| `docker-compose.master-slave.yml` | Master-Slave replication |
| `docker-compose.cluster.yml` | Cluster mode |
| `docker-compose.sentinel.yml` | High availability with Sentinel |

## Quick Start

Choose your platform:

```bash
# macOS
brew install lbp0200/boltdb/boltdb

# Docker (build from source)
cd deploy/docker
docker build -t lbp0200/boltDB:latest .
docker run -d -p 6337:6337 -v /tmp/bolt:/data lbp0200/boltDB:latest

# Ubuntu/Debian
sudo dpkg -i boltdb_*.deb

# CentOS/RHEL
sudo rpm -i boltdb-*.rpm
```

## TLS Configuration

BoltDB supports TLS encryption for client connections, replication links, cluster bus, and sentinel connections.

### Basic TLS (Server)

Start the server with a TLS certificate and key:

```bash
boltDB --tls-cert=/path/to/cert.pem --tls-key=/path/to/key.pem
```

This enables TLS on the main listener port. All client connections must use TLS (plain TCP connections will fail the TLS handshake).

### TLS with Client Certificate Verification

Provide a CA certificate to require client certificates signed by that CA:

```bash
boltDB \
  --tls-cert=/path/to/server-cert.pem \
  --tls-key=/path/to/server-key.pem \
  --tls-ca=/path/to/ca-cert.pem
```

When `--tls-ca` is set, clients must present a certificate signed by the CA. This is equivalent to Redis's `tls-auth-clients yes`.

### TLS for Replication (Master-Slave)

When TLS is configured on the master, slave connections automatically use TLS:

```bash
# Master
boltDB --tls-cert=/path/to/cert.pem --tls-key=/path/to/key.pem

# Slave
boltDB --replicaof=master-host:6337
```

The slave connects using the master's TLS config (loaded from the same `--tls-cert`/`--tls-key` flags).

### TLS for Cluster Mode

Cluster bus connections (node-to-node gossip) automatically use TLS when the server is started with TLS:

```bash
boltDB --cluster --tls-cert=/path/to/cert.pem --tls-key=/path/to/key.pem
```

The cluster bus uses the same TLS config as the main listener. All inter-node communication is encrypted.

### TLS for Sentinel

Sentinel outbound connections to monitored BoltDB servers support TLS:

```bash
sentinel \
  --tls-cert=/path/to/cert.pem \
  --tls-key=/path/to/key.pem \
  --tls-ca=/path/to/ca-cert.pem
```

This ensures sentinel-to-server connections (health checks, failover commands) are encrypted.

### Self-Signed Certificates (Testing)

For testing and development, you can generate self-signed certificates:

```bash
# Generate a self-signed certificate and key
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj "/CN=localhost"

# Start BoltDB with self-signed cert
boltDB --tls-cert=cert.pem --tls-key=key.pem
```

For production, use certificates signed by a trusted CA or your organization's internal CA.
