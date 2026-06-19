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
