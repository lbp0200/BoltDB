# Install BoltDB on CentOS/RHEL

## Prerequisites

- CentOS 7+ or RHEL 7+
- Root or sudo access

## Installation Methods

### Method 1: Install from RPM Package

#### Download RPM Package

```bash
# Download the latest release
curl -LO https://github.com/lbp0200/BoltDB/releases/latest/download/boltdb_latest_amd64.rpm

# Or download a specific version
curl -LO https://github.com/lbp0200/BoltDB/releases/download/v1.0.0/boltdb_1.0.0_amd64.rpm
```

#### Install the Package

```bash
# Install with dnf (CentOS 8+)
sudo dnf install -y boltdb_latest_amd64.rpm

# Or with yum (CentOS 7)
sudo yum install -y boltdb_latest_amd64.rpm
```

#### Verify Installation

```bash
# Check version
boltDB --version

# Test connection
boltDB -addr=:6337 &
redis-cli -p 6337 PING
```

### Method 2: Build from Source

```bash
# Install dependencies
sudo yum install -y golang git

# Clone repository
git clone https://github.com/lbp0200/BoltDB.git
cd BoltDB

# Build
go build -o ./build/boltDB ./cmd/boltDB/

# Install
sudo mv ./build/boltDB /usr/local/bin/boltDB
```

### Method 3: Using COPR (Future)

```bash
# Enable COPR repository (when available)
sudo dnf copr enable lbp0200/boltDB

# Install
sudo dnf install -y boltdb
```

## Configuration

### Create Data Directory

```bash
sudo mkdir -p /var/lib/bolt
sudo chown -R $USER:$USER /var/lib/bolt
```

### Create Log Directory

```bash
sudo mkdir -p /var/log/bolt
sudo chown -R $USER:$USER /var/log/bolt
```

## Running BoltDB

### Command Line

```bash
# Basic usage
boltDB -dir=/var/lib/bolt -log-level info

# With custom port
boltDB -addr=:6380 -dir=/var/lib/bolt

# Cluster mode
boltDB -cluster -addr=:6337 -dir=/var/lib/bolt

# Master-slave replication
boltDB -addr=:6337 -dir=/var/lib/bolt  # Master
boltDB -addr=:6380 -dir=/var/lib/bolt-slave -replicaof 127.0.0.1 6337  # Slave
```

### With systemd

Create `/etc/systemd/system/bolt.service`:

```ini
[Unit]
Description=BoltDB Redis-compatible database
Documentation=https://github.com/lbp0200/BoltDB
After=network.target

[Service]
Type=simple
User=bolt
Group=bolt
ExecStart=/usr/local/bin/boltDB -dir=/var/lib/bolt -log-level info
WorkingDirectory=/var/lib/bolt
Restart=always
RestartSec=5

# Security settings
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/bolt /var/log/bolt

# Hardening
Environment=BOLTDB_LOG_LEVEL=info

[Install]
WantedBy=multi-user.target
```

Create user and set permissions:

```bash
# Create dedicated user
sudo useradd -r -s /bin/false bolt

# Set ownership
sudo chown -R bolt:bolt /var/lib/bolt /var/log/bolt

# Reload systemd
sudo systemctl daemon-reload

# Start service
sudo systemctl start bolt

# Enable on boot
sudo systemctl enable bolt
```

## Management Commands

```bash
# Start BoltDB
sudo systemctl start bolt

# Stop BoltDB
sudo systemctl stop bolt

# Restart BoltDB
sudo systemctl restart bolt

# Check status
sudo systemctl status bolt

# View logs
sudo journalctl -u bolt -f
```

## Configuration File

BoltDB doesn't use a config file by default, but you can create one:

```bash
# Create config file
sudo mkdir -p /etc/bolt
sudo nano /etc/bolt/bolt.conf
```

Add your options:

```conf
# BoltDB Configuration
addr=:6337
dir=/var/lib/bolt
log-level=info
cluster=false
```

Run with config:

```bash
boltDB -config /etc/bolt/bolt.conf
```

## Upgrading

### From RPM Package

```bash
# Download new version
curl -LO https://github.com/lbp0200/BoltDB/releases/latest/download/boltdb_latest_amd64.rpm

# Upgrade
sudo dnf upgrade -y boltdb_latest_amd64.rpm

# Restart service
sudo systemctl restart bolt
```

### From Source

```bash
# Pull latest
cd BoltDB
git pull

# Rebuild
go build -o ./build/boltDB ./cmd/boltDB/

# Replace binary
sudo cp ./build/boltDB /usr/local/bin/boltDB

# Restart service
sudo systemctl restart bolt
```

## Uninstalling

```bash
# Remove package
sudo dnf remove -y boltdb

# Or with yum
sudo yum remove -y boltdb

# Remove data directory (optional)
sudo rm -rf /var/lib/bolt
```

## Firewall Configuration

If using firewalld:

```bash
# Allow BoltDB port
sudo firewall-cmd --permanent --add-port=6337/tcp

# Reload firewall
sudo firewall-cmd --reload

# Or allow specific IP range
sudo firewall-cmd --permanent --add-source=192.168.1.0/24
sudo firewall-cmd --permanent --add-port=6337/tcp
sudo firewall-cmd --reload
```

## SELinux (If Enabled)

If SELinux is enforcing, you may need to adjust:

```bash
# Check SELinux status
getenforce

# Allow BoltDB to bind to port
sudo semanage port -a -t http_port_t -p tcp 6337

# Or temporarily set to permissive
sudo setenforce 0
```

## Troubleshooting

### Service Won't Start

```bash
# Check logs
sudo journalctl -u bolt -n 100

# Check permissions
ls -la /var/lib/bolt
ls -la /var/log/bolt

# Check port
sudo netstat -tlnp | grep 6337
```

### Permission Denied

```bash
# Fix ownership
sudo chown -R bolt:bolt /var/lib/bolt /var/log/bolt
```

### Port Already in Use

```bash
# Find what's using the port
sudo lsof -i :6337

# Use different port
boltDB -addr=:6380
```

## Verification

```bash
# Test basic operations
redis-cli -p 6337 SET test "hello"
redis-cli -p 6337 GET test
redis-cli -p 6337 PING

# Check info
redis-cli -p 6337 INFO
redis-cli -p 6337 ROLE
```

## High Availability Setup

### Master-Slave with Sentinel

```bash
# Install Redis Sentinel
sudo yum install -y redis

# Configure sentinel
sudo nano /etc/redis-sentinel.conf
```

Add:

```conf
port 26379
sentinel monitor mymaster 127.0.0.1 6337 2
sentinel down-after-milliseconds mymaster 30000
sentinel failover-timeout mymaster 180000
```

Start sentinel:

```bash
sudo systemctl start redis-sentinel
sudo systemctl enable redis-sentinel
```
