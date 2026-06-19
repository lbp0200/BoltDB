# Install BoltDB on Ubuntu/Debian

## Prerequisites

- Ubuntu 18.04+ or Debian 10+
- Root or sudo access

## Installation Methods

### Method 1: Install from .deb Package

#### Download .deb Package

```bash
# Download the latest release
wget https://github.com/lbp0200/BoltDB/releases/latest/download/boltdb_latest_amd64.deb

# Or download a specific version
wget https://github.com/lbp0200/BoltDB/releases/download/v1.0.0/boltdb_1.0.0_amd64.deb
```

#### Install the Package

```bash
# Install with dpkg
sudo dpkg -i boltdb_latest_amd64.deb

# Fix any dependency issues
sudo apt-get install -f
```

#### Verify Installation

```bash
# Check version
boltDB --version

# Test connection
boltDB -addr=:6337 &
redis-cli -p 6337 PING
```

### Method 2: Install from Repository

#### Add Repository

```bash
# Add BoltDB GPG key
curl -fsSL https://github.com/lbp0200/BoltDB/gpg.key | sudo gpg --dearmor -o /usr/share/keyrings/boltdb.gpg

# Add repository
echo "deb [signed-by=/usr/share/keyrings/boltdb.gpg] https://lbp0200.github.io/boltdeb ./" | sudo tee /etc/apt/sources.list.d/boltdb.list

# Update package index
sudo apt update
```

#### Install BoltDB

```bash
sudo apt install boltdb
```

### Method 3: Build from Source

```bash
# Install Go (if not installed)
sudo apt install -y golang-go

# Clone repository
git clone https://github.com/lbp0200/BoltDB.git
cd BoltDB

# Build
go build -o ./build/boltDB cmd/boltDB/main.go

# Install
sudo mv ./build/boltDB /usr/local/bin/boltDB
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

### From .deb Package

```bash
# Download new version
wget https://github.com/lbp0200/BoltDB/releases/latest/download/boltdb_latest_amd64.deb

# Install
sudo dpkg -i boltdb_latest_amd64.deb

# Restart service
sudo systemctl restart bolt
```

### From Repository

```bash
sudo apt update
sudo apt upgrade boltdb
```

## Uninstalling

```bash
# Remove package
sudo dpkg -r boltdb

# Remove repository (if added)
sudo rm /etc/apt/sources.list.d/boltdb.list
sudo rm /usr/share/keyrings/boltdb.gpg
sudo apt update
```

## Firewall Configuration

If using UFW:

```bash
# Allow BoltDB port
sudo ufw allow 6337/tcp

# Allow specific IP
sudo ufw allow from 192.168.1.0/24 to any port 6337
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
