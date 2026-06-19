# BoltDB systemd Service Configuration

This guide covers running BoltDB as a systemd service on Linux.

## Prerequisites

- Linux with systemd (CentOS, RHEL, Ubuntu, Debian, Fedora)
- Root or sudo access

## Basic Setup

### 1. Install BoltDB Binary

```bash
# Download and install BoltDB
sudo curl -L -o /usr/local/bin/boltDB https://github.com/lbp0200/BoltDB/releases/latest/download/boltDB

# Make executable
sudo chmod +x /usr/local/bin/boltDB
```

### 2. Create Service User

```bash
# Create dedicated user (optional but recommended)
sudo useradd -r -s /bin/false bolt
```

### 3. Create Directories

```bash
# Data directory
sudo mkdir -p /var/lib/bolt
sudo chown bolt:bolt /var/lib/bolt

# Log directory
sudo mkdir -p /var/log/bolt
sudo chown bolt:bolt /var/log/bolt
```

## Service Configuration

### Standalone Mode

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
ExecStart=/usr/local/bin/boltDB -addr=:6337 -dir=/var/lib/bolt -log-level info
WorkingDirectory=/var/lib/bolt
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/bolt /var/log/bolt

[Install]
WantedBy=multi-user.target
```

### Cluster Mode

```ini
[Unit]
Description=BoltDB Redis-compatible database (Cluster Mode)
Documentation=https://github.com/lbp0200/BoltDB
After=network.target

[Service]
Type=simple
User=bolt
Group=bolt
ExecStart=/usr/local/bin/boltDB -cluster -addr=:6337 -dir=/var/lib/bolt -log-level info
WorkingDirectory=/var/lib/bolt
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/bolt /var/log/bolt

# Cluster ports (if using multiple nodes)
# ExecStart=/usr/local/bin/boltDB -cluster -addr=:6337 -dir=/var/lib/bolt -log-level info

[Install]
WantedBy=multi-user.target
```

### Master-Slave Setup

#### Master Service

```ini
[Unit]
Description=BoltDB Master
Documentation=https://github.com/lbp0200/BoltDB
After=network.target

[Service]
Type=simple
User=bolt
Group=bolt
ExecStart=/usr/local/bin/boltDB -addr=:6337 -dir=/var/lib/bolt -log-level info
WorkingDirectory=/var/lib/bolt
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/bolt /var/log/bolt

[Install]
WantedBy=multi-user.target
```

#### Slave Service

```ini
[Unit]
Description=BoltDB Slave
Documentation=https://github.com/lbp0200/BoltDB
After=network.target

[Service]
Type=simple
User=bolt
Group=bolt
ExecStart=/usr/local/bin/boltDB -addr=:6380 -dir=/var/lib/bolt-slave -replicaof 127.0.0.1 6337 -log-level info
WorkingDirectory=/var/lib/bolt-slave
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/bolt-slave /var/log/bolt

[Install]
WantedBy=multi-user.target
```

## Managing the Service

### Reload systemd

```bash
# Reload systemd after creating/modifying service file
sudo systemctl daemon-reload
```

### Start/Stop/Restart

```bash
# Start BoltDB
sudo systemctl start bolt

# Stop BoltDB
sudo systemctl stop bolt

# Restart BoltDB
sudo systemctl restart bolt

# Enable on boot
sudo systemctl enable bolt

# Disable on boot
sudo systemctl disable bolt
```

### Check Status

```bash
# Check service status
sudo systemctl status bolt

# Check if enabled
sudo systemctl is-enabled bolt
```

## Viewing Logs

### Using journalctl

```bash
# View recent logs
sudo journalctl -u bolt -n 50

# Follow logs in real-time
sudo journalctl -u bolt -f

# View logs since specific time
sudo journalctl -u bolt --since "1 hour ago"

# View logs for specific date
sudo journalctl -u bolt --since "2024-01-01"

# Show only error messages
sudo journalctl -u bolt -p err
```

### Log Rotation

Create `/etc/logrotate.d/bolt`:

```
/var/log/bolt/*.log {
    daily
    rotate 7
    compress
    delaycompress
    notifempty
    create 0644 bolt bolt
    sharedscripts
    postrotate
        systemctl reload bolt > /dev/null 2>&1 || true
    endscript
}
```

## Environment Variables

You can pass environment variables to BoltDB:

```ini
[Service]
Environment="BOLTDB_LOG_LEVEL=info"
Environment="BOLTDB_ADDR=:6337"
ExecStart=/usr/local/bin/boltDB -dir=/var/lib/bolt
```

Or create `/etc/sysconfig/bolt`:

```bash
# /etc/sysconfig/bolt
BOLTDB_LOG_LEVEL=info
BOLTDB_DIR=/var/lib/bolt
```

Update service file:

```ini
[Service]
EnvironmentFile=/etc/sysconfig/bolt
ExecStart=/usr/local/bin/boltDB -dir=$BOLTDB_DIR -log-level=$BOLTDB_LOG_LEVEL
```

## Resource Limits

Add resource limits to service file:

```ini
[Service]
# Memory limit (512MB)
MemoryMax=512M

# CPU limit (50%)
CPUQuota=50%

# Max open files
LimitNOFILE=1024
```

## Health Check

Add health check:

```ini
[Service]
# Health check via ExecStartPost
ExecStartPost=/bin/bash -c 'sleep 2 && redis-cli -p 6337 PING || exit 1'
```

## Multiple Instances

Run multiple BoltDB instances on different ports:

### Instance 1 (Port 6337)

```bash
sudo cp /etc/systemd/system/bolt.service /etc/systemd/system/bolt-6337.service
sudo nano /etc/systemd/system/bolt-6337.service
```

Update:

```ini
[Service]
ExecStart=/usr/local/bin/boltDB -addr=:6337 -dir=/var/lib/bolt-6337 -log-level info
```

### Instance 2 (Port 6380)

```bash
sudo cp /etc/systemd/system/bolt.service /etc/systemd/system/bolt-6380.service
sudo nano /etc/systemd/system/bolt-6380.service
```

Update:

```ini
[Service]
ExecStart=/usr/local/bin/boltDB -addr=:6380 -dir=/var/lib/bolt-6380 -log-level info
```

Create directories:

```bash
sudo mkdir -p /var/lib/bolt-6337 /var/lib/bolt-6380
sudo chown bolt:bolt /var/lib/bolt-6337 /var/lib/bolt-6380
```

Start both:

```bash
sudo systemctl daemon-reload
sudo systemctl start bolt-6337 bolt-6380
sudo systemctl enable bolt-6337 bolt-6380
```

## High Availability with Keepalived

### Install Keepalived

```bash
# Ubuntu/Debian
sudo apt install -y keepalived

# CentOS/RHEL
sudo yum install -y keepalived
```

### Create Keepalived Config

```bash
sudo nano /etc/keepalived/keepalived.conf
```

```conf
vrrp_instance VI_1 {
    state BACKUP
    interface eth0
    virtual_router_id 51
    priority 100
    advert_int 1
    authentication {
        auth_type PASS
        auth_pass secret123
    }
    virtual_ipaddress {
        192.168.1.100
    }
}
```

### Update BoltDB to Bind to VIP

```ini
[Service]
ExecStart=/usr/local/bin/boltDB -addr=192.168.1.100:6337 -dir=/var/lib/bolt
```

## Troubleshooting

### Service Fails to Start

```bash
# Check detailed logs
sudo journalctl -u bolt -xe

# Check if port is in use
sudo netstat -tlnp | grep 6337

# Check permissions
ls -la /var/lib/bolt
ls -la /var/log/bolt

# Test binary manually
sudo -u bolt /usr/local/bin/boltDB -dir=/var/lib/bolt
```

### Permission Issues

```bash
# Fix ownership
sudo chown -R bolt:bolt /var/lib/bolt /var/log/bolt

# Fix permissions
sudo chmod 755 /var/lib/bolt /var/log/bolt
```

### Memory Issues

```bash
# Check memory usage
systemctl status bolt

# Add memory limit
sudo systemctl edit bolt
```

Add:

```ini
[Service]
MemoryMax=1G
```

## Complete Example with All Features

```ini
[Unit]
Description=BoltDB Redis-compatible database
Documentation=https://github.com/lbp0200/BoltDB
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=bolt
Group=bolt

# Configuration
EnvironmentFile=/etc/sysconfig/bolt
ExecStart=/usr/local/bin/boltDB \
    -addr=:6337 \
    -dir=/var/lib/bolt \
    -log-level info \
    -cluster=${BOLT_CLUSTER:-false}

WorkingDirectory=/var/lib/bolt

# Restart policy
Restart=always
RestartSec=10

# Security
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/bolt /var/log/bolt

# Resource limits
MemoryMax=2G
LimitNOFILE=4096
CPUQuota=100%

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=bolt

# Health check
ExecStartPost=/bin/bash -c 'for i in 1 2 3; do redis-cli -p 6337 PING && break || sleep 1; done'

[Install]
WantedBy=multi-user.target
```

Create `/etc/sysconfig/bolt`:

```bash
BOLT_CLUSTER=false
```
