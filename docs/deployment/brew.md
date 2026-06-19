# Install BoltDB via Homebrew (macOS)

## Prerequisites

- macOS with Homebrew installed
- Terminal access

## Installation

### Option 1: From Custom Tap (Recommended)

```bash
# Add the BoltDB tap
brew tap lbp0200/boltdb

# Install BoltDB
brew install boltdb
```

### Option 2: Build from Source

```bash
# Clone the repository
git clone https://github.com/lbp0200/BoltDB.git
cd BoltDB

# Build
go build -o ./build/boltDB cmd/boltDB/main.go

# Or install directly
go install ./cmd/boltDB
```

## Starting BoltDB

```bash
# Start with default settings (port 6337, data dir ./data)
boltDB

# Start with custom settings
boltDB -addr=:6380 -dir=/var/lib/bolt -log-level info

# Start in cluster mode
boltDB -cluster -addr=:6337 -dir=/var/lib/bolt
```

## Running as a Service (launchd)

### Create Launch Daemon

```bash
# Create data directory
sudo mkdir -p /var/lib/bolt
sudo chown -R $(whoami) /var/lib/bolt

# Create log directory
sudo mkdir -p /var/log/bolt
sudo chown -R $(whoami) /var/log/bolt
```

Create `/Library/LaunchDaemons/com.boltdb.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.boltdb</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/boltDB</string>
        <string>-addr=:6337</string>
        <string>-dir=/var/lib/bolt</string>
        <string>-log-level=info</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/var/log/bolt/bolt.log</string>
    <key>StandardErrorPath</key>
    <string>/var/log/bolt/bolt.error.log</string>
    <key>WorkingDirectory</key>
    <string>/var/lib/bolt</string>
</dict>
</plist>
```

### Manage Service

```bash
# Load service (start on boot)
sudo launchctl load /Library/LaunchDaemons/com.boltdb.plist

# Start immediately
sudo launchctl start com.boltdb

# Stop
sudo launchctl stop com.boltdb

# Unload (remove from boot)
sudo launchctl unload /Library/LaunchDaemons/com.boltdb.plist

# Check status
sudo launchctl list | grep bolt

# View logs
tail -f /var/log/bolt/bolt.log
```

### Cluster Mode Service

For cluster mode, use this plist:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.boltdb.cluster</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/boltDB</string>
        <string>-cluster</string>
        <string>-addr=:6337</string>
        <string>-dir=/var/lib/bolt</string>
        <string>-log-level=info</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/var/log/bolt/cluster.log</string>
    <key>StandardErrorPath</key>
    <string>/var/log/bolt/cluster.error.log</string>
</dict>
</plist>
```

## Configuration Options

| Flag | Description | Default |
|------|-------------|---------|
| `-addr` | Server address | `:6337` |
| `-dir` | Data directory | `./data` |
| `-log-level` | Log level (debug/info/warn/error) | `warning` |
| `-cluster` | Enable cluster mode | `false` |
| `-replicaof` | Master address (for slave mode) | - |

## Upgrading

```bash
# Upgrade BoltDB
brew upgrade boltdb

# Or reinstall
brew reinstall boltdb
```

## Uninstalling

```bash
# Remove BoltDB
brew uninstall boltdb

# Remove tap (optional)
brew untap lbp0200/boltdb
```

## Troubleshooting

### Port Already in Use

If port 6337 is already in use, specify a different port:

```bash
boltDB -addr=:6380
```

### Permission Denied

If you encounter permission errors, ensure the data directory is writable:

```bash
mkdir -p /var/lib/bolt
chmod 755 /var/lib/bolt
boltDB -dir=/var/lib/bolt
```
