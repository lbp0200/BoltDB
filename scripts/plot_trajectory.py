#!/usr/bin/env python3
"""plot_trajectory.py — Multi-panel trajectory plot from JSONL timeline data.

Usage:
    python3 plot_trajectory.py <input.jsonl> [output.png]

Reads JSONL pressure samples and produces a 5-panel chart:
  - L0 Score (compaction pressure)
  - Active Retries / Backpressure
  - Goroutines
  - Replication Lag
  - Health (computed per-sample approximation)

Dependencies: matplotlib, numpy (pip install matplotlib numpy)
"""

import json
import sys
import os
from collections import defaultdict

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import numpy as np
except ImportError:
    print("ERROR: matplotlib and numpy required. Install with:")
    print("  pip install matplotlib numpy")
    sys.exit(1)


def load_jsonl(path):
    """Load JSONL file into a list of dicts."""
    samples = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                samples.append(json.loads(line))
    return samples


def compute_per_sample_health(s):
    """Approximate per-sample health score (0-1).
    
    Matches the Go computeHealth logic approximately:
    - L0 < 8: healthy
    - L0 < 20: degrading
    - L0 >= 20: failed
    - ActiveRetries penalties
    """
    l0 = s.get("l0", 0)
    ar = s.get("ar", 0)

    # L0 recovery health
    if l0 <= 8:
        l0_health = 1.0
    elif l0 <= 15:
        l0_health = 1.0 - (l0 - 8) / (15 - 8) * 0.5
    elif l0 >= 25:
        l0_health = 0.0
    else:
        l0_health = 0.5 - (l0 - 15) / (25 - 15) * 0.5

    # Retry health
    if ar <= 5:
        retry_health = 1.0
    elif ar <= 30:
        retry_health = 1.0 - (ar - 5) / (30 - 5) * 0.5
    elif ar >= 100:
        retry_health = 0.0
    else:
        retry_health = 0.5 - (ar - 30) / (100 - 30) * 0.5

    return 0.6 * l0_health + 0.4 * retry_health


def plot_trajectory(samples, output_path):
    """Generate 5-panel trajectory chart."""
    if not samples:
        print("ERROR: no samples loaded")
        return False

    # Parse timestamps
    timestamps = []
    for s in samples:
        ts = s.get("ts", 0)
        if ts > 1e15:  # nanoseconds
            ts = ts / 1e9
        timestamps.append(ts)

    base_time = timestamps[0]
    elapsed = [(t - base_time) / 60.0 for t in timestamps]  # minutes

    # Extract series
    l0 = [s.get("l0", 0) for s in samples]
    retries = [s.get("ar", 0) for s in samples]
    goroutines = [s.get("go", 0) for s in samples]
    heap_mb = [s.get("heap_mb", 0) for s in samples]
    master_offset = [s.get("mo", 0) for s in samples]
    slave_offset = [s.get("so", 0) for s in samples]
    reconnects = [s.get("rc", 0) for s in samples]
    l0_delayed = [s.get("dl", 0) for s in samples]
    l0_rejected = [s.get("rj", 0) for s in samples]

    # Replication lag (only meaningful when both offsets exist)
    repl_lag = []
    for mo, so in zip(master_offset, slave_offset):
        if mo > 0 and so > 0:
            repl_lag.append(mo - so)
        elif mo > 0:
            repl_lag.append(0)  # master with no slave offset
        else:
            repl_lag.append(None)

    # Approximate health per sample
    health = [compute_per_sample_health(s) for s in samples]

    # Count samples with oscillation/lag metrics
    has_repl = any(mo > 0 and so > 0 for mo, so in zip(master_offset, slave_offset))

    # Build figure
    n_panels = 5
    fig, axes = plt.subplots(n_panels, 1, figsize=(14, 12), sharex=True)
    fig.suptitle("BoltDB Trajectory: " + os.path.basename(output_path),
                 fontsize=14, fontweight="bold")

    colors = {
        "l0": "#d62728",
        "retries": "#ff7f0e",
        "goroutines": "#2ca02c",
        "repl_lag": "#1f77b4",
        "health": "#9467bd",
    }

    # Panel 1: L0 Score
    ax = axes[0]
    ax.plot(elapsed, l0, color=colors["l0"], linewidth=1.5, label="L0 Score")
    ax.axhline(y=8.0, color="green", linestyle="--", linewidth=0.8, alpha=0.6, label="Healthy (8)")
    ax.axhline(y=20.0, color="orange", linestyle="--", linewidth=0.8, alpha=0.6, label="Degraded (20)")
    ax.axhline(y=25.0, color="red", linestyle="--", linewidth=0.8, alpha=0.6, label="Collapsed (25)")
    ax.set_ylabel("L0 Score")
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(True, alpha=0.3)

    # Panel 2: Backpressure (retries + delayed + rejected)
    ax = axes[1]
    ax.plot(elapsed, retries, color=colors["retries"], linewidth=1.5, label="Active Retries")
    # Stacked area for delayed/rejected
    if any(l0_delayed) or any(l0_rejected):
        ax.fill_between(elapsed, 0, l0_delayed, alpha=0.3, color="orange", label="L0 Delayed")
        ax.fill_between(elapsed, l0_delayed, [a + b for a, b in zip(l0_delayed, l0_rejected)],
                        alpha=0.3, color="red", label="L0 Rejected")
    ax.set_ylabel("Backpressure")
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(True, alpha=0.3)

    # Panel 3: Goroutines / Heap
    ax = axes[2]
    ax.plot(elapsed, goroutines, color=colors["goroutines"], linewidth=1.5, label="Goroutines")
    ax_twin = ax.twinx()
    ax_twin.fill_between(elapsed, heap_mb, alpha=0.2, color="gray", label="Heap (MB)")
    ax_twin.set_ylabel("Heap (MB)", color="gray")
    ax.set_ylabel("Goroutines")
    ax.legend(loc="upper left", fontsize=8)
    ax_twin.legend(loc="upper right", fontsize=8)
    ax.grid(True, alpha=0.3)

    # Panel 4: Replication
    ax = axes[3]
    if has_repl:
        ax.plot(elapsed, repl_lag, color=colors["repl_lag"], linewidth=1.5,
                label="Replication Lag (offset diff)")
    else:
        ax.plot(elapsed, master_offset, color=colors["repl_lag"], linewidth=1.5,
                label="Master Offset")
    if any(reconnects):
        ax_twin = ax.twinx()
        ax_twin.plot(elapsed, reconnects, color="red", linestyle="--", linewidth=1,
                     alpha=0.6, label="Reconnects")
        ax_twin.set_ylabel("Reconnects", color="red")
        ax_twin.legend(loc="upper right", fontsize=8)
    ax.set_ylabel("Replication")
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(True, alpha=0.3)

    # Panel 5: Approximate Health
    ax = axes[4]
    ax.plot(elapsed, health, color=colors["health"], linewidth=2, label="Health (approx)")
    ax.axhline(y=0.85, color="green", linestyle="--", linewidth=0.8, alpha=0.6, label="Healthy (0.85)")
    ax.axhline(y=0.70, color="orange", linestyle="--", linewidth=0.8, alpha=0.6, label="Warn (0.70)")
    ax.axhline(y=0.50, color="red", linestyle="--", linewidth=0.8, alpha=0.6, label="Degraded (0.50)")
    ax.set_ylabel("Health")
    ax.set_xlabel("Elapsed Time (minutes)")
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(True, alpha=0.3)
    ax.set_ylim(-0.05, 1.05)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Trajectory plot saved: {output_path} (samples={len(samples)})")
    return True


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    input_path = sys.argv[1]
    if not os.path.exists(input_path):
        print(f"ERROR: file not found: {input_path}")
        sys.exit(1)

    output_path = sys.argv[2] if len(sys.argv) > 2 else input_path.replace(".jsonl", ".png")

    samples = load_jsonl(input_path)
    print(f"Loaded {len(samples)} samples from {input_path}")

    if len(samples) < 2:
        print("ERROR: need at least 2 samples to plot")
        sys.exit(1)

    success = plot_trajectory(samples, output_path)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
