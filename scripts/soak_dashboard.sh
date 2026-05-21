#!/usr/bin/env bash
# soak_dashboard.sh — real-time text dashboard for BoltDB soak tests
#
# Usage:
#   METRICS_URL=http://localhost:6338 ./scripts/soak_dashboard.sh
#   METRICS_URL=http://localhost:6338 REFRESH=2 ./scripts/soak_dashboard.sh
#
# Dependencies: curl, tput, bc (for metrics calc)

set -euo pipefail

METRICS_URL="${METRICS_URL:-http://localhost:6338}"
REFRESH="${REFRESH:-2}"
NAMES="${NAMES:-}"

# Parse names parameter: "node1=http://localhost:6338 node2=http://localhost:6339"
declare -A URLS
if [[ -n "$NAMES" ]]; then
	while IFS='=' read -r name url; do
		URLS["$name"]="$url"
	done <<< "$NAMES"
else
	URLS["default"]="$METRICS_URL"
fi

fetch_metrics() {
	local url="$1"
	curl -s --max-time 2 "$url/debug/metrics" 2>/dev/null || echo "ERROR"
}

render_header() {
	local cols="$1"
	printf '%*s\n' "$cols" '' | tr ' ' '='
	printf "%-*s\n" "$cols" "BoltDB Soak Dashboard  $(date '+%Y-%m-%d %H:%M:%S')"
	printf '%*s\n' "$cols" '' | tr ' ' '='
}

extract_val() {
	local line="$1"
	local field="$2"
	echo "$line" | grep -o "${field}=[^ ]*" | head -1 | cut -d= -f2
}

extract_metric() {
	local data="$1"
	local metric="$2"
	echo "$data" | grep -i "^.*$metric" | head -1 | awk '{print $2}'
}

render_single() {
	local name="$1"
	local data="$2"
	[[ "$data" == "ERROR" ]] && { echo "  $name: DOWN"; return; }

	local l0 retries go_mem repl backlog clients

	l0=$(echo "$data" | grep "L0:" | head -1)
	retries=$(echo "$data" | grep "retries" | head -1)
	go_mem=$(echo "$data" | grep "Go:" | head -1)
	repl=$(echo "$data" | grep "Repl:" | head -1)
	backlog=$(echo "$data" | grep "Backlog:" | head -1)
	clients=$(echo "$data" | grep "Clients:" | head -1)

	echo "  --- $name ---"
	echo "  $l0"
	echo "  $retries"
	echo "  $go_mem"
	echo "  $repl"
	echo "  $backlog"
	echo "  $clients"
}

watch -n "$REFRESH" --no-title --color 2>/dev/null || watch -n "$REFRESH" 2>/dev/null || {
	# Fallback: manual loop
	while true; do
		clear 2>/dev/null || true
		cols=$(tput cols 2>/dev/null || echo 80)
		render_header "$cols"
		for name in "${!URLS[@]}"; do
			data=$(fetch_metrics "${URLS[$name]}")
			render_single "$name" "$data"
			echo ""
		done
		sleep "$REFRESH"
	done
}
