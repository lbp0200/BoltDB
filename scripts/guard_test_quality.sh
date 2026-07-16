#!/usr/bin/env bash
# Guard against known "coverage theater" patterns in tests.
# Fails the build if empty assertions that always pass are reintroduced.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "=== Test quality guards ==="

fail=0

# 1) Empty assertions that always pass
if matches=$(rg -n --glob '*_test.go' --glob '!node_modules/**' \
  'assert\.True\(t,\s*true\)|require\.True\(t,\s*true\)' \
  . 2>/dev/null || true); then
  if [[ -n "${matches}" ]]; then
    echo "ERROR: found always-true assertions (coverage theater):"
    echo "${matches}"
    fail=1
  else
    echo "OK: no assert.True(t, true) / require.True(t, true)"
  fi
fi

# 2) Obvious no-op success patterns
if matches=$(rg -n --glob '*_test.go' --glob '!node_modules/**' \
  'assert\.Equal\(t,\s*true,\s*true\)|assert\.False\(t,\s*false\)' \
  . 2>/dev/null || true); then
  if [[ -n "${matches}" ]]; then
    echo "ERROR: found tautological assertions:"
    echo "${matches}"
    fail=1
  else
    echo "OK: no tautological Equal/False stubs"
  fi
fi

# 3) "Always true" boolean expressions used as assertions
if matches=$(rg -n --glob '*_test.go' --glob '!node_modules/**' \
  'err\s*!=\s*nil\s*\|\|\s*err\s*==\s*nil|assert\.True\(t,\s*.*> 0 \|\| .*\s*==\s*0\)' \
  . 2>/dev/null || true); then
  if [[ -n "${matches}" ]]; then
    echo "ERROR: found tautological boolean assertion patterns:"
    echo "${matches}"
    fail=1
  else
    echo "OK: no err!=nil||err==nil / count>0||count==0 theater"
  fi
fi

if [[ "${fail}" -ne 0 ]]; then
  echo ""
  echo "See docs/plans/TODO.md § 测试质量优化"
  exit 1
fi

echo "=== Test quality guards passed ==="
