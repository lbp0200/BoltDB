#!/usr/bin/env bash
# Simple mutation test: swap operators, run tests, report survivors.
# Compatible with any Go version. Slower than go-mutesting but works.
set -euo pipefail

TARGET="${1:-./internal/helper/...}"
TIMEOUT="${2:-60}"

tempdir=$(mktemp -d)
trap 'rm -rf "$tempdir"' EXIT

# Find Go files in the target package
go list -f '{{.Dir}}' "${TARGET}" > "$tempdir/pkgs.txt" 2>/dev/null || true

total=0
killed=0
lived=0
errors=0

while IFS= read -r dir; do
    [ -z "$dir" ] && continue
    find "$dir" -maxdepth 1 -name '*.go' ! -name '*_test.go' | while IFS= read -r file; do
        [ -z "$file" ] && continue
        base=$(basename "$file")
        pkg=$(basename "$dir")

        # Mutation 1: swap == and !=
        if grep -q '==' "$file" 2>/dev/null; then
            total=$((total + 1))
            cp "$file" "$tempdir/${pkg}_${base}_eq"
            sed -i 's/==/__EQ__/g; s/!=/==/g; s/__EQ__/!=/g' "$file"
            if timeout "$TIMEOUT" go test -count=1 "${TARGET}" 2>/dev/null; then
                echo "LIVED:  == -> !=  in $base"
                lived=$((lived + 1))
            else
                echo "KILLED: == -> !=  in $base"
                killed=$((killed + 1))
            fi
            cp "$tempdir/${pkg}_${base}_eq" "$file"
        fi

        # Mutation 2: swap true and false
        if grep -q '\btrue\b' "$file" 2>/dev/null || grep -q '\bfalse\b' "$file" 2>/dev/null; then
            total=$((total + 1))
            cp "$file" "$tempdir/${pkg}_${base}_bool"
            sed -i 's/\btrue\b/__TRUE__/g; s/\bfalse\b/true/g; s/__TRUE__/false/g' "$file"
            if timeout "$TIMEOUT" go test -count=1 "${TARGET}" 2>/dev/null; then
                echo "LIVED:  true<->false  in $base"
                lived=$((lived + 1))
            else
                echo "KILLED: true<->false  in $base"
                killed=$((killed + 1))
            fi
            cp "$tempdir/${pkg}_${base}_bool" "$file"
        fi

        # Mutation 3: swap && and ||
        if grep -q '&&' "$file" 2>/dev/null; then
            total=$((total + 1))
            cp "$file" "$tempdir/${pkg}_${base}_and"
            sed -i 's/&&/__AND__/g; s/||/&&/g; s/__AND__/||/g' "$file"
            if timeout "$TIMEOUT" go test -count=1 "${TARGET}" 2>/dev/null; then
                echo "LIVED:  && -> ||  in $base"
                lived=$((lived + 1))
            else
                echo "KILLED: && -> ||  in $base"
                killed=$((killed + 1))
            fi
            cp "$tempdir/${pkg}_${base}_and" "$file"
        fi

        # Mutation 4: swap comparison operators (>, >=, <, <=)
        # Pattern: swap > with >= and < with <= — catches boundary bugs like ttl>0→ttl>=0
        if grep -qE '[<>]=' "$file" 2>/dev/null || grep -qE '[<>][^=]' "$file" 2>/dev/null; then
            total=$((total + 1))
            cp "$file" "$tempdir/${pkg}_${base}_cmp"
            # First pass: swap >= with > (use sentinels to avoid ordering issues)
            sed -i 's/>=/__GE__/g; s/<=/__LE__/g; s/ > / __GT__ /g; s/ < / __LT__ /g' "$file"
            # Second pass: swap the sentinels
            sed -i 's/__GE__/ > /g; s/__LE__/ < /g; s/__GT__/>=/g; s/__LT__/<=/g' "$file"
            if timeout "$TIMEOUT" go test -count=1 "${TARGET}" 2>/dev/null; then
                echo "LIVED:  cmp boundary swap in $base"
                lived=$((lived + 1))
            else
                echo "KILLED: cmp boundary swap in $base"
                killed=$((killed + 1))
            fi
            cp "$tempdir/${pkg}_${base}_cmp" "$file"
        fi
    done
done < "$tempdir/pkgs.txt"

echo ""
echo "=== Mutation Results ==="
echo "Total : $total"
echo "Killed: $killed"
echo "Lived : $lived"
echo "Errors: $errors"
if [ "$total" -gt 0 ]; then
    score=$(echo "scale=1; $killed * 100 / $total" | bc)
    echo "Score : ${score}%"
fi
