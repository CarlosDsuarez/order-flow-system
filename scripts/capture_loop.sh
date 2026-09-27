#!/bin/sh
# One capture run into <base>/<SYMBOL>/<UTC start>/, then an integrity check.
#
# Meant to be restarted by a supervisor (launchd KeepAlive): each run is its own
# directory, so end-of-run checks never re-read days of data and a crash loses one
# run at most. See docs/storage/continuous-capture.md.
#
# Usage: scripts/capture_loop.sh SYMBOL BASE_DIR [SECONDS=86400] [SNAPSHOT_INTERVAL=10]
set -eu

symbol="${1:?usage: capture_loop.sh SYMBOL BASE_DIR [SECONDS] [SNAPSHOT_INTERVAL]}"
base="${2:?usage: capture_loop.sh SYMBOL BASE_DIR [SECONDS] [SNAPSHOT_INTERVAL]}"
seconds="${3:-86400}"
snapshot_interval="${4:-10}"

repo="$(cd "$(dirname "$0")/.." && pwd)"
python="$repo/.venv/bin/python" # launchd has no uv on PATH; run `uv sync` once
run="$base/$symbol/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$run"
cd "$repo"

# Keep the Mac awake for as long as this run lives (a sleeping Mac drops the socket).
if command -v caffeinate >/dev/null 2>&1; then
  caffeinate -i -w $$ &
fi

# sh does not forward signals to a foreground child: run the recorder in the
# background and relay SIGTERM / SIGINT so it can flush and write capture_meta.json.
signalled=0
# Flush every 60 s into large parts: 2 s flushes wrote ~128k files/day/symbol and
# compressed ~3.8x worse. A hard crash loses <= 60 s; SIGTERM loses nothing.
"$python" scripts/record_l2.py --symbol "$symbol" --seconds "$seconds" --out "$run" \
  --snapshot-interval "$snapshot_interval" --flush-interval 60 --buffer-size 50000 \
  >"$run/record.log" 2>&1 &
child=$!
trap 'signalled=1; kill -TERM "$child" 2>/dev/null || true' TERM INT
status=0
wait "$child" || status=$?
if [ "$signalled" -eq 1 ]; then
  # The first wait returns as soon as the trap runs; wait again for the flush.
  wait "$child" 2>/dev/null || status=$?
  exit "$status"
fi

"$python" scripts/validate_capture.py "$run" --symbol "$symbol" >>"$run/record.log" 2>&1 || true
exit "$status"
