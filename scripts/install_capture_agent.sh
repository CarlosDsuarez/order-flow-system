#!/bin/sh
# Install (or remove) a per-user launchd agent that keeps SYMBOL capturing into BASE_DIR.
#
# Usage: scripts/install_capture_agent.sh SYMBOL BASE_DIR           # install + start
#        scripts/install_capture_agent.sh SYMBOL BASE_DIR --remove  # stop + uninstall
#
# Run from the checkout the agent should use (after `uv sync`), not a throwaway worktree.
set -eu

symbol="${1:?usage: install_capture_agent.sh SYMBOL BASE_DIR [--remove]}"
base="${2:?usage: install_capture_agent.sh SYMBOL BASE_DIR [--remove]}"
repo="$(cd "$(dirname "$0")/.." && pwd)"
label="com.orderflow.capture.$(printf '%s' "$symbol" | tr '[:upper:]' '[:lower:]')"
plist="$HOME/Library/LaunchAgents/$label.plist"
domain="gui/$(id -u)"

if [ "${3:-}" = "--remove" ]; then
  launchctl bootout "$domain/$label" 2>/dev/null || true # SIGTERM: last run flushes
  rm -f "$plist"
  echo "removed $label"
  exit 0
fi

if [ ! -x "$repo/.venv/bin/python" ]; then
  echo "missing $repo/.venv: run 'uv sync' in $repo first" >&2
  exit 1
fi
mkdir -p "$base/$symbol" "$HOME/Library/LaunchAgents"
sed -e "s|__LABEL__|$label|g" -e "s|__REPO__|$repo|g" \
  -e "s|__SYMBOL__|$symbol|g" -e "s|__BASE__|$base|g" \
  "$repo/ops/launchd/capture.plist.template" >"$plist"
plutil -lint "$plist" >/dev/null
launchctl bootout "$domain/$label" 2>/dev/null || true
launchctl bootstrap "$domain" "$plist"
echo "loaded $label: $base/$symbol (logs: $base/$symbol/launchd.log)"
