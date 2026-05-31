#!/usr/bin/env bash
#
# run.sh — launch yt-bckp.
#
# Verifies that yt-dlp and ffmpeg are available, installing them via Homebrew if
# they're missing, then starts the standard-library Python server.
#
set -e

# Make sure the tools yt-bckp depends on are present.
if ! command -v yt-dlp >/dev/null 2>&1 || ! command -v ffmpeg >/dev/null 2>&1; then
  echo "yt-dlp and/or ffmpeg not found on PATH — installing via Homebrew..."
  brew install yt-dlp ffmpeg
fi

# Resolve the directory this script lives in so it works from anywhere.
SCRIPT_DIR="$(dirname "$0")"

# Hand off to the server (replaces this process so Ctrl-C goes straight to it).
exec python3 "$SCRIPT_DIR/server.py"
