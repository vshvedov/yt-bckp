#!/bin/sh
# Map the container's runtime user to the host's PUID/PGID so downloaded files
# on the mounted volume are owned by your user instead of root, then drop
# privileges and start the server.
set -e

PUID="${PUID:-1000}"
PGID="${PGID:-1000}"
DLDIR="${YT_BCKP_DOWNLOAD_DIR:-/downloads}"

# Create or re-point the 'ytbckp' group/user to the requested ids (idempotent
# across restarts; -o allows reusing an id that already exists).
if getent group ytbckp >/dev/null 2>&1; then
  groupmod -o -g "$PGID" ytbckp
else
  groupadd -o -g "$PGID" ytbckp
fi
if getent passwd ytbckp >/dev/null 2>&1; then
  usermod -o -u "$PUID" -g "$PGID" ytbckp
else
  useradd -o -u "$PUID" -g "$PGID" -d /app -s /usr/sbin/nologin ytbckp
fi

# Ensure the downloads dir exists and is writable by the runtime user.
mkdir -p "$DLDIR"
chown "$PUID:$PGID" "$DLDIR" || true

echo "[entrypoint] starting yt-bckp as UID=$PUID GID=$PGID  downloads=$DLDIR"

# gosu drops from root to the mapped user and execs the server (PID 1 signals work).
exec gosu ytbckp "$@"
