# yt-bckp

A local, open-source YouTube downloader. Paste one URL or a whole
batch, pick **MP3** (audio) or **MP4** (video), and yt-bckp saves the files to a
local `downloads/` folder. It runs entirely on your machine through a tiny web
UI — no accounts, no cloud, no tracking.

- Single URL or batch (many URLs at once)
- MP3 (best-quality audio extraction) or MP4 (best video + audio, merged)
- Live progress for every job, with a "Reveal in Finder" button
- Pure Python standard library backend (no Flask, no pip, no venv)

## Requirements

- [`yt-dlp`](https://github.com/yt-dlp/yt-dlp) and [`ffmpeg`](https://ffmpeg.org/) on your `PATH`
- `python3` (3.14 recommended)

### macOS

Install everything with Homebrew:

```bash
brew install yt-dlp ffmpeg
```

`python3` is also available via Homebrew (`brew install python`). If you ran the
command above and already have a Homebrew `python3`, you're ready to go.

### Linux

Use your distribution's package manager. `python3` is preinstalled on most
distros; install it alongside the others if it isn't.

```bash
# Debian / Ubuntu
sudo apt install yt-dlp ffmpeg python3

# Fedora
sudo dnf install yt-dlp ffmpeg python3

# Arch
sudo pacman -S yt-dlp ffmpeg python

# openSUSE
sudo zypper install yt-dlp ffmpeg python3
```

If your distro packages an outdated `yt-dlp` (YouTube changes often, so a stale
build may fail), grab the latest from [pip](https://pypi.org/project/yt-dlp/)
(`pipx install yt-dlp` or `python3 -m pip install -U yt-dlp`) or the
[official binary](https://github.com/yt-dlp/yt-dlp#installation), and make sure
it's on your `PATH`.

## Run

The easiest way:

```bash
./run.sh
```

`run.sh` checks that `yt-dlp` and `ffmpeg` are installed (and runs the Homebrew
install for you if they're missing), then starts the server.

Or start it directly:

```bash
python3 server.py
```

Either way, the app listens on:

```
http://127.0.0.1:8723
```

Open that URL in your browser, paste your YouTube links (one per line), choose a
format, and hit download. Finished files land in the `downloads/` folder next to
`server.py`.

### Choosing where files are saved

By default files go to `downloads/` next to `server.py`. To save somewhere else,
use the `--dir` flag or the `YT_BCKP_DOWNLOAD_DIR` environment variable (the flag
wins if you use both):

```bash
python3 server.py --dir ~/Music/yt        # CLI flag (~ and relative paths are fine)
YT_BCKP_DOWNLOAD_DIR=~/Music/yt python3 server.py   # env var
```

`--host` and `--port` work the same way (`python3 server.py --port 9000`). See
`python3 server.py --help` for all flags. In Docker, set the directory by
mounting a volume at `/downloads` (see [Run on a server](#run-on-a-server-docker)).

## Restricted videos & "confirm you're not a bot"

By default the app downloads **anonymously** — no login, independent of your browser. That's
all you need for the vast majority of videos, and ads are never part of the download (yt-dlp
fetches the source media directly; ads are injected only by the web player at playback).

Some videos do need your YouTube session — age-restricted, private, unlisted-with-login, or
members-only content — and occasionally YouTube throws a "confirm you're not a bot" check. For
those, tick **"Use my browser cookies"** in the UI and pick your browser. The app then passes
`--cookies-from-browser <browser>` to yt-dlp, which reads that browser's cookies locally and
sends them only to YouTube.

> Tip: if cookie access fails, fully **quit the selected browser** first (especially Chrome,
> which locks its cookie database while running).

## Run on a server (Docker)

yt-bckp runs headless in a container, so you can host it on a homelab box and use
it from any browser on your network. Files are written to a mounted volume — point
it at any path on your server.

```bash
# build (yt-dlp is pinned at build time; rebuild to update it)
docker compose build

# run
docker compose up -d
```

Then open `http://<your-server-ip>:8723`.

Edit `docker-compose.yml` to suit your setup:

- **`volumes:`** — change the left side of `./downloads:/downloads` to any host path
  (e.g. `/mnt/media/youtube:/downloads`). That's where files are saved.
- **`PUID` / `PGID`** — set these to your user's `id -u` / `id -g` so downloaded
  files are owned by you, not root.
- **`ports:`** — change the host port if 8723 is taken (`"9000:8723"`).

#### Setting the download directory with Compose

> **Important:** with Docker Compose you do **not** use the `--dir` flag. The
> download location is controlled by the **volume mount**, not by a CLI flag or by
> `YT_BCKP_DOWNLOAD_DIR` (which stays at its container default of `/downloads`).

In `docker-compose.yml`, the `volumes` entry has the form `HOST_PATH:/downloads`:

```yaml
volumes:
  - /mnt/media/youtube:/downloads   # change ONLY the left side
```

- **Left of the `:`** = the folder on your server — **change this** to wherever you
  want files saved.
- **Right of the `:`** = the path inside the container — **always leave it as
  `/downloads`**. The app writes there, and the mount maps it to your host folder.

More examples:

```yaml
- ./downloads:/downloads            # default: a 'downloads' folder next to the compose file
- /mnt/media/youtube:/downloads     # an absolute path on the server
- /Volumes/NAS/yt:/downloads        # a mounted NAS share
```

If you'd rather not edit the YAML, make the host path an environment variable so it
can come from your shell or a `.env` file next to `docker-compose.yml`:

```yaml
volumes:
  - ${YT_DIR:-./downloads}:/downloads
```

```bash
YT_DIR=/mnt/media/youtube docker compose up -d
# or put  YT_DIR=/mnt/media/youtube  in a .env file beside docker-compose.yml
```

> Make sure the host folder exists and is writable before starting, and set
> `PUID`/`PGID` (above) so the files aren't owned by root.

Without compose:

```bash
docker build -t yt-bckp .
docker run -d --name yt-bckp -p 8723:8723 \
  -e PUID=1000 -e PGID=1000 \
  -v /mnt/media/youtube:/downloads \
  yt-bckp
```

### Configuration (environment variables)

| Variable | Default (container) | Purpose |
|---|---|---|
| `YT_BCKP_HOST` | `0.0.0.0` | Bind address. `127.0.0.1` = local only; `0.0.0.0` = reachable on the network. |
| `YT_BCKP_PORT` | `8723` | Port the server listens on. |
| `YT_BCKP_DOWNLOAD_DIR` | `/downloads` | Where files are saved (mount a volume here). |
| `YT_BCKP_OPEN_BROWSER` | `0` | Auto-open a browser on start (desktop only; off in the container). |
| `YT_BCKP_YTDLP` | `/usr/local/bin/yt-dlp` | Path to the `yt-dlp` binary. |
| `PUID` / `PGID` | `1000` | User/group that owns files on the mounted volume. |

> **Security:** there is **no authentication**. Keep it on a trusted LAN, or put a
> reverse proxy / VPN (Authelia, Tailscale, etc.) in front. Do **not** expose it
> directly to the internet. "Reveal in Finder" is disabled on non-macOS hosts; the
> UI shows the saved filename instead.

## How it works

yt-bckp is a small HTTP server built on Python's standard library
(`http.server` + `ThreadingHTTPServer`). The browser UI talks to a tiny JSON
API; each download URL is handed to a background worker thread that shells out to
`yt-dlp` (which in turn uses `ffmpeg`) via `subprocess`. The server parses
`yt-dlp`'s line-by-line output to track progress, titles, and the final file
path, and exposes that state back to the UI so you can watch jobs complete in
real time. All downloaded files are written to the `downloads/` directory.

## License & use

MIT licensed — see [LICENSE](LICENSE).

yt-bckp is intended for **personal use and backup** of content you have the right
to download. Please respect [YouTube's Terms of Service](https://www.youtube.com/t/terms)
and applicable copyright law.
