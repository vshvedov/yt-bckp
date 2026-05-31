#!/usr/bin/env python3
"""
yt-bckp backend server.

A local, dependency-free YouTube downloader for macOS. Built entirely on the
Python standard library: it serves a single-page frontend and exposes a small
JSON API that drives yt-dlp subprocesses in background threads.

Run with:  python3 server.py   (no args, no deps)

Endpoints:
    GET  /                  -> serves index.html
    POST /api/download      -> queue downloads, spawn one worker thread per URL
    GET  /api/jobs          -> list jobs (newest first)
    GET  /api/reveal?id=ID  -> reveal a finished file in Finder
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# --------------------------------------------------------------------------- #
# Configuration
#
# Everything is overridable via environment variables so the same code runs
# unchanged on a Mac desktop (defaults below) or in a headless container.
# --------------------------------------------------------------------------- #


def _env_flag(name, default):
    """Read a boolean-ish env var ('0'/'false'/'no'/'' -> False)."""
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() not in ("", "0", "false", "no", "off")


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Bind address: 127.0.0.1 locally (safe), set YT_BCKP_HOST=0.0.0.0 in a container.
HOST = os.environ.get("YT_BCKP_HOST", "127.0.0.1")
PORT = int(os.environ.get("YT_BCKP_PORT", "8723"))

# Where files land. Point this at any path / mounted volume.
DOWNLOAD_DIR = os.environ.get("YT_BCKP_DOWNLOAD_DIR") or os.path.join(ROOT_DIR, "downloads")
INDEX_HTML = os.path.join(ROOT_DIR, "index.html")

# Path to yt-dlp: explicit override, then the homebrew path, then PATH lookup.
YT_DLP = os.environ.get("YT_BCKP_YTDLP") or "/opt/homebrew/bin/yt-dlp"
if not os.path.exists(YT_DLP):
    YT_DLP = shutil.which("yt-dlp") or YT_DLP

# Pop the browser open on startup (desktop only). Disable in containers.
OPEN_BROWSER = _env_flag("YT_BCKP_OPEN_BROWSER", True)

# "Reveal in Finder" only makes sense on the local macOS machine.
CAN_REVEAL = sys.platform == "darwin"

# Intermediate files (partial downloads, pre-merge / pre-extract artifacts) are
# staged here; yt-dlp moves only the FINISHED, converted file into DOWNLOAD_DIR.
INCOMPLETE_DIR = os.path.join(DOWNLOAD_DIR, "_incomplete_")

# Output filename template (relative — the directory comes from -P home below).
OUTPUT_TEMPLATE = "%(title)s.%(ext)s"

# Browsers yt-dlp can pull cookies from via --cookies-from-browser. Used for
# age-restricted / private videos and to clear "confirm you're not a bot" checks.
ALLOWED_BROWSERS = {
    "safari", "chrome", "chromium", "firefox",
    "brave", "edge", "opera", "vivaldi", "whale",
}

# --------------------------------------------------------------------------- #
# Shared job state
# --------------------------------------------------------------------------- #

# JOBS maps a job id -> JobObject dict (see contract in the project spec).
# It is read by the HTTP handler threads and written by the worker threads, so
# every access must be guarded by JOBS_LOCK.
JOBS = {}
JOBS_LOCK = threading.Lock()


def new_job(url, fmt, cookies_browser=None):
    """Create a job record in the 'queued' state and store it in JOBS."""
    job = {
        "id": str(uuid.uuid4()),
        "url": url,
        "format": fmt,
        "cookies": cookies_browser,   # browser name or None
        "status": "queued",
        "progress": 0,
        "title": None,
        "filename": None,
        "error": None,
        "created": time.time(),
    }
    with JOBS_LOCK:
        JOBS[job["id"]] = job
    return job


def update_job(job_id, **fields):
    """Thread-safe partial update of a job record."""
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if job is not None:
            job.update(fields)


def snapshot_jobs():
    """Return a list copy of all jobs, newest first."""
    with JOBS_LOCK:
        jobs = [dict(j) for j in JOBS.values()]
    jobs.sort(key=lambda j: j["created"], reverse=True)
    return jobs


# --------------------------------------------------------------------------- #
# yt-dlp worker
# --------------------------------------------------------------------------- #

# We drive yt-dlp's output ourselves with explicit, machine-readable markers
# (see build_command): a custom --progress-template plus --print lines. This is
# far more reliable than scraping the human-readable "[download] 42%" output,
# which yt-dlp suppresses while --print-json is active and emits on stderr.
PROGRESS_RE = re.compile(r"^PROG\s+([0-9]+(?:\.[0-9]+)?)%")   # download:PROG  42.3%
TITLE_RE = re.compile(r"^TITLE\s+(.+)$")                       # before_dl:TITLE ...
DONE_RE = re.compile(r"^DONE\s+(.+)$")                         # after_move:DONE <final path>
# Fallback only: a finished file that yt-dlp says was already downloaded.
ALREADY_RE = re.compile(r"\[download\]\s*(.+?)\s+has already been downloaded")


def underscore_filename(path):
    """
    Rename a finished file so spaces in its name become underscores
    (e.g. 'Me at the zoo.mp3' -> 'Me_at_the_zoo.mp3'). Only the filename is
    touched, never the parent directory. Returns the new path, or the original
    path unchanged if there are no spaces or the rename fails.
    """
    if not path or not os.path.exists(path):
        return path
    directory, base = os.path.split(path)
    underscored = base.replace(" ", "_")
    if underscored == base:
        return path
    target = os.path.join(directory, underscored)
    try:
        os.replace(path, target)  # atomic within the same directory
        return target
    except OSError:
        return path  # keep the original name if the rename fails


def build_command(url, fmt, cookies_browser=None):
    """Return the yt-dlp argv list for the requested format."""
    base = [
        YT_DLP,
        "--newline",            # emit progress on its own line so we can parse it
        "--no-playlist",        # grab ONLY the video in the URL, never its playlist/mix
        # Force progress onto stdout in a parseable form we control. The default
        # "[download] 42%" output is human-only and goes to stderr; this template
        # is emitted reliably on stdout instead.
        "--progress",
        "--progress-template", "download:PROG %(progress._percent_str)s",
        # Emit the title up front and the final on-disk path after the move.
        "--print", "before_dl:TITLE %(title)s",
        "--print", "after_move:DONE %(filepath)s",
        "-P", "home:" + DOWNLOAD_DIR,     # finished files are moved here
        "-P", "temp:" + INCOMPLETE_DIR,   # all intermediate work staged here
        "-o", OUTPUT_TEMPLATE,
    ]
    # Borrow the user's browser cookies for age-restricted / private videos and
    # to satisfy "confirm you're not a bot" challenges.
    if cookies_browser in ALLOWED_BROWSERS:
        base += ["--cookies-from-browser", cookies_browser]
    if fmt == "mp3":
        return base + [
            "-x",                       # extract audio
            "--audio-format", "mp3",
            "--audio-quality", "0",     # best quality
            url,
        ]
    # default / "mp4": best video + best audio, merged into mp4
    return base + [
        "-f", "bv*+ba/b",
        "--merge-output-format", "mp4",
        url,
    ]


def run_download(job_id):
    """
    Worker body: run yt-dlp for one job, streaming its stdout to update progress,
    title and the final filename. Runs in its own thread.
    """
    with JOBS_LOCK:
        job = dict(JOBS[job_id])
    url = job["url"]
    fmt = job["format"]
    cookies_browser = job.get("cookies")

    update_job(job_id, status="downloading")

    cmd = build_command(url, fmt, cookies_browser)

    # Captured along the way; reconciled into the final filename at the end.
    title = None
    final_dest = None         # final file path (from our after_move:DONE marker)
    stderr_tail = []          # keep last lines of stderr for error reporting

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,           # line-buffered
        )
    except FileNotFoundError:
        update_job(
            job_id,
            status="error",
            error=f"yt-dlp not found at {YT_DLP}",
        )
        return
    except Exception as exc:  # pragma: no cover - defensive
        update_job(job_id, status="error", error=str(exc))
        return

    # Drain stderr in a side thread so a full pipe never blocks stdout parsing.
    def drain_stderr():
        for line in proc.stderr:
            line = line.rstrip("\n")
            if line:
                stderr_tail.append(line)
                # bound memory: keep only the most recent lines
                if len(stderr_tail) > 40:
                    del stderr_tail[0]

    err_thread = threading.Thread(target=drain_stderr, daemon=True)
    err_thread.start()

    # Parse stdout line by line, looking for our explicit markers.
    for raw in proc.stdout:
        line = raw.rstrip("\n")
        if not line:
            continue

        # 1) Progress percentage (download:PROG  42.3%).
        m = PROGRESS_RE.search(line)
        if m:
            try:
                update_job(job_id, progress=float(m.group(1)))
            except ValueError:
                pass
            continue

        # 2) Title, emitted before the download starts (before_dl:TITLE ...).
        m = TITLE_RE.search(line)
        if m:
            title = m.group(1).strip()
            update_job(job_id, title=title)
            continue

        # 3) Final on-disk path, emitted after the move (after_move:DONE ...).
        m = DONE_RE.search(line)
        if m:
            final_dest = m.group(1).strip()
            update_job(job_id, progress=100)
            continue

        # 4) Fallback: yt-dlp skipped the download because the file already exists.
        m = ALREADY_RE.search(line)
        if m:
            final_dest = m.group(1).strip()
            update_job(job_id, progress=100)
            continue

    proc.stdout.close()
    returncode = proc.wait()
    err_thread.join(timeout=2)

    # ----------------------------------------------------------------------- #
    # Resolve the final on-disk filename.
    # ----------------------------------------------------------------------- #
    filename = final_dest

    # Fallback if the DONE marker was missing: look for <title>.<ext> in the
    # downloads dir (the move target).
    if (not filename or not os.path.exists(filename)) and title:
        ext = ".mp3" if fmt == "mp3" else ".mp4"
        candidate = os.path.join(DOWNLOAD_DIR, title + ext)
        if os.path.exists(candidate):
            filename = candidate

    if filename:
        filename = os.path.abspath(filename)
        # Store the final file with underscores instead of spaces in its name.
        filename = underscore_filename(filename)

    # ----------------------------------------------------------------------- #
    # Final status transition.
    # ----------------------------------------------------------------------- #
    if returncode == 0:
        update_job(
            job_id,
            status="done",
            progress=100,
            title=title,
            filename=filename,
            error=None,
        )
    else:
        tail = "\n".join(stderr_tail[-10:]).strip()
        update_job(
            job_id,
            status="error",
            title=title,
            filename=filename,
            error=tail or f"yt-dlp exited with code {returncode}",
        )


# --------------------------------------------------------------------------- #
# HTTP handler
# --------------------------------------------------------------------------- #


class Handler(BaseHTTPRequestHandler):
    server_version = "yt-bckp/1.0"

    # -- helpers ---------------------------------------------------------- #

    def _send_json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html, status=200):
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        # Compact one-line access log.
        print(f"[http] {self.address_string()} - {fmt % args}")

    # -- routing ---------------------------------------------------------- #

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/":
            self._serve_index()
        elif path == "/api/config":
            self._send_json({"can_reveal": CAN_REVEAL})
        elif path == "/api/jobs":
            self._send_json({"jobs": snapshot_jobs()})
        elif path == "/api/reveal":
            self._handle_reveal(parse_qs(parsed.query))
        else:
            self._send_json({"error": "not found"}, status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/download":
            self._handle_download()
        else:
            self._send_json({"error": "not found"}, status=404)

    # -- endpoint implementations ---------------------------------------- #

    def _serve_index(self):
        """Serve index.html from disk; fall back to a placeholder if absent."""
        if os.path.exists(INDEX_HTML):
            try:
                with open(INDEX_HTML, "r", encoding="utf-8") as fh:
                    self._send_html(fh.read())
                return
            except OSError as exc:
                self._send_html(f"<h1>Error reading index.html</h1><pre>{exc}</pre>",
                                status=500)
                return
        self._send_html(
            "<!doctype html><meta charset='utf-8'>"
            "<title>yt-bckp</title>"
            "<h1>yt-bckp</h1>"
            "<p>index.html not found yet. The backend is running on "
            f"http://{HOST}:{PORT}.</p>"
        )

    def _handle_download(self):
        """Parse the request body, create jobs, and spawn worker threads."""
        try:
            length = int(self.headers.get("Content-Length", 0))
        except (TypeError, ValueError):
            length = 0
        raw = self.rfile.read(length) if length else b""

        try:
            data = json.loads(raw.decode("utf-8")) if raw else {}
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._send_json({"error": "invalid JSON body"}, status=400)
            return

        urls = data.get("urls")
        fmt = data.get("format", "mp3")
        cookies_browser = data.get("cookies")  # browser name, or None/falsy

        # Validate inputs.
        if not isinstance(urls, list) or not urls:
            self._send_json({"error": "'urls' must be a non-empty list"},
                            status=400)
            return
        if fmt not in ("mp3", "mp4"):
            self._send_json({"error": "'format' must be 'mp3' or 'mp4'"},
                            status=400)
            return
        # Normalize cookies: accept a known browser name, else treat as disabled.
        if isinstance(cookies_browser, str):
            cookies_browser = cookies_browser.strip().lower()
        if cookies_browser not in ALLOWED_BROWSERS:
            cookies_browser = None

        # Create one job + worker thread per URL.
        jobs_out = []
        for url in urls:
            if not isinstance(url, str) or not url.strip():
                continue
            url = url.strip()
            job = new_job(url, fmt, cookies_browser)
            jobs_out.append({"id": job["id"], "url": url})
            worker = threading.Thread(
                target=run_download, args=(job["id"],), daemon=True
            )
            worker.start()

        if not jobs_out:
            self._send_json({"error": "no valid urls provided"}, status=400)
            return

        self._send_json({"jobs": jobs_out})

    def _handle_reveal(self, query):
        """Open Finder at the downloaded file for the given job id (macOS only)."""
        if not CAN_REVEAL:
            self._send_json(
                {"ok": False, "error": "reveal is only supported on the local macOS host"},
                status=400,
            )
            return

        ids = query.get("id")
        job_id = ids[0] if ids else None
        if not job_id:
            self._send_json({"ok": False, "error": "missing id"}, status=400)
            return

        with JOBS_LOCK:
            job = JOBS.get(job_id)
            filename = job["filename"] if job else None

        if not job:
            self._send_json({"ok": False, "error": "unknown job"}, status=404)
            return
        if not filename or not os.path.exists(filename):
            self._send_json({"ok": False, "error": "file not available"},
                            status=404)
            return

        try:
            subprocess.run(["open", "-R", filename], check=False)
        except Exception as exc:  # pragma: no cover - defensive
            self._send_json({"ok": False, "error": str(exc)}, status=500)
            return

        self._send_json({"ok": True})


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def parse_args(argv=None):
    """CLI flags. Each overrides its matching env var / default when given."""
    parser = argparse.ArgumentParser(
        prog="yt-bckp",
        description="Local YouTube downloader (mp3/mp4). Saves to a downloads folder.",
    )
    parser.add_argument(
        "-d", "--dir", metavar="PATH", default=None,
        help="directory to save finished files in "
             "(overrides YT_BCKP_DOWNLOAD_DIR; default: ./downloads)",
    )
    parser.add_argument(
        "--host", metavar="ADDR", default=None,
        help="bind address (overrides YT_BCKP_HOST; default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port", type=int, metavar="N", default=None,
        help="port to listen on (overrides YT_BCKP_PORT; default: 8723)",
    )
    return parser.parse_args(argv)


def main():
    # Resolve configuration. Precedence: CLI flag > env var > default. The env
    # vars were already applied to the module globals at import; a CLI flag, if
    # present, overrides them here (build_command reads these globals at runtime).
    global HOST, PORT, DOWNLOAD_DIR, INCOMPLETE_DIR

    args = parse_args()
    if args.host is not None:
        HOST = args.host
    if args.port is not None:
        PORT = args.port
    if args.dir is not None:
        DOWNLOAD_DIR = os.path.abspath(os.path.expanduser(args.dir))
        INCOMPLETE_DIR = os.path.join(DOWNLOAD_DIR, "_incomplete_")

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(INCOMPLETE_DIR, exist_ok=True)

    server = ThreadingHTTPServer((HOST, PORT), Handler)
    url = f"http://{HOST}:{PORT}/"

    print("=" * 60)
    print(" yt-bckp backend running")
    print(f"   URL:        {url}")
    print(f"   yt-dlp:     {YT_DLP}")
    print(f"   downloads:  {DOWNLOAD_DIR}")
    print("=" * 60)

    # Best-effort: pop the UI open in the default browser (desktop only).
    if OPEN_BROWSER:
        try:
            webbrowser.open(url)
        except Exception:
            pass

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
