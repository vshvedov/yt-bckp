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

# Output template shared by both formats. yt-dlp expands %(title)s / %(ext)s.
OUTPUT_TEMPLATE = os.path.join(DOWNLOAD_DIR, "%(title)s.%(ext)s")

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

# "[download]  42.3% of  10.50MiB at ..." -> capture the float percentage.
PROGRESS_RE = re.compile(r"\[download\]\s+([0-9]+(?:\.[0-9]+)?)%")

# Lines that announce where the *final* file lives. yt-dlp prints these after
# post-processing, so they are more reliable than the JSON for the on-disk name.
DESTINATION_RE = re.compile(r"\[download\] Destination:\s*(.+)$")
EXTRACT_AUDIO_RE = re.compile(r"\[ExtractAudio\] Destination:\s*(.+)$")
MERGER_RE = re.compile(r'\[Merger\] Merging formats into "(.+)"')
# yt-dlp may report "already been downloaded" instead of re-downloading.
ALREADY_RE = re.compile(r"\[download\]\s*(.+?)\s+has already been downloaded")


def build_command(url, fmt, cookies_browser=None):
    """Return the yt-dlp argv list for the requested format."""
    base = [
        YT_DLP,
        "--newline",            # emit progress on its own line so we can parse it
        "--print-json",         # dump the info dict (one JSON object) to stdout
        "--no-simulate",        # --print-json implies simulate; force a real download
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
    info_json = None
    download_dest = None      # raw container before post-processing
    final_dest = None         # post-processed final file (mp3 / merged mp4)
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

    # Parse stdout line by line.
    for raw in proc.stdout:
        line = raw.rstrip("\n")
        if not line:
            continue

        # 1) Progress percentage.
        m = PROGRESS_RE.search(line)
        if m:
            try:
                update_job(job_id, progress=float(m.group(1)))
            except ValueError:
                pass
            continue

        # 2) Raw download destination (pre post-processing).
        m = DESTINATION_RE.search(line)
        if m:
            download_dest = m.group(1).strip()
            if title is None:
                title = os.path.splitext(os.path.basename(download_dest))[0]
            continue

        # 3) Already-downloaded shortcut: treat as the final file.
        m = ALREADY_RE.search(line)
        if m:
            final_dest = m.group(1).strip()
            update_job(job_id, progress=100)
            continue

        # 4) Post-processed audio destination (mp3).
        m = EXTRACT_AUDIO_RE.search(line)
        if m:
            final_dest = m.group(1).strip()
            continue

        # 5) Merged output destination (mp4).
        m = MERGER_RE.search(line)
        if m:
            final_dest = m.group(1).strip()
            continue

        # 6) The info JSON dump (a single object printed on one line).
        if line.startswith("{") and line.endswith("}"):
            try:
                info_json = json.loads(line)
                if info_json.get("title"):
                    title = info_json["title"]
                    update_job(job_id, title=title)
            except json.JSONDecodeError:
                pass
            continue

    proc.stdout.close()
    returncode = proc.wait()
    err_thread.join(timeout=2)

    # ----------------------------------------------------------------------- #
    # Resolve the final on-disk filename.
    # ----------------------------------------------------------------------- #
    filename = final_dest or download_dest

    # If we only saw the pre-processing path, correct the extension to match the
    # requested format (yt-dlp rewrites it during post-processing).
    if filename:
        wanted_ext = ".mp3" if fmt == "mp3" else ".mp4"
        base_no_ext = os.path.splitext(filename)[0]
        candidate = base_no_ext + wanted_ext
        if os.path.exists(candidate):
            filename = candidate

    # Last resort: derive the path from the JSON info dict.
    if (not filename or not os.path.exists(filename)) and info_json:
        guess_title = info_json.get("title")
        if guess_title:
            ext = ".mp3" if fmt == "mp3" else ".mp4"
            candidate = os.path.join(DOWNLOAD_DIR, guess_title + ext)
            if os.path.exists(candidate):
                filename = candidate

    if filename:
        filename = os.path.abspath(filename)

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


def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

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
