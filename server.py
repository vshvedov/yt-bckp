#!/usr/bin/env python3
"""
yt-bckp backend server.

A local, dependency-free YouTube downloader for macOS. Built entirely on the
Python standard library: it serves a single-page frontend and exposes a small
JSON API that drives yt-dlp subprocesses in background threads.

Run with:  python3 server.py   (no args, no deps)

Downloads run through a bounded worker pool (concurrency cap) and job state is
persisted to a small SQLite database, so history survives restarts and jobs that
were in flight are re-queued on the next boot.

Endpoints:
    GET  /                  -> serves index.html
    GET  /api/config        -> { can_reveal, download_dir, versions }
    POST /api/download      -> enqueue downloads onto the worker pool
    POST /api/check         -> { existing: [urls already downloaded for a format] }
    GET  /api/jobs          -> list jobs (newest first)
    GET  /api/events        -> Server-Sent Events stream of live job updates
    GET  /api/reveal?id=ID  -> reveal a finished file in Finder (macOS host)
"""

import argparse
import json
import os
import queue as queue_lib
import re
import shutil
import sqlite3
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

# Path to ffmpeg (the mp3/mp4 encoder yt-dlp shells out to). Same resolution.
FFMPEG = os.environ.get("YT_BCKP_FFMPEG") or "/opt/homebrew/bin/ffmpeg"
if not os.path.exists(FFMPEG):
    FFMPEG = shutil.which("ffmpeg") or FFMPEG

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

# Tool versions, resolved once at first request and cached for the UI.
_VERSIONS = None


def get_versions():
    """Return {'yt_dlp': ..., 'ffmpeg': ...} version strings (or None each)."""
    global _VERSIONS
    if _VERSIONS is not None:
        return _VERSIONS
    yt_dlp_v = ffmpeg_v = None
    try:
        out = subprocess.run([YT_DLP, "--version"], capture_output=True,
                             text=True, timeout=10).stdout.strip()
        yt_dlp_v = out or None
    except Exception:
        pass
    try:
        out = subprocess.run([FFMPEG, "-version"], capture_output=True,
                             text=True, timeout=10).stdout
        first = out.splitlines()[0] if out else ""
        m = re.search(r"ffmpeg version (\S+)", first)
        ffmpeg_v = m.group(1) if m else (first or None)
    except Exception:
        pass
    _VERSIONS = {"yt_dlp": yt_dlp_v, "ffmpeg": ffmpeg_v}
    return _VERSIONS

# Max simultaneous downloads; the rest wait in a queue. Keeping this modest
# avoids hammering the network/CPU and reduces the chance of YouTube throttling
# or "confirm you're not a bot" challenges. Override with --jobs.
MAX_CONCURRENT = int(os.environ.get("YT_BCKP_MAX_CONCURRENT", "3"))

# SQLite job database: durable history + a queue that survives restarts. Default
# is a hidden file INSIDE the download dir (resolved in main(), since --dir can
# change it), so it persists on the mounted volume in a container. Override with
# YT_BCKP_DB. A value of None here means "resolve from the download dir later".
DB_PATH = os.environ.get("YT_BCKP_DB")

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
        "message": None,              # transient phase text (e.g. "Converting to MP3…")
        "title": None,
        "filename": None,
        "error": None,
        "created": time.time(),
    }
    with JOBS_LOCK:
        JOBS[job["id"]] = job
    persist_job(job)
    broadcast()
    return job


def update_job(job_id, **fields):
    """Thread-safe partial update of a job record.

    Durable field changes (status/title/filename/error) are written to the DB;
    progress-only updates stay in memory. Progress is ephemeral runtime state —
    if the server restarts mid-download the job is re-queued and restarts anyway,
    so persisting every percentage tick would only add disk churn. Every update
    (including progress) is pushed live to connected browsers via SSE.
    """
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if job is None:
            return
        # Don't let a late, out-of-order update regress a finished job. The
        # stderr drain thread can deliver a post-processing line just after the
        # main thread set status=done; ignore such stragglers.
        if job.get("status") in ("done", "error") \
                and fields.get("status") not in (None, "done", "error"):
            return
        job.update(fields)
        snapshot = dict(job)
    # progress and message are ephemeral runtime state — skip the DB write for
    # those (status changes that accompany them still trigger a persist).
    if set(fields) - {"progress", "message"}:
        persist_job(snapshot)
    broadcast()


def snapshot_jobs():
    """Return a list copy of all jobs, newest first."""
    with JOBS_LOCK:
        jobs = [dict(j) for j in JOBS.values()]
    jobs.sort(key=lambda j: j["created"], reverse=True)
    return jobs


# --------------------------------------------------------------------------- #
# Persistence (SQLite) + job queue
# --------------------------------------------------------------------------- #

# Bounded work queue + a pool of worker threads provide the concurrency cap:
# at most len(pool) == MAX_CONCURRENT downloads run at once; the rest wait here.
JOB_QUEUE = queue_lib.Queue()

# Server-Sent Events: each connected browser registers a Queue here; broadcast()
# pushes the full jobs snapshot to all of them on every change. This replaces
# client-side polling — updates are live and the connection auto-reconnects.
SSE_LOCK = threading.Lock()
SSE_SUBSCRIBERS = set()


def sse_subscribe():
    """Register a new SSE listener; returns its message queue."""
    q = queue_lib.Queue(maxsize=64)
    with SSE_LOCK:
        SSE_SUBSCRIBERS.add(q)
    return q


def sse_unsubscribe(q):
    with SSE_LOCK:
        SSE_SUBSCRIBERS.discard(q)


def broadcast():
    """Push the current jobs snapshot to every connected SSE client."""
    with SSE_LOCK:
        if not SSE_SUBSCRIBERS:
            return
        subs = list(SSE_SUBSCRIBERS)
    payload = json.dumps({"jobs": snapshot_jobs()})
    for q in subs:
        try:
            q.put_nowait(payload)
        except queue_lib.Full:
            # Slow/stuck client: drop this update rather than block workers.
            # It still gets the next one (each carries the full state).
            pass

DB_LOCK = threading.Lock()
_DB = None  # sqlite3.Connection, set by init_db()
_JOB_COLUMNS = ["id", "url", "format", "cookies", "status", "progress",
                "title", "filename", "error", "created"]


def init_db(path):
    """Open (creating if needed) the SQLite job database."""
    global _DB
    _DB = sqlite3.connect(path, check_same_thread=False)
    _DB.execute(
        "CREATE TABLE IF NOT EXISTS jobs ("
        "id TEXT PRIMARY KEY, url TEXT, format TEXT, cookies TEXT, "
        "status TEXT, progress REAL, title TEXT, filename TEXT, "
        "error TEXT, created REAL)"
    )
    _DB.commit()


def persist_job(job):
    """UPSERT a job row. No-op if the DB isn't initialised (e.g. in unit tests)."""
    if _DB is None:
        return
    values = [job.get(c) for c in _JOB_COLUMNS]
    with DB_LOCK:
        _DB.execute(
            "INSERT INTO jobs (id, url, format, cookies, status, progress, "
            "title, filename, error, created) VALUES (?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET status=excluded.status, "
            "progress=excluded.progress, title=excluded.title, "
            "filename=excluded.filename, error=excluded.error",
            values,
        )
        _DB.commit()


def load_jobs():
    """Return all persisted job records as dicts."""
    if _DB is None:
        return []
    with DB_LOCK:
        cur = _DB.execute(
            "SELECT id, url, format, cookies, status, progress, title, "
            "filename, error, created FROM jobs"
        )
        rows = cur.fetchall()
    return [dict(zip(_JOB_COLUMNS, r)) for r in rows]


def enqueue(job_id):
    """Hand a job id to the worker pool."""
    JOB_QUEUE.put(job_id)


def worker_loop():
    """Pull job ids off the queue and run them one at a time. The number of
    these threads IS the concurrency limit."""
    while True:
        job_id = JOB_QUEUE.get()
        try:
            run_download(job_id)
        except Exception as exc:  # pragma: no cover - defensive
            update_job(job_id, status="error", error=str(exc))
        finally:
            JOB_QUEUE.task_done()


def start_workers(n):
    """Spawn the worker pool (n daemon threads)."""
    for _ in range(max(1, n)):
        threading.Thread(target=worker_loop, daemon=True).start()


def recover_jobs():
    """Load persisted jobs into memory at startup and re-queue any that were
    still queued / downloading when the server last stopped. Returns
    (loaded_count, requeued_count)."""
    rows = load_jobs()
    requeue = []
    with JOBS_LOCK:
        for j in rows:
            JOBS[j["id"]] = j
            if j["status"] in ("queued", "downloading", "processing"):
                j["status"] = "queued"
                j["progress"] = 0
                j["message"] = None
                requeue.append(j["id"])
    for jid in requeue:
        with JOBS_LOCK:
            snapshot = dict(JOBS[jid])
        persist_job(snapshot)
        enqueue(jid)
    return len(rows), len(requeue)


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
# Post-processing events: "PP <status> <PostprocessorName>" (e.g. PP started
# ExtractAudio). yt-dlp does NOT expose a percentage for these, so we show a
# phase message with an indeterminate bar instead of a number.
PP_RE = re.compile(r"^PP\s+(\w+)\s+(\w+)")
# Friendly message per postprocessor. Only the slow ones (re-encode / merge) are
# listed; fast/instant steps (MoveFiles, Metadata, …) are ignored to avoid flicker.
PP_MESSAGES = {
    "ExtractAudio": "Converting to MP3…",
    "VideoConvertor": "Converting video…",
    "Merger": "Merging audio + video…",
    "VideoRemuxer": "Finalizing video…",
    "FFmpegVideoConvertor": "Converting video…",
}
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
        # Post-processing phase events (audio extract / merge), so the UI can
        # show "Converting…" instead of looking stuck at 100%.
        "--progress-template",
        "postprocess:PP %(progress.status)s %(progress.postprocessor)s",
        # Emit the title up front and the final on-disk path after the move.
        "--print", "before_dl:TITLE %(title)s",
        "--print", "after_move:DONE %(filepath)s",
        "-P", "home:" + DOWNLOAD_DIR,     # finished files are moved here
        "-P", "temp:" + INCOMPLETE_DIR,   # all intermediate work staged here
        "-o", OUTPUT_TEMPLATE,
        # Embed the video thumbnail as cover art and write title/artist/etc. tags
        # into the output file (needs ffmpeg, which we already require).
        "--embed-thumbnail",
        "--embed-metadata",
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
    # yt-dlp emits POST-PROCESSING progress (audio extract / merge) on stderr,
    # so we watch for the PP marker here too — not just on stdout.
    def drain_stderr():
        for line in proc.stderr:
            line = line.rstrip("\n")
            if not line:
                continue
            m = PP_RE.search(line)
            if m:
                pp_status, pp_name = m.group(1), m.group(2)
                msg = PP_MESSAGES.get(pp_name)
                if msg and pp_status in ("started", "processing"):
                    update_job(job_id, status="processing", message=msg)
                continue
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

        # 3b) Post-processing phase (audio extract / merge). No percentage is
        # available, so we switch to a "processing" status with a phase message.
        m = PP_RE.search(line)
        if m:
            pp_status, pp_name = m.group(1), m.group(2)
            msg = PP_MESSAGES.get(pp_name)
            if msg and pp_status in ("started", "processing"):
                update_job(job_id, status="processing", message=msg)
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
            message=None,
            title=title,
            filename=filename,
            error=None,
        )
    else:
        tail = "\n".join(stderr_tail[-10:]).strip()
        update_job(
            job_id,
            status="error",
            message=None,
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
            self._send_json({
                "can_reveal": CAN_REVEAL,
                "download_dir": DOWNLOAD_DIR,
                "versions": get_versions(),
            })
        elif path == "/api/jobs":
            self._send_json({"jobs": snapshot_jobs()})
        elif path == "/api/events":
            self._handle_events()
        elif path == "/api/reveal":
            self._handle_reveal(parse_qs(parsed.query))
        else:
            self._send_json({"error": "not found"}, status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/download":
            self._handle_download()
        elif parsed.path == "/api/check":
            self._handle_check()
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

    def _read_json_body(self):
        """Read and parse a JSON request body. Returns (data, error_sent)."""
        try:
            length = int(self.headers.get("Content-Length", 0))
        except (TypeError, ValueError):
            length = 0
        raw = self.rfile.read(length) if length else b""
        try:
            return (json.loads(raw.decode("utf-8")) if raw else {}), False
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._send_json({"error": "invalid JSON body"}, status=400)
            return None, True

    def _already_downloaded(self, fmt):
        """Return a set of URLs that already have a completed file on disk for
        the given format (used to warn before re-downloading)."""
        done = set()
        for j in snapshot_jobs():
            if (j["status"] == "done" and j.get("format") == fmt
                    and j.get("filename") and os.path.exists(j["filename"])):
                done.add(j["url"])
        return done

    def _handle_check(self):
        """Report which of the given URLs were already downloaded for this format.
        Lets the UI ask the user to confirm before overwriting."""
        data, sent = self._read_json_body()
        if sent:
            return
        urls = data.get("urls")
        fmt = data.get("format", "mp3")
        if not isinstance(urls, list):
            self._send_json({"error": "'urls' must be a list"}, status=400)
            return
        done = self._already_downloaded(fmt)
        existing = [u.strip() for u in urls
                    if isinstance(u, str) and u.strip() in done]
        self._send_json({"existing": existing})

    def _handle_download(self):
        """Parse the request body, create jobs, and spawn worker threads."""
        data, sent = self._read_json_body()
        if sent:
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

        # Create one job per URL and hand it to the worker pool. The pool
        # enforces the concurrency cap; extra jobs wait in the queue as "queued".
        jobs_out = []
        for url in urls:
            if not isinstance(url, str) or not url.strip():
                continue
            url = url.strip()
            job = new_job(url, fmt, cookies_browser)
            jobs_out.append({"id": job["id"], "url": url})
            enqueue(job["id"])

        if not jobs_out:
            self._send_json({"error": "no valid urls provided"}, status=400)
            return

        self._send_json({"jobs": jobs_out})

    def _handle_events(self):
        """Server-Sent Events stream: pushes the jobs snapshot on every change.

        Sends the current state immediately, then one message per change. A
        periodic comment keeps the connection alive through idle periods and lets
        us detect a disconnected client (the write raises, we clean up).
        """
        q = sse_subscribe()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")  # disable proxy buffering
            self.end_headers()

            # Initial snapshot so a fresh page renders without waiting for a change.
            self._sse_write("data: %s\n\n" % json.dumps({"jobs": snapshot_jobs()}))

            while True:
                try:
                    payload = q.get(timeout=15)
                    self._sse_write("data: %s\n\n" % payload)
                except queue_lib.Empty:
                    # Heartbeat comment; also surfaces a broken pipe so we exit.
                    self._sse_write(": keep-alive\n\n")
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass  # client disconnected — normal
        finally:
            sse_unsubscribe(q)

    def _sse_write(self, text):
        self.wfile.write(text.encode("utf-8"))
        self.wfile.flush()

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
    parser.add_argument(
        "-j", "--jobs", type=int, metavar="N", default=None,
        help="max simultaneous downloads "
             "(overrides YT_BCKP_MAX_CONCURRENT; default: 3)",
    )
    parser.add_argument(
        "--db", metavar="PATH", default=None,
        help="path to the job database "
             "(overrides YT_BCKP_DB; default: <download-dir>/.yt-bckp.db)",
    )
    return parser.parse_args(argv)


def main():
    # Resolve configuration. Precedence: CLI flag > env var > default. The env
    # vars were already applied to the module globals at import; a CLI flag, if
    # present, overrides them here (build_command reads these globals at runtime).
    global HOST, PORT, DOWNLOAD_DIR, INCOMPLETE_DIR, MAX_CONCURRENT, DB_PATH

    args = parse_args()
    if args.host is not None:
        HOST = args.host
    if args.port is not None:
        PORT = args.port
    if args.dir is not None:
        DOWNLOAD_DIR = os.path.abspath(os.path.expanduser(args.dir))
        INCOMPLETE_DIR = os.path.join(DOWNLOAD_DIR, "_incomplete_")
    if args.jobs is not None:
        MAX_CONCURRENT = args.jobs
    if args.db is not None:
        DB_PATH = os.path.abspath(os.path.expanduser(args.db))

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(INCOMPLETE_DIR, exist_ok=True)

    # Default the DB to a hidden file inside the (now resolved) download dir.
    if DB_PATH is None:
        DB_PATH = os.path.join(DOWNLOAD_DIR, ".yt-bckp.db")

    # Bring up persistence and the worker pool, then recover any unfinished jobs.
    init_db(DB_PATH)
    loaded, requeued = recover_jobs()
    start_workers(MAX_CONCURRENT)

    server = ThreadingHTTPServer((HOST, PORT), Handler)
    url = f"http://{HOST}:{PORT}/"

    print("=" * 60)
    print(" yt-bckp backend running")
    print(f"   URL:         {url}")
    print(f"   yt-dlp:      {YT_DLP}")
    print(f"   downloads:   {DOWNLOAD_DIR}")
    print(f"   database:    {DB_PATH}")
    print(f"   concurrency: {MAX_CONCURRENT}")
    if loaded:
        print(f"   recovered:   {loaded} job(s) from history, "
              f"{requeued} re-queued")
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
