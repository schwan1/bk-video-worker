#!/usr/bin/env python3
"""
Bright Kids AI -- Railway-hosted NotebookLM video worker.

Runs in a persistent Docker container on Railway.
Polls Supabase every 5 min (via entrypoint.sh loop) for:
  - status='queued'  -> fire NotebookLM cinematic video job
  - status='processing' (with notebook_id set) -> try to download + upload to Supabase Storage

Required env vars (set in Railway dashboard):
  NEXT_PUBLIC_SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY
  NOTEBOOKLM_STORAGE_STATE_B64   (base64-encoded storage_state.json)
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID
"""

import asyncio
import base64
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import httpx

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SUPABASE_URL     = os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY     = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
TG_TOKEN         = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID       = os.environ.get("TELEGRAM_CHAT_ID", "")
SESSION_B64      = os.environ.get("NOTEBOOKLM_STORAGE_STATE_B64", "")
OUTPUT_DIR       = Path("/tmp/bk_videos")
STORAGE_PATH     = Path("/root/.notebooklm/storage_state.json")

VIDEO_INSTRUCTION = (
    "Create an engaging, warm cinematic overview for parents of neurodiverse children. "
    "Tone: empathetic, clear, and reassuring. Focus on practical takeaways and specific benefits. "
    "Brand: Bright Kids AI -- empowering every bright mind. "
    "Slant the content toward how Bright Kids AI helps children and families in a genuine, "
    "non-pushy way that builds trust and encourages parents to learn more."
)

# ---------------------------------------------------------------------------
# Bootstrap: write NotebookLM session from env var
# ---------------------------------------------------------------------------

def bootstrap_session():
    if not SESSION_B64:
        raise RuntimeError("NOTEBOOKLM_STORAGE_STATE_B64 env var is not set.")
    STORAGE_PATH.parent.mkdir(parents=True, exist_ok=True)
    decoded = base64.b64decode(SESSION_B64).decode("utf-8")
    STORAGE_PATH.write_text(decoded)
    log(f"NotebookLM session written to {STORAGE_PATH}")


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def notify(msg: str):
    if not TG_TOKEN or not TG_CHAT_ID:
        log(f"[NOTIFY - no TG config] {msg}")
        return
    try:
        httpx.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": msg},
            timeout=10,
        )
    except Exception as e:
        log(f"Telegram notify failed: {e}")


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

def _headers() -> dict:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }


def supa_get(table: str, params: dict) -> list:
    r = httpx.get(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=_headers(),
        params=params,
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


def supa_patch(table: str, match: dict, data: dict):
    qs = {k: f"eq.{v}" for k, v in match.items()}
    r = httpx.patch(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**_headers(), "Prefer": "return=minimal"},
        params=qs,
        json=data,
        timeout=15,
    )
    r.raise_for_status()


def supabase_upload_video(local_path: str, job_id: str) -> str:
    """
    Upload video to Supabase Storage bucket 'video-jobs'.
    Returns the public URL.
    """
    file_name = f"{job_id}.mp4"
    storage_url = f"{SUPABASE_URL}/storage/v1/object/video-jobs/{file_name}"

    with open(local_path, "rb") as f:
        r = httpx.put(
            storage_url,
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "video/mp4",
                "x-upsert": "true",
            },
            content=f.read(),
            timeout=120,
        )
        r.raise_for_status()

    # Return public URL
    public_url = f"{SUPABASE_URL}/storage/v1/object/public/video-jobs/{file_name}"
    return public_url


# ---------------------------------------------------------------------------
# Phase 1: process queued jobs
# ---------------------------------------------------------------------------

async def process_queued_jobs():
    if not SUPABASE_URL or not SUPABASE_KEY:
        log("ERROR: Missing Supabase env vars.")
        return

    jobs = supa_get("video_jobs", {
        "status": "eq.queued",
        "limit": "3",
        "order": "created_at.asc",
    })

    if not jobs:
        log("No queued jobs.")
        return

    log(f"Found {len(jobs)} queued job(s) to start.")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    from notebooklm import NotebookLMClient  # type: ignore

    async with await NotebookLMClient.from_storage() as client:
        for job in jobs:
            job_id     = job["id"]
            post_title = job.get("blog_post_title", "Untitled")
            post_url   = job.get("blog_post_url", "")
            post_text  = job.get("blog_post_content", "")

            log(f"  Starting job {job_id[:8]} -- '{post_title}'")
            supa_patch("video_jobs", {"id": job_id}, {
                "status": "processing",
                "updated_at": now_iso(),
            })

            try:
                # Create notebook
                timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                nb_title  = f"BK_{post_title[:30]}_{timestamp}"
                nb        = await client.notebooks.create(nb_title)
                nb_id     = nb.id
                log(f"    Notebook created: {nb_id}")

                # Add source -- always use stored text content first (avoids 404 risk).
                # Text content is already in Supabase from when the job was queued.
                # URL is added as a second source only if text is short/missing.
                if post_text and len(post_text.strip()) > 100:
                    await client.sources.add_text(nb_id, post_title, post_text, wait=True)
                    log(f"    Text indexed ({len(post_text)} chars).")
                    # Also add URL as supplementary source if available
                    if post_url:
                        try:
                            await client.sources.add_url(nb_id, post_url, wait=True)
                            log(f"    URL also indexed: {post_url}")
                        except Exception as url_err:
                            log(f"    URL index skipped (non-fatal): {url_err}")
                elif post_url:
                    await client.sources.add_url(nb_id, post_url, wait=True)
                    log(f"    URL indexed: {post_url}")
                else:
                    raise ValueError("No source content (no text or URL).")

                # Fire video generation (standard Video Overview -- works with Pro)
                status  = await client.artifacts.generate_video(
                    nb_id,
                    instructions=VIDEO_INSTRUCTION,
                )
                task_id = status.task_id
                log(f"    Video job started: task_id={task_id}")

                supa_patch("video_jobs", {"id": job_id}, {
                    "notebook_id": nb_id,
                    "task_id":     task_id,
                    "status":      "processing",
                    "updated_at":  now_iso(),
                })

                nb_url = f"https://notebooklm.google.com/notebook/{nb_id}"
                notify(f"BK video started 🎬\n'{post_title}'\nWatch progress in Studio:\n{nb_url}\nChecking every 5 min...")

            except Exception as e:
                log(f"    FAILED: {e}")
                supa_patch("video_jobs", {"id": job_id}, {
                    "status":        "failed",
                    "error_message": str(e)[:500],
                    "updated_at":    now_iso(),
                })
                notify(f"BK video FAILED to start: '{post_title}'\n{str(e)[:100]}")


# ---------------------------------------------------------------------------
# Phase 2: check in-flight jobs (processing + notebook_id set)
# ---------------------------------------------------------------------------

def _poll_video_statuses(jobs: list, cookies: list) -> dict:
    """
    Synchronous helper (runs in executor thread).
    Uses notebooklm_tools.poll_studio_status to check each notebook.
    Returns dict keyed by notebook_id:
      - None               -> still in_progress
      - {"video_url": ...} -> completed, ready to download
      - {"_error": ...}    -> poll failed
    """
    from notebooklm_tools.core.client import NotebookLMClient as NLMToolsClient  # type: ignore

    results: dict = {}
    with NLMToolsClient(cookies=cookies) as client:
        for job in jobs:
            nb_id = job["notebook_id"]
            try:
                artifacts = client.poll_studio_status(nb_id)
                video = next(
                    (a for a in artifacts
                     if a.get("type") == "video"
                     and a.get("status") == "completed"
                     and a.get("video_url")),
                    None,
                )
                results[nb_id] = video  # None means still in progress
            except Exception as e:
                results[nb_id] = {"_error": str(e)}
    return results


async def check_processing_jobs():
    # Fetch jobs that are processing and have a notebook_id (i.e., video was fired)
    jobs = supa_get("video_jobs", {
        "status":      "eq.processing",
        "notebook_id": "not.is.null",
        "limit":       "10",
        "order":       "created_at.asc",
    })

    if not jobs:
        log("No in-flight jobs to check.")
        return

    log(f"Checking {len(jobs)} in-flight job(s)...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load Playwright cookies from the bootstrapped storage_state.json
    storage_state = json.loads(STORAGE_PATH.read_text())
    cookies = storage_state.get("cookies", [])

    # Poll all notebooks in one sync session (avoids repeated CSRF refresh overhead)
    try:
        statuses = await asyncio.get_event_loop().run_in_executor(
            None, _poll_video_statuses, jobs, cookies
        )
    except Exception as e:
        log(f"  ERROR polling NotebookLM: {e}")
        return

    for job in jobs:
        job_id   = job["id"]
        nb_id    = job["notebook_id"]
        title    = job.get("blog_post_title", "Untitled")
        out_path = str(OUTPUT_DIR / f"{job_id}.mp4")

        log(f"  Checking '{title}' (job {job_id[:8]})...")

        artifact = statuses.get(nb_id)

        if artifact is None:
            log(f"    Still processing -- will check again next run.")
            continue

        if "_error" in artifact:
            err = artifact["_error"]
            log(f"    FAILED to poll status: {err}")
            supa_patch("video_jobs", {"id": job_id}, {
                "status":        "failed",
                "error_message": err[:500],
                "updated_at":    now_iso(),
            })
            notify(f"BK video FAILED (poll error): '{title}'\n{err[:120]}")
            continue

        # Video is complete -- download the signed CDN URL (no auth needed)
        video_cdn_url = artifact["video_url"]
        log(f"    Video completed! Downloading from CDN...")

        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=120) as http:
                async with http.stream("GET", video_cdn_url) as resp:
                    resp.raise_for_status()
                    with open(out_path, "wb") as f:
                        async for chunk in resp.aiter_bytes(65536):
                            f.write(chunk)

            log(f"    Downloaded: {out_path}")

            # Upload to Supabase Storage
            video_url = supabase_upload_video(out_path, job_id)
            log(f"    Uploaded to Supabase Storage: {video_url}")

            supa_patch("video_jobs", {"id": job_id}, {
                "status":     "done",
                "video_url":  video_url,
                "video_path": out_path,
                "updated_at": now_iso(),
            })

            # Clean up local file
            Path(out_path).unlink(missing_ok=True)

            notify(
                f"BK video ready! 🎬\n"
                f"'{title}'\n"
                f"Open in admin → Blog → click the red YouTube icon"
            )

        except Exception as e:
            log(f"    FAILED to download/upload: {e}")
            supa_patch("video_jobs", {"id": job_id}, {
                "status":        "failed",
                "error_message": str(e)[:500],
                "updated_at":    now_iso(),
            })
            notify(f"BK video FAILED: '{title}'\n{str(e)[:120]}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    log("=" * 55)
    log("BK Video Worker (Railway) -- start")

    bootstrap_session()

    await process_queued_jobs()
    await check_processing_jobs()

    log("BK Video Worker -- done")
    log("=" * 55)


if __name__ == "__main__":
    asyncio.run(main())
