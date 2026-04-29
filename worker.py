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
  GEMINI_API_KEY                 (for bridge doc generation)
"""

import asyncio
import base64
import json
import os
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
GEMINI_API_KEY   = os.environ.get("GEMINI_API_KEY", "")
OUTPUT_DIR       = Path("/tmp/bk_videos")
STORAGE_PATH     = Path("/root/.notebooklm/storage_state.json")

VIDEO_INSTRUCTION = (
    "Create an engaging, warm cinematic overview for parents of neurodiverse children. "
    "Tone: empathetic, clear, and reassuring. Focus on practical takeaways and specific benefits. "
    "Brand: Bright Kids AI -- empowering every bright mind. "
    "The third source describes the Bright Kids AI tools that families are already using. "
    "Where it fits naturally -- not as a pitch, but as a 'families are finding this helps' moment -- "
    "name one or two of those tools by name and show how they connect to what this article covers. "
    "If a connection does not feel genuine, leave it out entirely. "
    "CRITICAL TEXT REQUIREMENT: Every text overlay, caption, callout bubble, label, and on-screen "
    "graphic must contain clear, complete, fully-visible text. Never leave a text field, speech "
    "bubble, or placeholder empty or blank. If a visual element is shown, it must display "
    "readable, meaningful text that is relevant to the narration at that moment."
)

# ---------------------------------------------------------------------------
# Bright Kids AI ecosystem context (added to every notebook as a source)
# ---------------------------------------------------------------------------

BK_ECOSYSTEM_CONTEXT = """\
Bright Kids AI — Tools Families Are Using

This document describes the tools available inside Bright Kids AI, written from
the perspective of how parents and children actually use them day to day.

---

MY STARS (Emotional Check-In)

My Stars is a visual check-in tool built around four Star States — a simple
framework that replaces the need for words when a child is overwhelmed. Each
state has its own color, emoji, and name:

  Sleepy Star (violet / 😴) — low energy, tired, disconnected
  Bright Star (teal / ⭐) — calm, focused, ready to learn
  Zoom Star (amber / 💫) — excited, buzzing, high energy
  Big Boom (orange / 💥) — overwhelmed, dysregulated, flooded

When a child opens My Stars, they see their own custom avatar and tap the star
that matches how they feel. The key principle: all four states are okay. The
goal is not to fix the state but to name it, because named feelings are easier
to work with. The tool works for children who cannot yet read and for children
of any age — it removes the need for verbal emotional vocabulary in the moment.

Parents often use it at predictable transition points: arriving home from
school, before homework, after therapy appointments, or at bedtime.

---

WEEKLY BRIEF (Pattern Recognition)

After seven consecutive check-ins, parents see a Weekly Brief — a visual
breakdown showing which star states appeared most often and when. This is not
about frequency counts; it is about pattern recognition. Parents start noticing
that Big Boom clusters after school on Tuesdays, or that Sleepy Star appears
every morning before the child has eaten. Once a pattern is visible, it becomes
actionable. The Weekly Brief turns daily micro-observations into a picture a
parent can bring to a therapist, teacher, or IEP meeting.

---

PERSONALIZED STORIES

Personalized storybooks place the child as the main character in every story.
The child's name, interests, favorite toy, reading level, and current learning
goals are all woven into the narrative and illustrations. Stories are generated
by AI but feel handcrafted because they are built entirely from what the parent
has shared about their specific child.

For neurodiverse children, seeing themselves represented in stories — not as a
character who overcomes a disability, but as the hero doing something they love
— has a different quality than reading about a generic protagonist. Parents
report children asking to hear the same story repeatedly, which is itself a
form of emotional processing.

Stories can be read aloud in the Explorer app with audio narration, making them
accessible to pre-readers and children who process better through listening.

---

PERSONALIZED SONGS

Original songs are generated using the child's name, favorite things, and
current themes the parent wants to reinforce (a new school routine, learning to
ask for help, celebrating a milestone). Songs are short, melodic, and designed
to be played on repeat — which is exactly how many children with sensory
processing differences or ADHD naturally engage with music.

Parents use songs as transition anchors: a specific song signals that it is time
to get ready for school, or that the bedtime routine is starting. Because the
song uses the child's name and references their actual interests, it cuts
through in a way that a generic transition cue does not.

---

JENNY (Parenting Guide)

Jenny is a warm, conversational guide inside the platform for parents — not for
children. Parents can ask her about what they are observing, describe a
situation they are stuck on, or ask for a recommendation. She responds in two
to three sentences: enough to be useful, short enough not to overwhelm.

Jenny is designed for the moment between noticing something and knowing what to
do about it. She does not replace therapists or clinicians. She is the voice
that helps a parent feel less alone at 10pm when their child had a hard day and
they do not know what to try next.

---

CHILD PROFILE

Every tool in Bright Kids AI is driven by a child profile that the parent builds
with the help of Sunny, a profile creation assistant. The profile captures first
name, age, reading level, interests, favorite toy, and a description of the
child's appearance for illustrations. This profile is what makes every story,
song, and check-in feel personal rather than generic.

Parents update profiles as children grow. A reading level that was accurate at
age five needs revisiting at age seven. Adding a new interest unlocks new story
themes. The profile is not a diagnostic record — it is a creative brief that
the AI uses to build something that feels like it was made for this one child.
"""

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
# Gemini: generate per-post bridge document
# ---------------------------------------------------------------------------

def generate_bridge_doc(post_title: str, post_text: str) -> str:
    """
    Calls Gemini Flash to produce a short document connecting this blog post
    to 1-2 specific Bright Kids AI tools. Returns empty string on any failure.
    """
    if not GEMINI_API_KEY:
        log("    No GEMINI_API_KEY -- skipping bridge doc.")
        return ""

    try:
        import google.generativeai as genai  # type: ignore
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-2.0-flash")

        prompt = f"""\
You are helping create context for a NotebookLM video about a Bright Kids AI blog post.

The blog post is titled: "{post_title}"

Here is an excerpt from the post (first 1500 characters):
---
{post_text[:1500]}
---

Bright Kids AI offers these tools for families of neurodiverse children:
- My Stars: visual emotional check-in using four Star States (Sleepy Star, Bright Star, Zoom Star, Big Boom)
- Weekly Brief: pattern view of check-ins over 7 days, helps parents spot emotional triggers
- Personalized Stories: AI-generated storybooks where the child is the main character
- Personalized Songs: original songs using the child's name and interests as transition anchors
- Jenny: a conversational parenting guide inside the app (short, warm responses for parents)
- Child Profile: built with Sunny, drives all personalization (name, age, reading level, interests, favorite toy)

Write a 150-200 word document (not a list, flowing prose) from a parent's perspective that explains:
1. Which 1-2 of the above tools most naturally connect to what this blog post covers, and why
2. How a parent would realistically use that tool alongside the strategies in this post during an actual week

Rules:
- Do not pitch or sell. Write as if describing what another parent already does.
- Only reference tools that genuinely connect to the post topic. If none connect naturally, say so briefly and return only that sentence.
- Do not mention prices, subscriptions, or calls to action.
- Keep it under 200 words.
"""

        response = model.generate_content(prompt)
        text = response.text.strip()
        log(f"    Bridge doc generated ({len(text)} chars).")
        return text

    except Exception as e:
        log(f"    Bridge doc generation failed (non-fatal): {e}")
        return ""


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
            timeout=300,
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

                # Source 1: blog post text (primary)
                if post_text and len(post_text.strip()) > 100:
                    await client.sources.add_text(nb_id, post_title, post_text, wait=True)
                    log(f"    Blog text indexed ({len(post_text)} chars).")
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

                # Source 2: static BK ecosystem overview (added to every notebook)
                try:
                    await client.sources.add_text(
                        nb_id,
                        "Bright Kids AI — Tools Families Are Using",
                        BK_ECOSYSTEM_CONTEXT,
                        wait=True,
                    )
                    log(f"    Ecosystem context indexed.")
                except Exception as eco_err:
                    log(f"    Ecosystem source skipped (non-fatal): {eco_err}")

                # Source 3: Gemini-generated bridge connecting this post to BK tools
                bridge_text = generate_bridge_doc(post_title, post_text)
                if bridge_text:
                    try:
                        await client.sources.add_text(
                            nb_id,
                            f"How '{post_title[:50]}' Connects to Bright Kids AI",
                            bridge_text,
                            wait=True,
                        )
                        log(f"    Bridge doc indexed.")
                    except Exception as bridge_err:
                        log(f"    Bridge source skipped (non-fatal): {bridge_err}")

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
                completed = next(
                    (a for a in artifacts
                     if a.get("type") == "video"
                     and a.get("status") == "completed"
                     and a.get("video_url")),
                    None,
                )
                if completed:
                    results[nb_id] = completed
                else:
                    nlm_failed = next(
                        (a for a in artifacts
                         if a.get("type") == "video"
                         and a.get("status") == "failed"),
                        None,
                    )
                    results[nb_id] = {"_nlm_failed": True} if nlm_failed else None
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

        if "_nlm_failed" in artifact:
            log(f"    NotebookLM video generation FAILED -- resetting to queued for retry.")
            supa_patch("video_jobs", {"id": job_id}, {
                "status":        "queued",
                "notebook_id":   None,
                "task_id":       None,
                "error_message": "NotebookLM video generation failed -- retrying with new notebook",
                "updated_at":    now_iso(),
            })
            notify(f"BK video gen failed in NLM, retrying: '{title}'")
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

        # Video is complete -- download using Google session cookies so the CDN
        # serves the actual MP4 rather than an auth redirect page.
        video_cdn_url = artifact["video_url"]
        log(f"    Video completed! Downloading from CDN...")

        # Build httpx cookie jar from Playwright storage_state cookies
        cookie_jar = httpx.Cookies()
        for ck in cookies:
            try:
                cookie_jar.set(ck["name"], ck["value"], domain=ck.get("domain", ""))
            except Exception:
                pass

        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=300,
                cookies=cookie_jar,
                headers={"User-Agent": "Mozilla/5.0"},
            ) as http:
                async with http.stream("GET", video_cdn_url) as resp:
                    resp.raise_for_status()
                    with open(out_path, "wb") as f:
                        async for chunk in resp.aiter_bytes(65536):
                            f.write(chunk)

            file_size = Path(out_path).stat().st_size
            log(f"    Downloaded: {out_path} ({file_size / 1_048_576:.1f} MB)")
            if file_size < 1_000_000:
                raise ValueError(
                    f"Downloaded file is only {file_size} bytes -- likely an auth redirect, not a real video."
                )

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
