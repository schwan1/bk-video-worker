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
def _envstr(name: str) -> str:
    """Read env var and strip surrounding whitespace/newlines (Railway sometimes adds them)."""
    return os.environ.get(name, "").strip()

SUPABASE_URL     = _envstr("NEXT_PUBLIC_SUPABASE_URL").rstrip("/")
SUPABASE_KEY     = _envstr("SUPABASE_SERVICE_ROLE_KEY")
TG_TOKEN         = _envstr("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID       = _envstr("TELEGRAM_CHAT_ID")
SESSION_B64      = _envstr("NOTEBOOKLM_STORAGE_STATE_B64")
GEMINI_API_KEY   = _envstr("GEMINI_API_KEY")
YOUTUBE_TOKEN_B64 = _envstr("YOUTUBE_TOKEN_B64")
# BK outro card -- fallback to known Supabase Storage URL if env var is empty.
# This was added because Railway has occasionally failed to inject BK_OUTRO_URL
# into the running container even though the variable is set in the dashboard.
_BK_OUTRO_FALLBACK = "https://xcembolzsphaddfcinxc.supabase.co/storage/v1/object/public/assets/bk_outro_card.png"
BK_OUTRO_URL     = _envstr("BK_OUTRO_URL") or _BK_OUTRO_FALLBACK
OUTPUT_DIR       = Path("/tmp/bk_videos")
STORAGE_PATH     = Path("/root/.notebooklm/storage_state.json")
YOUTUBE_TOKEN_PATH = Path("/root/.config/youtube/token.json")

# Try these Gemini models in order until one succeeds (model availability varies by API key)
GEMINI_MODEL_FALLBACKS = [
    "gemini-2.5-flash",
    "gemini-2.5-flash-preview-04-17",
    "gemini-2.0-flash-001",
    "gemini-1.5-flash-002",
    "gemini-1.5-flash",
]

VIDEO_INSTRUCTION = (
    "Create an engaging cinematic video for parents of neurodiverse children. "
    "NARRATIVE STRUCTURE -- follow this arc exactly: "
    "(1) OPEN with the specific parent problem this blog post addresses. Name the scenario "
    "concretely -- a moment a parent will immediately recognise ('Your child falls apart every "
    "morning before school. You've tried everything. Here's what the research actually shows.'). "
    "Do NOT open with a general empowerment statement or a definition. "
    "(2) REFRAME -- introduce the counterintuitive or surprising finding that changes how the "
    "parent understands the situation. This is the hook. Make it clear and prominent. "
    "(3) EVIDENCE -- walk through what the research shows. Be specific: name the mechanism, "
    "the study finding, or the expert insight. One concrete data point beats three vague claims. "
    "(4) TACTICS -- show 2-3 specific things a parent can try this week. Not general advice -- "
    "actual steps tied to the reframe. "
    "(5) BRIGHT KIDS CONNECTION -- the third source describes Bright Kids AI tools. "
    "Reference 1-2 of them by name ONLY where they directly address the specific problem in "
    "this video. Write it as 'families are finding this helps' -- never a pitch, never a "
    "call to action. If no tool fits naturally, skip this section entirely. "
    "(6) CLOSE with one sentence that shifts the parent's perspective -- something that turns "
    "guilt or frustration into understanding. "
    "Tone: warm and direct. Speak to the tired parent at 11pm, not a conference audience. "
    "OPENING TITLE SLIDE: The very first slide must display the BLOG POST TITLE as the main, "
    "prominent heading. Add 'produced by Bright Kids AI' as a small subtitle line. "
    "The blog post title is always the headline; Bright Kids AI is only the producer credit. "
    "CRITICAL TEXT REQUIREMENT: Every text overlay, caption, callout bubble, label, and on-screen "
    "graphic must contain clear, complete, fully-visible text. Never leave any text element "
    "empty or blank. Every visual element must display readable, meaningful text relevant "
    "to the narration at that moment."
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


def bootstrap_youtube():
    if not YOUTUBE_TOKEN_B64:
        log("YOUTUBE_TOKEN_B64 not set -- YouTube upload will be skipped.")
        return
    YOUTUBE_TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    YOUTUBE_TOKEN_PATH.write_text(base64.b64decode(YOUTUBE_TOKEN_B64).decode("utf-8"))
    log(f"YouTube token written to {YOUTUBE_TOKEN_PATH}")


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
# Gemini: helper that tries multiple models until one works
# ---------------------------------------------------------------------------

def _gemini_generate(prompt: str) -> str:
    """
    Call Gemini using the new google-genai SDK, trying GEMINI_MODEL_FALLBACKS
    in order. Returns generated text, or raises the last exception.
    """
    from google import genai  # type: ignore
    client = genai.Client(api_key=GEMINI_API_KEY)
    last_err: Exception | None = None
    for model in GEMINI_MODEL_FALLBACKS:
        try:
            response = client.models.generate_content(model=model, contents=prompt)
            log(f"    Gemini OK with model: {model}")
            return response.text.strip()
        except Exception as e:
            last_err = e
            log(f"    Gemini model {model} failed: {str(e)[:200]}")
            continue
    raise last_err if last_err else RuntimeError("All Gemini models failed")


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

Write a 150-200 word document (not a list, flowing prose) from a parent's perspective that:
1. Names the specific problem or scenario this post addresses (one sentence)
2. Identifies which 1-2 of the above tools most naturally help with THAT SPECIFIC scenario
3. Describes how a parent would realistically use that tool for this exact situation during an actual week

Rules:
- Ground every tool reference in the specific scenario from this post -- not the general topic.
- Do not pitch or sell. Write as if describing what another parent already does.
- Only reference tools that genuinely connect to the specific scenario. If none connect naturally, write one sentence saying so and stop.
- Do not mention prices, subscriptions, or calls to action.
- Keep it under 200 words.
"""

        text = _gemini_generate(prompt)
        log(f"    Bridge doc generated ({len(text)} chars).")
        return text

    except Exception as e:
        log(f"    Bridge doc generation failed (non-fatal): {e}")
        return ""


# ---------------------------------------------------------------------------
# Post-processing: outro, thumbnail, description, YouTube upload
# ---------------------------------------------------------------------------

OUTRO_PATH = OUTPUT_DIR / "bk_outro.mp4"


class OutroAppendError(Exception):
    """Raised when the BK outro card cannot be appended. The pipeline treats this
    as fatal so the admin UI surfaces the real ffmpeg error instead of silently
    publishing an outro-less video to YouTube."""


def build_outro_card() -> tuple[bool, str]:
    """
    Download the outro asset from BK_OUTRO_URL and build a 5-second 1280x720
    silent MP4. Called at worker startup so any URL or ffmpeg problems surface
    immediately. ALWAYS rebuilds (does not trust the cache) because:
    - Railway's /tmp can persist between container restarts in some cases
    - The user may have replaced the source PNG at the same URL (same path,
      new content), so the cache filename alone can't tell us if it's stale

    Returns (success, message). Message is printable detail for diagnostics.
    """
    import subprocess

    if not BK_OUTRO_URL:
        return False, "BK_OUTRO_URL is empty (env var missing AND fallback constant cleared)"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Always start fresh so the outro reflects the current source image
    OUTRO_PATH.unlink(missing_ok=True)

    try:
        r = httpx.get(BK_OUTRO_URL, timeout=60, follow_redirects=True)
        r.raise_for_status()
    except Exception as e:
        return False, f"Download from {BK_OUTRO_URL[:80]} failed: {e}"

    content_type = r.headers.get("content-type", "")
    is_image = "image" in content_type or BK_OUTRO_URL.lower().endswith(
        (".png", ".jpg", ".jpeg", ".webp")
    )

    # Resolution chosen to match NotebookLM's 1280x720 default video output, so
    # the concat step can copy streams without re-encoding the long source video.
    if is_image:
        png_path = OUTPUT_DIR / "bk_outro_card.png"
        png_path.write_bytes(r.content)
        try:
            conv = subprocess.run(
                [
                    "ffmpeg", "-y",
                    "-loop", "1", "-i", str(png_path),
                    "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo",
                    "-t", "5",
                    "-c:v", "libx264", "-preset", "ultrafast",
                    "-c:a", "aac", "-ar", "44100",
                    "-pix_fmt", "yuv420p",
                    "-r", "30",
                    "-vf", (
                        "scale=1280:720:force_original_aspect_ratio=decrease,"
                        "pad=1280:720:(ow-iw)/2:(oh-ih)/2"
                    ),
                    "-shortest", str(OUTRO_PATH),
                ],
                capture_output=True, text=True, timeout=120,
            )
            png_path.unlink(missing_ok=True)
            if conv.returncode != 0:
                return False, f"PNG→MP4 ffmpeg failed: {conv.stderr[-400:]}"
        except subprocess.TimeoutExpired:
            return False, "PNG→MP4 conversion timed out (120s)"
        except Exception as e:
            return False, f"PNG→MP4 conversion error: {e}"
    else:
        OUTRO_PATH.write_bytes(r.content)

    if not OUTRO_PATH.exists() or OUTRO_PATH.stat().st_size < 1000:
        return False, "Outro file was not created or is too small"

    return True, f"Built {OUTRO_PATH.stat().st_size // 1024} KB at {OUTRO_PATH}"


def append_outro(video_path: str, job_id: str) -> str:
    """
    Concatenate the cached BK outro to the end of the given video and return
    the branded path. Raises OutroAppendError on failure so the pipeline can
    surface the real error in status_detail/error_message instead of silently
    skipping the outro.

    Uses -preset ultrafast and a 15-minute timeout because Railway's CPU is
    too slow to re-encode a 7+ minute 1080p video with the default preset
    inside 5 minutes (which is what was silently failing before).
    """
    import subprocess

    if not OUTRO_PATH.exists():
        # Try to build it now if startup-build didn't run for some reason
        ok, msg = build_outro_card()
        if not ok:
            raise OutroAppendError(f"Outro asset unavailable: {msg}")

    branded_path = str(OUTPUT_DIR / f"{job_id}_branded.mp4")

    # Explicitly normalize BOTH inputs to identical dimensions / pixel format /
    # framerate / audio sample rate before concatenating. The previous concat
    # was failing with libx264 error -22 because the cached outro was built at
    # a different resolution than the NotebookLM video. Forcing both streams
    # through the same scale+format+fps pipeline guarantees they match.
    filter_graph = (
        "[0:v]scale=1280:720:force_original_aspect_ratio=decrease,"
        "pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=30,format=yuv420p[v0];"
        "[0:a]aformat=sample_rates=44100:channel_layouts=stereo[a0];"
        "[1:v]scale=1280:720:force_original_aspect_ratio=decrease,"
        "pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=30,format=yuv420p[v1];"
        "[1:a]aformat=sample_rates=44100:channel_layouts=stereo[a1];"
        "[v0][a0][v1][a1]concat=n=2:v=1:a=1[outv][outa]"
    )

    try:
        result = subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", video_path,
                "-i", str(OUTRO_PATH),
                "-filter_complex", filter_graph,
                "-map", "[outv]", "-map", "[outa]",
                "-c:v", "libx264", "-preset", "ultrafast",
                "-c:a", "aac", "-ar", "44100",
                "-pix_fmt", "yuv420p",
                "-movflags", "+faststart",
                branded_path,
            ],
            capture_output=True, text=True, timeout=900,  # 15 min ceiling
        )
    except subprocess.TimeoutExpired:
        raise OutroAppendError("Concat ffmpeg timed out after 15 minutes")
    except Exception as e:
        raise OutroAppendError(f"Concat ffmpeg crashed: {e}")

    if result.returncode != 0 or not Path(branded_path).exists():
        raise OutroAppendError(f"Concat ffmpeg failed (rc={result.returncode}): {result.stderr[-500:]}")

    log(f"    Outro appended: {branded_path}")
    return branded_path


def create_thumbnail(video_path: str, job_id: str) -> str | None:
    """Extract first frame as 1280x720 JPEG thumbnail using ffmpeg."""
    thumb_path = str(OUTPUT_DIR / f"{job_id}_thumb.jpg")
    try:
        import subprocess
        result = subprocess.run(
            ["ffmpeg", "-y", "-ss", "0.0", "-i", video_path,
             "-vframes", "1", "-s", "1280x720", "-q:v", "2", thumb_path],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0 and Path(thumb_path).exists():
            log(f"    Thumbnail created: {thumb_path}")
            return thumb_path
        else:
            log(f"    Thumbnail failed (non-fatal): {result.stderr[:200]}")
            return None
    except Exception as e:
        log(f"    Thumbnail error (non-fatal): {e}")
        return None


DESCRIPTION_FALLBACK = (
    "Bright Kids AI creates personalized storybooks, songs, and learning tools "
    "for neurodiverse children.\n\n"
    "🔗 Learn more: https://brightkidsai.com\n"
    "📧 Get free resources: https://brightkidsai.com/subscribe\n\n"
    "#Neurodiversity #ADHD #Autism #NeurodiverseParenting #BrightKidsAI"
)


def generate_video_description(title: str, post_text: str) -> str:
    """Use Gemini to create a YouTube description from blog post content."""
    if not GEMINI_API_KEY:
        log("    No GEMINI_API_KEY -- using fallback description.")
        return f"{title}\n\n{DESCRIPTION_FALLBACK}"

    try:
        prompt = f"""\
Write a YouTube video description for a Bright Kids AI video about this blog post.

Title: "{title}"

Blog post excerpt:
---
{post_text[:2000]}
---

Format EXACTLY as follows (no extra commentary):

[2-3 sentence warm summary of the video's main message]

---

In this video, we explore [topic 1], [topic 2], and [topic 3].

Key Takeaways:
• [Actionable insight 1]
• [Actionable insight 2]
• [Actionable insight 3]

---

Bright Kids AI creates personalized storybooks, songs, and learning tools for neurodiverse children.

🔗 Learn more: https://brightkidsai.com
📧 Get free resources: https://brightkidsai.com/subscribe

#Neurodiversity #ADHD #Autism #NeurodiverseParenting #BrightKidsAI

Rules: under 500 words, tone empathetic and empowering for parents of neurodiverse children.
"""

        desc = _gemini_generate(prompt)
        log(f"    Description generated ({len(desc)} chars).")
        return desc

    except Exception as e:
        log(f"    Description generation failed (non-fatal): {e}")
        return f"{title}\n\n{DESCRIPTION_FALLBACK}"


def upload_to_youtube(
    video_path: str,
    thumbnail_path: str | None,
    title: str,
    description: str,
) -> str | None:
    """
    Upload video to YouTube using stored OAuth token.
    Returns YouTube video ID, or None if upload is skipped/failed.
    """
    if not YOUTUBE_TOKEN_B64:
        log("    YOUTUBE_TOKEN_B64 not set -- skipping YouTube upload.")
        return None

    if not YOUTUBE_TOKEN_PATH.exists():
        log("    YouTube token file missing -- skipping YouTube upload.")
        return None

    try:
        import subprocess
        from googleapiclient.discovery import build           # type: ignore
        from googleapiclient.http import MediaFileUpload      # type: ignore
        from google.oauth2.credentials import Credentials     # type: ignore
        from google.auth.transport.requests import Request    # type: ignore

        creds_data = json.loads(YOUTUBE_TOKEN_PATH.read_text())
        creds = Credentials(
            token=creds_data.get("token"),
            refresh_token=creds_data.get("refresh_token"),
            token_uri=creds_data.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=creds_data.get("client_id"),
            client_secret=creds_data.get("client_secret"),
            scopes=creds_data.get("scopes", ["https://www.googleapis.com/auth/youtube.upload"]),
        )

        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            # Persist refreshed token so next run doesn't need re-auth
            updated = {**creds_data, "token": creds.token}
            YOUTUBE_TOKEN_PATH.write_text(json.dumps(updated))
            log("    YouTube token refreshed and saved.")

        youtube = build("youtube", "v3", credentials=creds, cache_discovery=False)

        body = {
            "snippet": {
                "title": title,
                "description": description,
                "tags": ["Neurodiversity", "ADHD", "Autism", "Parenting", "Special Education",
                         "BrightKidsAI", "Neurodiverse Children"],
                "categoryId": "26",  # Howto & Style
                "defaultLanguage": "en",
            },
            "status": {
                "privacyStatus": "unlisted",
                "selfDeclaredMadeForKids": False,
            },
        }

        media = MediaFileUpload(
            video_path, mimetype="video/mp4",
            resumable=True, chunksize=10 * 1024 * 1024,
        )
        insert_request = youtube.videos().insert(
            part="snippet,status", body=body, media_body=media,
        )

        response = None
        while response is None:
            status, response = insert_request.next_chunk()
            if status:
                log(f"    Upload progress: {int(status.progress() * 100)}%")

        video_id = response["id"]
        log(f"    YouTube upload complete: https://youtu.be/{video_id}")

        # Upload custom thumbnail (first frame = title card)
        if thumbnail_path and Path(thumbnail_path).exists():
            try:
                thumb_media = MediaFileUpload(thumbnail_path, mimetype="image/jpeg")
                youtube.thumbnails().set(videoId=video_id, media_body=thumb_media).execute()
                log(f"    Thumbnail uploaded for {video_id}.")
            except Exception as thumb_err:
                log(f"    Thumbnail upload failed (non-fatal): {thumb_err}")

        return video_id

    except Exception as e:
        log(f"    YouTube upload FAILED: {e}")
        return None


def _extract_youtube_id(url: str) -> str | None:
    """Pull the YouTube video ID out of either youtu.be/ID or youtube.com/watch?v=ID."""
    if not url:
        return None
    if "youtu.be/" in url:
        return url.split("youtu.be/")[-1].split("?")[0].split("/")[0]
    if "v=" in url:
        return url.split("v=")[-1].split("&")[0]
    return None


def update_youtube_metadata(youtube_video_id: str, title: str, description: str) -> bool:
    """
    Update title + description on an existing YouTube video. Returns True on success.
    Used by the metadata-only regeneration flow from the admin UI.
    """
    if not YOUTUBE_TOKEN_PATH.exists():
        log("    YouTube token file missing -- cannot update metadata.")
        return False

    try:
        from googleapiclient.discovery import build           # type: ignore
        from google.oauth2.credentials import Credentials     # type: ignore
        from google.auth.transport.requests import Request    # type: ignore

        creds_data = json.loads(YOUTUBE_TOKEN_PATH.read_text())
        creds = Credentials(
            token=creds_data.get("token"),
            refresh_token=creds_data.get("refresh_token"),
            token_uri=creds_data.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=creds_data.get("client_id"),
            client_secret=creds_data.get("client_secret"),
            scopes=creds_data.get("scopes", ["https://www.googleapis.com/auth/youtube.upload"]),
        )
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            YOUTUBE_TOKEN_PATH.write_text(json.dumps({**creds_data, "token": creds.token}))

        youtube = build("youtube", "v3", credentials=creds, cache_discovery=False)

        # Must include categoryId on update -- YouTube rejects snippet without it.
        body = {
            "id": youtube_video_id,
            "snippet": {
                "title": title,
                "description": description,
                "categoryId": "26",
                "tags": ["Neurodiversity", "ADHD", "Autism", "Parenting", "Special Education",
                         "BrightKidsAI", "Neurodiverse Children"],
                "defaultLanguage": "en",
            },
        }
        youtube.videos().update(part="snippet", body=body).execute()
        log(f"    YouTube metadata updated for video {youtube_video_id}.")
        return True

    except Exception as e:
        log(f"    YouTube metadata update FAILED: {e}")
        return False


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


def set_progress(job_id: str, message: str):
    """
    Write a human-readable progress message to video_jobs.status_detail so the
    admin UI can show what stage the worker is in (creating notebook, downloading,
    uploading to YouTube, etc.). Non-fatal -- swallows errors.
    """
    log(f"    [progress] {message}")
    try:
        supa_patch("video_jobs", {"id": job_id}, {
            "status_detail": message,
            "updated_at": now_iso(),
        })
    except Exception as e:
        log(f"    set_progress failed (non-fatal): {e}")


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
                "status":        "processing",
                "status_detail": "Creating NotebookLM notebook",
                "updated_at":    now_iso(),
            })

            try:
                # Create notebook
                timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                nb_title  = f"BK_{post_title[:30]}_{timestamp}"
                nb        = await client.notebooks.create(nb_title)
                nb_id     = nb.id
                log(f"    Notebook created: {nb_id}")
                set_progress(job_id, "Indexing blog content into NotebookLM")

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
                set_progress(job_id, "Firing NotebookLM video generation")
                status  = await client.artifacts.generate_video(
                    nb_id,
                    instructions=VIDEO_INSTRUCTION,
                )
                task_id = status.task_id
                log(f"    Video job started: task_id={task_id}")

                supa_patch("video_jobs", {"id": job_id}, {
                    "notebook_id":   nb_id,
                    "task_id":       task_id,
                    "status":        "processing",
                    "status_detail": "NotebookLM rendering video (5–15 min)",
                    "updated_at":    now_iso(),
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
            log(f"    Still processing -- will check again in 90 sec.")
            # Refresh status_detail with a "last checked" timestamp so the admin
            # UI shows the worker is actively polling, not stuck.
            stamp = datetime.now().strftime("%H:%M:%S")
            set_progress(job_id, f"NotebookLM still rendering (last checked {stamp})")
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
        set_progress(job_id, "Downloading video from NotebookLM CDN")

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

            # ── Step 1: Append BK outro card (FATAL on failure now) ─────
            set_progress(job_id, "Appending Bright Kids AI outro card")
            try:
                branded_path = append_outro(out_path, job_id)
            except OutroAppendError as outro_err:
                log(f"    OUTRO APPEND FAILED: {outro_err}")
                supa_patch("video_jobs", {"id": job_id}, {
                    "status":        "failed",
                    "status_detail": "Outro append failed -- see error_message",
                    "error_message": str(outro_err)[:1000],
                    "updated_at":    now_iso(),
                })
                notify(f"BK video FAILED at outro step: '{title}'\n{str(outro_err)[:200]}")
                continue

            # ── Step 2: Extract first-frame thumbnail ────────────────────
            set_progress(job_id, "Extracting thumbnail from first frame")
            thumb_path = create_thumbnail(branded_path, job_id)

            # ── Step 3: Upload branded video to Supabase Storage ────────
            set_progress(job_id, "Uploading branded video to Supabase Storage")
            storage_url = supabase_upload_video(branded_path, job_id)
            log(f"    Uploaded to Supabase Storage: {storage_url}")

            # ── Step 4: Generate Gemini description ─────────────────────
            set_progress(job_id, "Generating YouTube description with Gemini")
            post_text_for_desc = job.get("blog_post_content", "")
            description = generate_video_description(title, post_text_for_desc)

            # ── Step 5: Upload to YouTube ────────────────────────────────
            supa_patch("video_jobs", {"id": job_id}, {
                "status":        "uploading",
                "status_detail": "Uploading to YouTube",
                "updated_at":    now_iso(),
            })
            yt_video_id = upload_to_youtube(branded_path, thumb_path, title, description)

            # ── Step 6: Update database ──────────────────────────────────
            final_url = f"https://youtu.be/{yt_video_id}" if yt_video_id else storage_url
            supa_patch("video_jobs", {"id": job_id}, {
                "status":        "done",
                "status_detail": "Video live on YouTube" if yt_video_id else "Stored (YouTube upload skipped)",
                "video_url":     final_url,
                "video_path":    branded_path,
                "updated_at":    now_iso(),
            })

            # ── Step 7: Cleanup local files ──────────────────────────────
            Path(out_path).unlink(missing_ok=True)
            if branded_path != out_path:
                Path(branded_path).unlink(missing_ok=True)
            if thumb_path:
                Path(thumb_path).unlink(missing_ok=True)

            if yt_video_id:
                notify(
                    f"BK video live on YouTube! 🎬\n"
                    f"'{title}'\n"
                    f"https://youtu.be/{yt_video_id}"
                )
            else:
                notify(
                    f"BK video ready (Supabase Storage) 🎬\n"
                    f"'{title}'\n"
                    f"{storage_url}\n"
                    f"(Set YOUTUBE_TOKEN_B64 in Railway to enable auto-upload)"
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
# Phase 3: regenerate YouTube metadata only (no new video build)
# ---------------------------------------------------------------------------

def process_metadata_only_jobs():
    """
    Pick up jobs with status='metadata_queued' and refresh ONLY the YouTube
    title + description for the existing video. The video itself is not
    re-rendered. Sets status='done' on success or 'failed' on error.
    """
    jobs = supa_get("video_jobs", {
        "status": "eq.metadata_queued",
        "limit":  "5",
        "order":  "created_at.asc",
    })

    if not jobs:
        log("No metadata-only jobs.")
        return

    log(f"Refreshing YouTube metadata for {len(jobs)} job(s)...")

    for job in jobs:
        job_id    = job["id"]
        title     = job.get("blog_post_title", "Untitled")
        post_text = job.get("blog_post_content", "")
        video_url = job.get("video_url", "") or ""

        yt_id = _extract_youtube_id(video_url)
        if not yt_id:
            err = f"Cannot extract YouTube video ID from video_url: {video_url}"
            log(f"  {err}")
            supa_patch("video_jobs", {"id": job_id}, {
                "status": "failed", "error_message": err[:500], "updated_at": now_iso(),
            })
            continue

        set_progress(job_id, f"Generating new description for '{title}'...")
        description = generate_video_description(title, post_text)

        set_progress(job_id, f"Updating YouTube metadata for {yt_id}...")
        ok = update_youtube_metadata(yt_id, title, description)

        if ok:
            supa_patch("video_jobs", {"id": job_id}, {
                "status":        "done",
                "status_detail": "Metadata refreshed on YouTube",
                "updated_at":    now_iso(),
            })
            notify(f"BK video metadata refreshed 📝\n'{title}'\nhttps://youtu.be/{yt_id}")
        else:
            supa_patch("video_jobs", {"id": job_id}, {
                "status":        "failed",
                "error_message": "YouTube metadata update failed -- see worker logs",
                "updated_at":    now_iso(),
            })


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    log("=" * 55)
    log("BK Video Worker (Railway) -- start")

    # Diagnostic: confirm which env vars actually reached the container.
    # BK_OUTRO_URL falls back to a hardcoded constant if the env var is missing.
    using_outro_fallback = (BK_OUTRO_URL == _BK_OUTRO_FALLBACK and not _envstr("BK_OUTRO_URL"))
    log(f"  ENV: SUPABASE_URL        = {'set (' + str(len(SUPABASE_URL)) + ' chars)' if SUPABASE_URL else 'MISSING'}")
    log(f"  ENV: SUPABASE_KEY        = {'set' if SUPABASE_KEY else 'MISSING'}")
    log(f"  ENV: GEMINI_API_KEY      = {'set' if GEMINI_API_KEY else 'MISSING'}")
    log(f"  ENV: YOUTUBE_TOKEN_B64   = {'set' if YOUTUBE_TOKEN_B64 else 'MISSING'}")
    log(f"  ENV: BK_OUTRO_URL        = {BK_OUTRO_URL[:80]}{' (FALLBACK -- env var empty)' if using_outro_fallback else ''}")
    log(f"  ENV: SESSION_B64         = {'set' if SESSION_B64 else 'MISSING'}")

    bootstrap_session()
    bootstrap_youtube()

    # Build the BK outro card NOW so failures surface in startup logs, not
    # buried inside an individual video job 10 minutes later.
    log("  Building BK outro card...")
    ok, msg = build_outro_card()
    if ok:
        log(f"  BK outro READY: {msg}")
    else:
        log(f"  BK outro FAILED at startup: {msg}")
        log("  Videos will still be uploaded, but jobs will be marked FAILED")
        log("  with the real ffmpeg error so you can see what went wrong.")

    await process_queued_jobs()
    await check_processing_jobs()
    process_metadata_only_jobs()

    log("BK Video Worker -- done")
    log("=" * 55)


if __name__ == "__main__":
    asyncio.run(main())
