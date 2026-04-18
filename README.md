# bk-video-worker

Railway-hosted Docker worker that turns Bright Kids AI blog posts into NotebookLM cinematic videos and stores them in Supabase Storage.

---

## How it works

The worker runs on a 5-minute loop (`entrypoint.sh`). Each pass does two things:

**Phase 1 — Start new jobs (`status = queued`)**
1. Fetches up to 3 queued rows from `video_jobs`
2. Creates a NotebookLM notebook with three sources:
   - **Source 1:** The blog post text (primary content)
   - **Source 2:** Static Bright Kids AI ecosystem overview (`BK_ECOSYSTEM_CONTEXT`) — describes every tool families use (My Stars, Weekly Brief, Stories, Songs, Jenny, Child Profile)
   - **Source 3:** Gemini-generated bridge doc — a 150–200 word prose piece connecting this specific post's topic to 1–2 relevant BK tools naturally (skipped if `GEMINI_API_KEY` is absent)
3. Fires NotebookLM video generation with a branded instruction prompt
4. Saves `notebook_id` to the row and sets `status = processing`
5. Sends a Telegram notification with a link to the notebook Studio view

**Phase 2 — Check in-flight jobs (`status = processing`)**
1. Fetches all processing rows that have a `notebook_id`
2. Polls each notebook's studio status using `notebooklm_tools.poll_studio_status()`
3. When a video is `completed`, downloads it directly from the signed CDN URL (no auth headers needed — auth is embedded in the URL)
4. Uploads the MP4 to Supabase Storage bucket `video-jobs` as `{job_id}.mp4`
5. Sets `status = done`, stores the public `video_url`, sends a Telegram notification
6. On any error: sets `status = failed` with `error_message`, sends a Telegram alert

---

## Architecture

```
Supabase (video_jobs table)
       │
       ▼ poll every 5 min
bk-video-worker (Railway Docker)
       │
       ├── Phase 1: notebooklm-py (async)
       │     notebooklm.NotebookLMClient.from_storage()
       │     → notebooks.create()
       │     → sources.add_text() × 3
       │     → artifacts.generate_video()
       │
       ├── Phase 2: notebooklm-mcp-cli (sync, run in executor)
       │     notebooklm_tools.NotebookLMClient(cookies=...)
       │     → poll_studio_status(nb_id) → video_url
       │     → httpx stream download from lh3.googleusercontent.com
       │     → Supabase Storage PUT
       │
       └── Telegram notifications (start / done / failed)
```

---

## Database schema

```sql
create table video_jobs (
  id               uuid primary key default gen_random_uuid(),
  blog_post_id     text not null,
  blog_post_title  text,
  blog_post_url    text,
  blog_post_content text,         -- full post text, used as Source 1
  status           text not null default 'queued',
  notebook_id      text,          -- set after Phase 1 fires
  task_id          text,          -- NotebookLM task id (informational)
  video_path       text,          -- tmp path, cleared after upload
  video_url        text,          -- public Supabase Storage URL
  error_message    text,
  created_at       timestamptz default now(),
  updated_at       timestamptz default now()
);
```

See `supabase_setup.sql` for full DDL including the `video-jobs` storage bucket.

---

## Video instruction

The worker instructs NotebookLM to create a warm, empathetic cinematic overview for parents of neurodiverse children. The instruction asks NotebookLM to reference BK tools by name only when a connection feels genuine — not as a pitch. If a connection doesn't fit naturally, the video omits it entirely.

---

## Environment variables

Set these in the Railway dashboard under **Variables**:

| Variable | Required | Description |
|----------|----------|-------------|
| `NEXT_PUBLIC_SUPABASE_URL` | Yes | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Yes | Service role key (full DB + Storage access) |
| `NOTEBOOKLM_STORAGE_STATE_B64` | Yes | Base64-encoded Playwright `storage_state.json` (Google auth cookies) |
| `TELEGRAM_BOT_TOKEN` | Yes | Telegram bot token for job notifications |
| `TELEGRAM_CHAT_ID` | Yes | Telegram chat ID to receive notifications |
| `GEMINI_API_KEY` | Optional | Gemini API key for bridge doc generation (skipped gracefully if absent) |

### Refreshing Google auth cookies

NotebookLM requires valid Google session cookies. Export a fresh `storage_state.json` from Playwright after signing in, then re-encode it:

```bash
base64 -w 0 storage_state.json
```

Paste the output into the `NOTEBOOKLM_STORAGE_STATE_B64` Railway variable and redeploy.

---

## Queueing a job

Insert a row directly into `video_jobs` from the Bright Kids AI admin dashboard (Blog → generate video button) or via SQL:

```sql
INSERT INTO video_jobs (blog_post_id, blog_post_title, blog_post_url, blog_post_content)
VALUES ('post-uuid', 'My Blog Post Title', 'https://...', 'Full post text here...');
```

The worker picks it up on the next 5-minute poll.

---

## Re-queueing failed jobs

If a job failed but the video was already generated in NotebookLM, reset it to `processing` (not `queued`) so the worker skips Phase 1 and goes straight to download:

```sql
UPDATE video_jobs
SET status = 'processing', error_message = NULL, updated_at = NOW()
WHERE status = 'failed'
  AND notebook_id IS NOT NULL;
```

To retry from scratch (re-create the notebook):

```sql
UPDATE video_jobs
SET status = 'queued', notebook_id = NULL, task_id = NULL,
    error_message = NULL, updated_at = NOW()
WHERE status = 'failed';
```

---

## Local development

The worker is designed to run in Docker but can be tested locally:

```bash
# Install dependencies
pip install -r requirements.txt
python -m playwright install chromium

# Set env vars (or use a .env file with python-dotenv)
export NEXT_PUBLIC_SUPABASE_URL="https://..."
export SUPABASE_SERVICE_ROLE_KEY="..."
export NOTEBOOKLM_STORAGE_STATE_B64="$(base64 -w 0 storage_state.json)"
export TELEGRAM_BOT_TOKEN="..."
export TELEGRAM_CHAT_ID="..."
export GEMINI_API_KEY="..."

python worker.py
```

---

## Deployment

Railway auto-deploys on push to `main`. The Dockerfile installs Python 3.11, all system deps for Playwright/Chromium, pip packages, and the Chromium browser. Build time is ~3–4 minutes on first install of `notebooklm-mcp-cli` and `google-generativeai`.

---

## Files

| File | Purpose |
|------|---------|
| `worker.py` | Main worker — all logic |
| `requirements.txt` | Python dependencies |
| `Dockerfile` | Container definition |
| `entrypoint.sh` | 5-minute polling loop |
| `supabase_setup.sql` | DDL for `video_jobs` table and `video-jobs` storage bucket |
