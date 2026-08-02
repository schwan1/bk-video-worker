"""Free FFmpeg/Pillow renderer for Bright Kids 9:16 social Reel jobs."""
import json, subprocess, textwrap
from pathlib import Path
import httpx
from PIL import Image, ImageDraw, ImageFont

W, H = 1080, 1920

def _font(size, bold=False):
    candidates = [
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    for candidate in candidates:
        if Path(candidate).exists(): return ImageFont.truetype(candidate, size)
    return ImageFont.load_default()

def _write_centered(draw, text, y, font, color, width=880):
    lines = textwrap.wrap(text, width=max(12, int(34 * 42 / max(16, getattr(font, "size", 42)))))
    for line in lines:
        box = draw.textbbox((0, 0), line, font=font)
        draw.text(((W - (box[2] - box[0])) / 2, y), line, font=font, fill=color)
        y += int((box[3] - box[1]) * 1.35)

def _scene(path, title, caption, image_path=None, cta=False):
    image = Image.new("RGB", (W, H), "#10213d" if not cta else "#19bfd2")
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((54, 54, W-54, H-54), 42, outline="#ffffff", width=5)
    if image_path and Path(image_path).exists():
        art = Image.open(image_path).convert("RGB")
        art.thumbnail((W-120, 980))
        x, y = (W-art.width)//2, 180
        image.paste(art, (x, y)); caption_y = min(H-450, y + art.height + 70)
    else: caption_y = 560
    _write_centered(draw, title, 105 if not image_path else 80, _font(62, True), "#ffffff")
    _write_centered(draw, caption, caption_y, _font(52, True), "#ffffff")
    if cta: _write_centered(draw, "Bright Kids AI", H-180, _font(36, True), "#06152d")
    image.save(path, "PNG")

def process_social_reel_jobs(supa_get, supa_patch, supabase_url, supabase_key, output_dir, log, now_iso):
    jobs = supa_get("SocialReelJob", {"status": "eq.queued", "limit": "2", "order": "createdAt.asc"})
    for job in jobs:
        job_id, post_id = job["id"], job["campaignPostId"]
        try:
            supa_patch("SocialReelJob", {"id": job_id}, {"status": "processing", "statusDetail": "Rendering five social Reel scenes", "updatedAt": now_iso()})
            plan = json.loads(job["planJson"]); work = Path(output_dir) / f"social_{job_id}"; work.mkdir(parents=True, exist_ok=True)
            image_path = None
            if job.get("imageUrl"):
                image_path = str(work / "source.png")
                with httpx.stream("GET", job["imageUrl"], timeout=60, follow_redirects=True) as r:
                    r.raise_for_status(); Path(image_path).write_bytes(r.read())
            scenes = []
            for index, beat in enumerate(plan["captionBeats"]):
                scene = work / f"scene-{index}.png"; scenes.append(scene)
                _scene(scene, plan.get("title") or "Bright Kids AI", beat["text"], image_path if index == 1 else None, index == 4)
            concat = work / "scenes.txt"; concat.write_text("".join(f"file '{scene}'\nduration 4\n" for scene in scenes) + f"file '{scenes[-1]}'\n")
            out = work / "reel.mp4"; command = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(concat)]
            if job.get("narrationUrl"): command += ["-i", job["narrationUrl"], "-map", "0:v:0", "-map", "1:a:0", "-shortest"]
            command += ["-r", "30", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", str(out)]
            subprocess.run(command, check=True, capture_output=True, timeout=600)
            storage_path = f"{post_id}/reel-{job_id}.mp4"; upload_url = f"{supabase_url}/storage/v1/object/social-reels/{storage_path}"
            with open(out, "rb") as stream:
                response = httpx.put(upload_url, headers={"apikey": supabase_key, "Authorization": f"Bearer {supabase_key}", "Content-Type": "video/mp4", "x-upsert": "true"}, content=stream.read(), timeout=300)
                response.raise_for_status()
            public_url = f"{supabase_url}/storage/v1/object/public/social-reels/{storage_path}"
            supa_patch("SocialReelJob", {"id": job_id}, {"status": "done", "statusDetail": "MP4 ready for review", "videoUrl": public_url, "errorMessage": None, "updatedAt": now_iso()})
        except Exception as error:
            log(f"Social reel {job_id} failed: {error}")
            supa_patch("SocialReelJob", {"id": job_id}, {"status": "failed", "statusDetail": "Render failed", "errorMessage": str(error)[:500], "updatedAt": now_iso()})
