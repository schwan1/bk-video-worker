-- Run this in Supabase SQL Editor

-- 1. video_jobs table (already created, here for reference)
create table if not exists video_jobs (
  id uuid primary key default gen_random_uuid(),
  blog_post_id text not null,
  blog_post_title text,
  blog_post_url text,
  blog_post_content text,
  status text not null default 'queued',
  notebook_id text,
  task_id text,
  video_path text,
  video_url text,
  error_message text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create index if not exists idx_video_jobs_status on video_jobs(status);
create index if not exists idx_video_jobs_blog_post_id on video_jobs(blog_post_id);

-- 2. Storage bucket for completed videos
insert into storage.buckets (id, name, public)
values ('video-jobs', 'video-jobs', true)
on conflict (id) do nothing;

-- 3. Allow service role full access to the bucket
create policy "Service role full access to video-jobs"
on storage.objects
for all
using (bucket_id = 'video-jobs')
with check (bucket_id = 'video-jobs');
