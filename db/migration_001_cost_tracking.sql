-- Migration: add cost & transparency tracking to pipeline_runs
-- Run this in the Supabase SQL editor before the next deploy.

alter table pipeline_runs
    add column if not exists cost_usd numeric(10,4) default 0,
    add column if not exists api_calls_made integer default 0,
    add column if not exists rate_limited_count integer default 0,
    add column if not exists failed_count integer default 0,
    add column if not exists needs_review_count integer default 0,
    add column if not exists status text default 'running';  -- running | completed | failed

-- Per-call cost log, so every single Brave/Claude call is individually traceable,
-- not just a rolled-up total per run.
create table if not exists api_call_log (
    id uuid primary key default gen_random_uuid(),
    pipeline_run_id uuid references pipeline_runs(id),
    company_key text,
    provider text,              -- brave_search | claude_haiku
    cost_usd numeric(10,6) default 0,
    outcome text,                -- success | rate_limited_retried | failed
    created_at timestamptz default now()
);
