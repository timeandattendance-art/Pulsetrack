# PulseTrack Enrichment Pipeline — Setup Guide

Tier 0 (free local signals) + Tier 1 (website scraping) + Tier 3 (search + Claude
Haiku classification). Tier 2 (LinkedIn confirmation) is intentionally excluded —
handle that tier manually for the small residual that survives Tiers 0/1/3.

## What you need (15-20 minutes to set up)

1. **Supabase account** (you have this) — for the Postgres database
2. **Railway account** (you have this) — to run the pipeline
3. **GitHub account** (you have this) — to deploy from
4. **Anthropic API key** — console.anthropic.com → API Keys → Create Key
5. **Brave Search API key** — brave.com/search/api → free tier available, sign up

## Step 1 — Set up the database

1. Open your Supabase project → SQL Editor → New Query
2. Paste the entire contents of `db/schema.sql` and run it
3. Confirm tables exist: Table Editor → you should see `companies`, `people`,
   `snapshots`, `signals`, `offers`, `match_alerts`, `pipeline_runs`
4. Get your connection string: Project Settings → Database → Connection string (URI)
   — use the **pooler** connection (port 6543) if available, not the direct one,
   since Railway will be making connections from outside Supabase's network

## Step 2 — Push this code to GitHub

```bash
cd pulsetrack
git init
git add .
git commit -m "Initial PulseTrack enrichment pipeline"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/pulsetrack.git
git push -u origin main
```

## Step 3 — Connect Railway to the repo

1. Railway dashboard → New Project → Deploy from GitHub repo → select `pulsetrack`
2. Railway will detect `requirements.txt` and `railway.json` automatically
3. Go to the new service → Variables tab → add all variables from `.env.example`
   with your real values (SUPABASE_DB_URL, ANTHROPIC_API_KEY, BRAVE_API_KEY)
4. Upload your lead CSV to the repo under `data/leads.csv` (or change the path
   in `railway.json`'s startCommand)

## Step 4 — Run it

The default `railway.json` runs the full pipeline once on deploy. For repeated
runs (e.g. processing a new CSV each week), trigger a redeploy from the Railway
dashboard, or set up a Railway Cron Job pointed at the same start command.

**What happens when it runs:**
1. Tier 0 runs instantly (pandas logic, no network calls) — writes auto-resolved
   companies straight to Supabase
2. Tier 1 runs next — async website checks against the Tier-0 leftovers, writes
   resolved ones to Supabase
3. Tier 3 submits a Claude Batch API job for whatever's still unresolved, then
   **exits** — batch jobs process asynchronously, can take a few minutes to ~24h

## Step 5 — Collect Tier 3 results

Once the batch finishes (check status in Anthropic Console → Batches, or poll
programmatically), run:

```bash
python pipeline_tier3_collect.py <batch_id>
```

This pulls the classifications and writes them to Supabase. Anything Claude
returned with confidence below 0.6 gets flagged `needs_review` rather than
auto-resolved — that's your manual-review queue, viewable directly in Supabase:

```sql
select name, classification_evidence from companies where resolution_status = 'needs_review';
```

## Tier 2 (LinkedIn) — manual process

For companies still ambiguous after all three tiers, query:

```sql
select c.name, p.full_name, p.linkedin_url
from companies c join people p on p.company_id = c.id
where c.resolution_status = 'needs_review';
```

Open each `linkedin_url` yourself and confirm current title, then update the
record manually (or mark `resolution_status = 'manually_reviewed'`).

## Checking results anytime

```sql
-- overview of where everything landed
select company_type, resolution_status, count(*) 
from companies 
group by company_type, resolution_status 
order by count(*) desc;

-- the manual review queue, sorted by how close it got
select name, classification_confidence, usable_lead_reason 
from companies 
where resolution_status = 'needs_review' 
order by classification_confidence desc;
```

## Known limitations / what to expect

- Tier 0 is genuinely free but resolves a small fraction (~1% on the dataset
  tested) — the source data simply doesn't carry much internal evidence
- Tier 1 success rate depends heavily on company website quality/structure —
  small business sites vary wildly, expect a meaningful "unreachable or no
  match" rate, not 100% resolution
- Tier 3 costs a small amount per company (one Brave search + one Claude Haiku
  batch call) — at a few thousand residual companies this should be single-
  digit-to-low-double-digit dollars total, not hundreds
- Nothing in this pipeline touches LinkedIn directly — that's intentional,
  per the Tier 2 discussion
