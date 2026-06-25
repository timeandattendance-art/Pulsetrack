"""
config.py — shared configuration for the PulseTrack enrichment pipeline.
All secrets are read from environment variables. On Railway, set these
in the project's Variables tab. Locally, copy .env.example to .env and
fill it in (the pipeline loads it automatically via python-dotenv).
"""
import os
from dotenv import load_dotenv
load_dotenv()
# --- Supabase / Postgres ---
SUPABASE_DB_URL = os.environ.get("SUPABASE_DB_URL", "")
# Format: postgresql://postgres:[YOUR-PASSWORD]@db.[PROJECT-REF].supabase.co:5432/postgres
# Find this in Supabase: Project Settings > Database > Connection string (URI)
# --- Anthropic (Claude) ---
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
# --- Search API (Serper.dev) ---
SERPER_API_KEY = os.environ.get("SERPER_API_KEY", "")
SEARCH_PROVIDER = os.environ.get("SEARCH_PROVIDER", "serper")
# --- Pipeline tuning ---
TIER1_CONCURRENCY = int(os.environ.get("TIER1_CONCURRENCY", "10"))
TIER1_TIMEOUT_SECONDS = int(os.environ.get("TIER1_TIMEOUT_SECONDS", "10"))
TIER3_BATCH_SIZE = int(os.environ.get("TIER3_BATCH_SIZE", "20"))
# Staff bucket values observed in source data (ceiling, not exact count)
STAFF_BUCKETS = [30, 125, 350, 10001]