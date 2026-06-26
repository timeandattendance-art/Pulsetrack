"""
pipeline.py - orchestrates the full PulseTrack run:

    1. Pull all source CSVs from the shared Google Drive folder
    2. Merge them into one dataframe, tagging conflicts (nothing deleted)
    3. Run Tier 0 -> Tier 1 -> Tier 3 exactly as before
    4. Write resolved companies/people into Supabase
    5. Write three local CSVs: leads / manual_review / run_summary
    6. Send an email alert on completion, or immediately on hard failure

Usage:
    python -m pipeline.pipeline
"""

import os
import sys
import asyncio
import traceback
import pandas as pd

from pipeline.drive_fetch import fetch_all_csvs
from pipeline.merge_leads import merge_and_tag
from pipeline.tier0_local_signals import run_tier0
from pipeline.tier1_website_scrape import run_tier1_batch
from pipeline.tier3_search_classify import submit_batch, SearchQuotaExceeded
from pipeline.config import TIER3_TEST_LIMIT
from pipeline.db import get_conn, upsert_company, insert_person, log_pipeline_run
from pipeline.alerts import send_run_completed, send_run_failed, send_budget_exceeded

DRIVE_FOL