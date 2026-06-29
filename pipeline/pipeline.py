"""
pipeline.py - orchestrates the full PulseTrack run:

    1. Pull all source CSVs from the shared Google Drive folder
    2. Merge them into one dataframe, tagging conflicts (nothing deleted)
    3. Run Tier 0 -> Tier 1 -> Tier 3 exactly as before
    4. Write EVERY company into Supabase (not just auto-resolved ones),
       tagged with its current resolution_status, so Supabase becomes
       the single durable source of truth for the entire dataset
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

DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID")
DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw_from_drive")
MERGED_PATH = os.path.join(DATA_DIR, "leads.csv")
OUTPUT_DIR = "outputs"

COL_FIRST_NAME = "First Name.1"
COL_LAST_NAME = "Last Name.1"
COL_FULL_NAME = "Contact Full Name"
COL_TITLE = "Title"
COL_EMAIL = "Contact Email"
COL_PHONE = "Contact Phone"
COL_LINKEDIN = "Contact LI Profile URL"
COL_NAME_CLEANED = "Company Name - Cleaned"


def safe_get(row, col, default=None):
    if col in row.index:
        val = row[col]
        if pd.isna(val):
            return default
        return val
    return default


def build_person_record(row, company_id: str, raw_source_file: str) -> dict:
    full_name = safe_get(row, COL_FULL_NAME)
    first_name = safe_get(row, COL_FIRST_NAME)
    last_name = safe_get(row, COL_LAST_NAME)
    if not full_name and (first_name or last_name):
        full_name = f"{first_name or ''} {last_name or ''}".strip()

    return {
        "company_id": company_id,
        "full_name": full_name,
        "first_name": first_name,
        "last_name": last_name,
        "title": safe_get(row, COL_TITLE),
        "title_normalized": None,
        "seniority": None,
        "email": safe_get(row, COL_EMAIL),
        "email_validation": None,
        "phone": safe_get(row, COL_PHONE),
        "linkedin_url": safe_get(row, COL_LINKEDIN),
        "is_current": True,
        "resolution_confidence": None,
        "resolution_source": raw_source_file,
        "time_in_role_months": None,
        "time_at_company_months": None,
        "job_change_type": None,
        "raw_source_file": raw_source_file,
    }


def insert_people_for_company(conn, raw_df: pd.DataFrame, name_cleaned: str,
                               company_id: str, source_tag: str) -> int:
    if COL_NAME_CLEANED not in raw_df.columns:
        return 0
    group = raw_df[raw_df[COL_NAME_CLEANED] == name_cleaned]
    inserted = 0
    for _, row in group.iterrows():
        person_record = build_person_record(row, company_id, source_tag)
        if not person_record["full_name"] and not person_record["email"]:
            continue
        insert_person(conn, person_record)
        inserted += 1
    return inserted


def is_junk_company_name(name) -> bool:
    if not name or not isinstance(name, str):
        return True
    return len(name.strip()) < 2


def fetch_and_merge_source_data() -> pd.DataFrame:
    print(f"=== Fetching source CSVs from Drive folder {DRIVE_FOLDER_ID} ===")
    csv_paths = fetch_all_csvs(DRIVE_FOLDER_ID, RAW_DIR)
    print(f"Downloaded {len(csv_paths)} files: {[p.split('/')[-1] for p in csv_paths]}")

    print("=== Merging and tagging conflicts (no rows removed) ===")
    tagged = merge_and_tag(csv_paths)
    print(tagged["merge_status"].value_counts())
    print(f"Genuine top-title conflicts: {tagged['true_conflict'].sum()} rows "
          f"across {tagged[tagged['true_conflict']]['company_key'].nunique()} companies")

    os.makedirs(DATA_DIR, exist_ok=True)
    tagged.to_csv(MERGED_PATH, index=False)
    return tagged


def write_split_outputs(tagged: pd.DataFrame, run_summary: dict):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    needs_review_mask = tagged.get("true_conflict", False) == True
    if "resolution_status" in tagged.columns:
        needs_review_mask = needs_review_mask | (tagged["resolution_status"] == "needs_review")

    manual_review = tagged[needs_review_mask]
    leads = tagged[~needs_review_mask]

    leads_path = os.path.join(OUTPUT_DIR, "leads.csv")
    review_path = os.path.join(OUTPUT_DIR, "manual_review.csv")
    summary_path = os.path.join(OUTPUT_DIR, "run_summary.csv")

    leads.to_csv(leads_path, index=False)
    manual_review.to_csv(review_path, index=False)
    pd.DataFrame([run_summary]).to_csv(summary_path, index=False)

    print(f"Wrote {len(leads)} rows -> {leads_path}")
    print(f"Wrote {len(manual_review)} rows -> {review_path}")
    print(f"Wrote run summary -> {summary_path}")

    run_summary["needs_review_count"] = len(manual_review)


def main():
    run_summary = {
        "tier": "full_run",
        "companies_processed": 0,
        "companies_resolved": 0,
        "needs_review_count": 0,
        "failed_count": 0,
        "rate_limited_count": 0,
        "api_calls_made": 0,
        "cost_usd": 0.0,
        "people_inserted": 0,
        "companies_written_to_supabase": 0,
    }

    try:
        tagged = fetch_and_merge_source_data()
        raw_df = pd.read_csv(MERGED_PATH, dtype=str, low_memory=False)

        print(f"=== TIER 0: local signal resolution on {MERGED_PATH} ===")
        tier0_results = run_tier0(MERGED_PATH)
        print(tier0_results["resolution_status"].value_counts())

        auto_resolved = tier0_results[tier0_results["resolution_status"] == "auto_resolved"]
        needs_review = tier0_results[tier0_results["resolution_status"] == "needs_review"]

        run_summary["companies_processed"] += len(tier0_results)
        run_summary["companies_resolved"] += len(auto_resolved)

        with get_conn() as conn:
            for _, row in auto_resolved.iterrows():
                company_record = {
                    "name": row["name"], "name_cleaned": row["name_cleaned"],
                    "domain": None, "website": None, "industry": None,
                    "revenue_range": None, "staff_bucket": None,
                    "city": None, "state": None, "country": None, "description": None,
                    "company_type": row["company_type"],
                    "usable_lead": row["usable_lead"],
                    "usable_lead_reason": row["usable_lead_reason"],
                    "classification_confidence": row["classification_confidence"],
                    "classification_source": row["classification_source"],
                    "classification_evidence": row["classification_evidence"],
                    "resolution_status": row["resolution_status"],
                }
                company_id = upsert_company(conn, company_record)
                run_summary["people_inserted"] += insert_people_for_company(
                    conn, raw_df, row["name_cleaned"], company_id, "tier0_auto_resolved"
                )
                run_summary["companies_written_to_supabase"] += 1

            for _, row in needs_review.iterrows():
                company_record = {
                    "name": row["name"], "name_cleaned": row["name_cleaned"],
                    "domain": None, "website": None, "industry": None,
                    "revenue_range": None, "staff_bucket": None,
                    "city": None, "state": None, "country": None, "description": None,
                    "company_type": row.get("company_type"),
                    "usable_lead": row.get("usable_lead"),
                    "usable_lead_reason": row.get("usable_lead_reason"),
                    "classification_confidence": row.get("classification_confidence"),
                    "classification_source": "tier0_signal",
                    "classification_evidence": row.get("classification_evidence"),
                    "resolution_status": "needs_review",
                }
                company_id = upsert_company(conn, company_record)
                run_summary["people_inserted"] += insert_people_for_company(
                    conn, raw_df, row["name_cleaned"], company_id, "tier0_needs_review"
                )
                run_summary["companies_written_to_supabase"] += 1

            log_pipeline_run(conn, "tier0", len(tier0_results), len(auto_resolved),
                              cost_usd=0.0, api_calls_made=0)

        print(f"Tier 0 resolved {len(auto_resolved)} / {len(tier0_results)} companies. "
              f"{len(needs_review)} pass to Tier 1. All written to Supabase.")

        resolved_tier1 = pd.DataFrame()
        residual = pd.DataFrame()

        if len(needs_review) > 0:
            print(f"=== TIER 1: website scraping for {len(needs_review)} companies ===")
            companies_to_check = []
            for _, row in needs_review.iterrows():
                group = raw_df[raw_df[COL_NAME_CLEANED] == row["name_cleaned"]]
                domain = (group["Website"].dropna().iloc[0]
                          if "Website" in group.columns and not group["Website"].dropna().empty
                          else None)
                names = (group[COL_FULL_NAME].dropna().unique().tolist()
                         if COL_FULL_NAME in group.columns else [])
                companies_to_check.append({
                    "name_cleaned": row["name_cleaned"], "domain": domain,
                    "candidate_names": names, "n_contacts": row["n_contacts"],
                })

            tier1_results = asyncio.run(run_tier1_batch(companies_to_check))
            tier1_df = pd.DataFrame(tier1_results)
            print(tier1_df["resolution"].value_counts())

            resolved_tier1 = tier1_df[tier1_df["resolution"] == "resolved_tier1"]
            residual = tier1_df[tier1_df["resolution"].isin(["unresolved_tier1", "ambiguous_tier1"])]

            run_summary["companies_resolved"] += len(resolved_tier1)

            with get_conn() as conn:
                for _, row in resolved_tier1.iterrows():
                    company_record = {
                        "name": row["name_cleaned"], "name_cleaned": row["name_cleaned"],
                        "domain": row["domain"], "website": row["domain"],
                        "industry": None, "revenue_range": None, "staff_bucket": None,
                        "city": None, "state": None, "country": None, "description": None,
                        "company_type": "standalone_business", "usable_lead": True,
                        "usable_lead_reason": f"confirmed via company website: {row['evidence']}",
                        "classification_confidence": row["confidence"],
                        "classification_source": "tier1_website",
                        "classification_evidence": row["evidence"],
                        "resolution_status": "auto_resolved",
                    }
                    company_id = upsert_company(conn, company_record)
                    run_summary["people_inserted"] += insert_people_for_company(
                        conn, raw_df, row["name_cleaned"], company_id, "tier1_website"
                    )

                for _, row in residual.iterrows():
                    company_record = {
                        "name": row["name_cleaned"], "name_cleaned": row["name_cleaned"],
                        "domain": row.get("domain"), "website": row.get("domain"),
                        "industry": None, "revenue_range": None, "staff_bucket": None,
                        "city": None, "state": None, "country": None, "description": None,
                        "company_type": None, "usable_lead": None,
                        "usable_lead_reason": None,
                        "classification_confidence": row.get("confidence"),
                        "classification_source": "tier1_website",
                        "classification_evidence": row.get("evidence"),
                        "resolution_status": "pending_tier3",
                    }
                    upsert_company(conn, company_record)

                log_pipeline_run(conn, "tier1", len(tier1_df), len(resolved_tier1),
                                  cost_usd=0.0, api_calls_made=0)

            print(f"Tier 1 resolved {len(resolved_tier1)} / {len(tier1_df)}. "
                  f"{len(residual)} pass to Tier 3 (search + Claude batch). All written to Supabase.")

        if len(residual) > 0:
            residual_companies = residual.to_dict("records")

            seen_names = set()
            deduped = []
            for c in residual_companies:
                name = c.get("name_cleaned")
                if is_junk_company_name(name):
                    continue
                if name in seen_names:
                    continue
                seen_names.add(name)
                deduped.append(c)
            dropped = len(residual_companies) - len(deduped)
            if dropped > 0:
                print(f"Dropped {dropped} junk/duplicate company names before Tier 3.")
            residual_companies = deduped

            if TIER3_TEST_LIMIT > 0:
                residual_companies = residual_companies[:TIER3_TEST_LIMIT]
                print(f"=== TIER 3 TEST MODE: limiting to first {len(residual_companies)} "
                      f"of {len(residual)} companies (TIER3_TEST_LIMIT set) ===")
            else:
                print(f"=== TIER 3: submitting {len(residual_companies)} companies to search + Claude batch ===")

            try:
                outcome = submit_batch(residual_companies)
                stats = outcome["stats"]

                run_summary["api_calls_made"] += stats["api_calls_made"]
                run_summary["cost_usd"] += stats["cost_usd"]
                run_summary["failed_count"] += stats["failed_count"]
                run_summary["needs_review_count"] += len(residual_companies)

                print(f"Tier 3 batch submitted: {outcome['batch_id']}")
                if outcome["batch_completed"]:
                    print(f"Batch completed within the polling window, "
                          f"{outcome['batch_applied_count']} classifications applied to Supabase.")
                else:
                    print(f"Batch still processing on Anthropic's side. "
                          f"Batch ID logged for manual follow-up: {outcome['batch_id']}")

            except SearchQuotaExceeded as e:
                stats = getattr(e, "stats", {})
                run_summary["api_calls_made"] += stats.get("api_calls_made", 0)
                run_summary["cost_usd"] += stats.get("cost_usd", 0.0)
                run_summary["failed_count"] += stats.get("failed_count", 0)

                print(f"=== SERPER QUOTA/RATE LIMIT EXCEEDED - stopping Tier 3 ===\n{e}")
                print("Note: any companies already searched successfully before this error "
                      "are checkpointed in Supabase (tier3_search_log) and will be skipped, "
                      "not re-paid for, on the next run. All companies remain visible in "
                      "the companies table tagged pending_tier3 either way.")
                run_summary["needs_review_count"] += len(residual_companies)
                write_split_outputs(tagged, run_summary)
                with get_conn() as conn:
                    log_pipeline_run(
                        conn, "tier3_stopped_budget",
                        run_summary["companies_processed"],
                        run_summary["companies_resolved"],
                        notes=str(e),
                        cost_usd=run_summary["cost_usd"],
                        api_calls_made=run_summary["api_calls_made"],
                        failed_count=run_summary["failed_count"],
                        status="quota_exceeded",
                    )
                send_budget_exceeded("Serper", str(e))
                print("Run halted cleanly, everything resolved so far is saved and in Supabase.")
                return

        write_split_outputs(tagged, run_summary)

        with get_conn() as conn:
            log_pipeline_run(
                conn, "full_run",
                run_summary["companies_processed"],
                run_summary["companies_resolved"],
                cost_usd=run_summary["cost_usd"],
                api_calls_made=run_summary["api_calls_made"],
                failed_count=run_summary["failed_count"],
                status="ok",
            )

        send_run_completed(run_summary)
        print(f"=== Run complete. {run_summary['people_inserted']} people inserted. "
              f"{run_summary['companies_written_to_supabase']} companies written to Supabase. "
              f"${run_summary['cost_usd']} estimated Tier 3 spend. ===")

    except Exception as e:
        error_text = traceback.format_exc()
        print(f"=== HARD FAILURE ===\n{error_text}")
        send_run_failed(tier="full_run", error=str(e) + "\n\n" + error_text[-1000:])
        sys.exit(1)


if __name__ == "__main__":
    main()