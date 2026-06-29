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

DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID")
DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw_from_drive")
MERGED_PATH = os.path.join(DATA_DIR, "leads.csv")
OUTPUT_DIR = "outputs"


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
    }

    try:
        tagged = fetch_and_merge_source_data()

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
                upsert_company(conn, company_record)
            log_pipeline_run(conn, "tier0", len(tier0_results), len(auto_resolved),
                            cost_usd=0.0, api_calls_made=0)

        print(f"Tier 0 resolved {len(auto_resolved)} / {len(tier0_results)} companies. "
              f"{len(needs_review)} pass to Tier 1.")

        resolved_tier1 = pd.DataFrame()
        residual = pd.DataFrame()

        if len(needs_review) > 0:
            print(f"=== TIER 1: website scraping for {len(needs_review)} companies ===")
            raw_df = pd.read_csv(MERGED_PATH, dtype=str, low_memory=False)
            companies_to_check = []
            for _, row in needs_review.iterrows():
                group = raw_df[raw_df["Company Name - Cleaned"] == row["name_cleaned"]]
                domain = (group["Website"].dropna().iloc[0]
                          if "Website" in group.columns and not group["Website"].dropna().empty
                          else None)
                names = (group["Contact Full Name"].dropna().unique().tolist()
                         if "Contact Full Name" in group.columns else [])
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
                    upsert_company(conn, company_record)
                log_pipeline_run(conn, "tier1", len(tier1_df), len(resolved_tier1),
                                cost_usd=0.0, api_calls_made=0)

            print(f"Tier 1 resolved {len(resolved_tier1)} / {len(tier1_df)}. "
                  f"{len(residual)} pass to Tier 3 (search + Claude batch).")

        if len(residual) > 0:
            residual_companies = residual.to_dict("records")
            if TIER3_TEST_LIMIT > 0:
                residual_companies = residual_companies[:TIER3_TEST_LIMIT]
                print(f"=== TIER 3 TEST MODE: limiting to first {len(residual_companies)} "
                      f"of {len(residual)} companies (TIER3_TEST_LIMIT set) ===")
            else:
                print(f"=== TIER 3: submitting {len(residual_companies)} companies to search + Claude batch ===")
            try:
                batch_id = submit_batch(residual_companies)
                print(f"Tier 3 batch submitted: {batch_id}")
                print("Run `python pipeline_tier3_collect.py <batch_id>` once the batch finishes.")
                run_summary["needs_review_count"] += len(residual_companies)
            except SearchQuotaExceeded as e:
                print(f"=== SERPER QUOTA/RATE LIMIT EXCEEDED - stopping Tier 3 ===\n{e}")
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
                print("Run halted cleanly - everything resolved so far is saved and in Supabase.")
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
        print("=== Run complete ===")

    except Exception as e:
        error_text = traceback.format_exc()
        print(f"=== HARD FAILURE ===\n{error_text}")
        send_run_failed(tier="full_run", error=str(e) + "\n\n" + error_text[-1000:])
        sys.exit(1)


if __name__ == "__main__":
    main()