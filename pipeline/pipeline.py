"""
pipeline.py — orchestrates Tier 0 -> Tier 1 -> Tier 3 end to end and
writes resolved companies/people into Supabase.

Usage:
    python pipeline.py data/leads.csv

This runs Tier 0 and Tier 1 synchronously (fast, cheap/free), then submits
the Tier 3 batch and exits — Tier 3 batch results need to be polled
separately since they process asynchronously (see tier3_search_classify.py).
"""

import sys
import asyncio
import pandas as pd

from pipeline.tier0_local_signals import run_tier0
from pipeline.tier1_website_scrape import run_tier1_batch
from pipeline.tier3_search_classify import submit_batch
from pipeline.db import get_conn, upsert_company, insert_person, log_pipeline_run


def main(csv_path: str):
    print(f"=== TIER 0: local signal resolution on {csv_path} ===")
    tier0_results = run_tier0(csv_path)
    print(tier0_results["resolution_status"].value_counts())

    auto_resolved = tier0_results[tier0_results["resolution_status"] == "auto_resolved"]
    needs_review = tier0_results[tier0_results["resolution_status"] == "needs_review"]

    raw_df = pd.read_csv(csv_path, dtype=str, low_memory=False)

    # --- Write Tier 0 auto-resolved companies straight to DB ---
    with get_conn() as conn:
        for _, row in auto_resolved.iterrows():
            company_record = {
                "name": row["name"],
                "name_cleaned": row["name_cleaned"],
                "domain": None,
                "website": None,
                "industry": None,
                "revenue_range": None,
                "staff_bucket": None,
                "city": None,
                "state": None,
                "country": None,
                "description": None,
                "company_type": row["company_type"],
                "usable_lead": row["usable_lead"],
                "usable_lead_reason": row["usable_lead_reason"],
                "classification_confidence": row["classification_confidence"],
                "classification_source": row["classification_source"],
                "classification_evidence": row["classification_evidence"],
                "resolution_status": row["resolution_status"],
            }
            upsert_company(conn, company_record)
        log_pipeline_run(conn, "tier0", len(tier0_results), len(auto_resolved))

    print(f"Tier 0 resolved {len(auto_resolved)} / {len(tier0_results)} companies. "
          f"{len(needs_review)} pass to Tier 1.")

    if len(needs_review) == 0:
        print("Nothing left to resolve. Done.")
        return

    # --- TIER 1: website scraping for the rest ---
    print(f"=== TIER 1: website scraping for {len(needs_review)} companies ===")
    companies_to_check = []
    for _, row in needs_review.iterrows():
        group = raw_df[raw_df["Company Name - Cleaned"] == row["name_cleaned"]]
        domain = group["Website"].dropna().iloc[0] if not group["Website"].dropna().empty else None
        names = group["Contact Full Name"].dropna().unique().tolist()
        companies_to_check.append({
            "name_cleaned": row["name_cleaned"],
            "domain": domain,
            "candidate_names": names,
            "n_contacts": row["n_contacts"],
        })

    tier1_results = asyncio.run(run_tier1_batch(companies_to_check))
    tier1_df = pd.DataFrame(tier1_results)
    print(tier1_df["resolution"].value_counts())

    resolved_tier1 = tier1_df[tier1_df["resolution"] == "resolved_tier1"]
    residual = tier1_df[tier1_df["resolution"].isin(["unresolved_tier1", "ambiguous_tier1"])]

    with get_conn() as conn:
        for _, row in resolved_tier1.iterrows():
            company_record = {
                "name": row["name_cleaned"],
                "name_cleaned": row["name_cleaned"],
                "domain": row["domain"],
                "website": row["domain"],
                "industry": None, "revenue_range": None, "staff_bucket": None,
                "city": None, "state": None, "country": None, "description": None,
                "company_type": "standalone_business",
                "usable_lead": True,
                "usable_lead_reason": f"confirmed via company website: {row['evidence']}",
                "classification_confidence": row["confidence"],
                "classification_source": "tier1_website",
                "classification_evidence": row["evidence"],
                "resolution_status": "auto_resolved",
            }
            upsert_company(conn, company_record)
        log_pipeline_run(conn, "tier1", len(tier1_df), len(resolved_tier1))

    print(f"Tier 1 resolved {len(resolved_tier1)} / {len(tier1_df)}. "
          f"{len(residual)} pass to Tier 3 (search + Claude batch).")

    if len(residual) == 0:
        print("Nothing left to resolve. Done.")
        return

    # --- TIER 3: submit batch, exits without waiting (async) ---
    print(f"=== TIER 3: submitting {len(residual)} companies to search + Claude Haiku batch ===")
    residual_companies = residual.to_dict("records")
    batch_id = submit_batch(residual_companies)
    print(f"Tier 3 batch submitted: {batch_id}")
    print("Run `python pipeline_tier3_collect.py <batch_id>` once the batch finishes to write results to DB.")


if __name__ == "__main__":
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "data/leads.csv"
    main(csv_path)
