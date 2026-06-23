"""
pipeline_tier3_collect.py — run this after a Tier 3 batch finishes processing
to pull results and write final classifications into Supabase.

Usage:
    python pipeline_tier3_collect.py <batch_id>
"""

import sys
import anthropic
from pipeline.config import ANTHROPIC_API_KEY
from pipeline.tier3_search_classify import retrieve_batch_results
from pipeline.db import get_conn, upsert_company, log_pipeline_run


def main(batch_id: str):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    batch = client.messages.batches.retrieve(batch_id)
    print(f"Batch status: {batch.processing_status}")

    if batch.processing_status != "ended":
        print("Batch not finished yet. Try again later.")
        return

    results = retrieve_batch_results(batch_id)
    print(f"Retrieved {len(results)} results.")

    resolved_count = 0
    with get_conn() as conn:
        for r in results:
            # custom_id format: "company-{i}-{name_prefix}"
            name_part = r["custom_id"].split("-", 2)[-1]
            company_record = {
                "name": name_part,
                "name_cleaned": name_part,
                "domain": None, "website": None, "industry": None, "revenue_range": None,
                "staff_bucket": None, "city": None, "state": None, "country": None, "description": None,
                "company_type": r.get("company_type", "needs_manual_review"),
                "usable_lead": r.get("usable_lead"),
                "usable_lead_reason": r.get("evidence", ""),
                "classification_confidence": r.get("confidence", 0.0),
                "classification_source": "tier3_search",
                "classification_evidence": r.get("evidence", ""),
                "resolution_status": "auto_resolved" if r.get("confidence", 0) >= 0.6 else "needs_review",
            }
            upsert_company(conn, company_record)
            if company_record["resolution_status"] == "auto_resolved":
                resolved_count += 1

        log_pipeline_run(conn, "tier3", len(results), resolved_count)

    print(f"Tier 3 resolved {resolved_count} / {len(results)} with confidence >= 0.6.")
    print(f"Remaining {len(results) - resolved_count} flagged needs_review for manual check.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python pipeline_tier3_collect.py <batch_id>")
        sys.exit(1)
    main(sys.argv[1])
