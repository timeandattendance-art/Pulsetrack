"""
tier0_local_signals.py — free, local-only company/person resolution.

Uses only signals already present in the source data:
  1. Contact count vs. staff-bucket ceiling -> data_garbage (evidence: impossible headcount)
  2. Explicit Job Change Type flag (New Hire / New Promotion) -> resolves which contact is current
  3. Franchise / multi-partner-firm / membership-org keyword detection on title text

Every resolution carries a confidence score and an evidence string. Nothing is
flagged data_garbage or auto-resolved without a concrete, checkable trigger —
"insufficient evidence" always falls through to needs_review, never to a guess.
"""

import re
import pandas as pd
import numpy as np
from pipeline.config import STAFF_BUCKETS

FRANCHISE_KEYWORDS = re.compile(r"franchise", re.IGNORECASE)
LAW_FIRM_NAME_PATTERN = re.compile(r"llp|llc|& associates|law office|law firm", re.IGNORECASE)
LAW_FIRM_TITLE_PATTERN = re.compile(r"partner|lawyer|counsel", re.IGNORECASE)
ORG_NAME_PATTERN = re.compile(r"chapter|fbla|deca|association|network|society", re.IGNORECASE)


def parse_tenure_months(value):
    if pd.isna(value) or value == "":
        return np.nan
    s = str(value).lower()
    years = re.search(r"(\d+)\s*year", s)
    months = re.search(r"(\d+)\s*month", s)
    total, found = 0, False
    if years:
        total += int(years.group(1)) * 12
        found = True
    if months:
        total += int(months.group(1))
        found = True
    return total if found else np.nan


def classify_company_group(name: str, group: pd.DataFrame) -> dict:
    """
    Runs the full Tier-0 rule set against all contact rows for one company.
    Returns a dict ready to merge into the companies table record.
    """
    n_contacts = len(group)
    staff_bucket = pd.to_numeric(group.get("Company Staff Count"), errors="coerce").iloc[0] \
        if "Company Staff Count" in group.columns else np.nan
    titles_lower = group.get("Title", pd.Series(dtype=str)).fillna("").str.lower()

    # --- Trigger 1: impossible headcount (evidence-based data_garbage) ---
    if pd.notna(staff_bucket) and n_contacts > staff_bucket:
        return {
            "company_type": "data_garbage",
            "usable_lead": False,
            "usable_lead_reason": f"{n_contacts} contacts exceed staff bucket ceiling of {int(staff_bucket)}",
            "classification_confidence": 0.85,
            "classification_source": "tier0_signal",
            "classification_evidence": "contact_count_exceeds_staff_bucket_ceiling",
            "resolution_status": "auto_resolved",
        }

    # --- Trigger 2: franchise keyword in title text ---
    if titles_lower.str.contains(FRANCHISE_KEYWORDS).any():
        return {
            "company_type": "franchise_unit",
            "usable_lead": True,
            "usable_lead_reason": "title text contains explicit 'franchise' reference",
            "classification_confidence": 0.75,
            "classification_source": "tier0_signal",
            "classification_evidence": "franchise_keyword_in_title",
            "resolution_status": "auto_resolved",
        }

    # --- Trigger 3: multi-partner firm (law-firm-shaped name + partner-type titles) ---
    if LAW_FIRM_NAME_PATTERN.search(name or "") and titles_lower.str.contains(LAW_FIRM_TITLE_PATTERN).any():
        return {
            "company_type": "multi_partner_firm",
            "usable_lead": True,
            "usable_lead_reason": "law-firm-shaped name with multiple partner/lawyer titles",
            "classification_confidence": 0.7,
            "classification_source": "tier0_signal",
            "classification_evidence": "law_firm_name_and_partner_titles",
            "resolution_status": "auto_resolved",
        }

    # --- Trigger 4: membership / chapter org ---
    if ORG_NAME_PATTERN.search(name or ""):
        return {
            "company_type": "membership_or_chapter_org",
            "usable_lead": False,
            "usable_lead_reason": "company name matches membership/chapter organization pattern",
            "classification_confidence": 0.65,
            "classification_source": "tier0_signal",
            "classification_evidence": "org_name_keyword_match",
            "resolution_status": "auto_resolved",
        }

    # --- Trigger 5: explicit job change event resolves which contact is current ---
    if n_contacts > 1 and "Job Change Type" in group.columns:
        changers = group[group["Job Change Type"].isin(["New Hire", "New Promotion"])]
        if len(changers) == 1:
            return {
                "company_type": "standalone_business",
                "usable_lead": True,
                "usable_lead_reason": "single explicit job-change event identifies current contact",
                "classification_confidence": 0.75,
                "classification_source": "tier0_signal",
                "classification_evidence": f"job_change_event:{changers.iloc[0]['Contact Full Name']}",
                "resolution_status": "auto_resolved",
                "_current_contact_name": changers.iloc[0]["Contact Full Name"],
            }

    # --- No reliable free signal found: explicitly fall through, do not guess ---
    if n_contacts == 1:
        return {
            "company_type": "standalone_business",
            "usable_lead": True,
            "usable_lead_reason": "single contact, no ambiguity",
            "classification_confidence": 0.9,
            "classification_source": "tier0_signal",
            "classification_evidence": "single_contact_no_conflict",
            "resolution_status": "auto_resolved",
        }

    return {
        "company_type": "unresolved",
        "usable_lead": None,
        "usable_lead_reason": "multiple contacts, no reliable free signal to resolve",
        "classification_confidence": 0.0,
        "classification_source": "tier0_signal",
        "classification_evidence": "no_reliable_signal",
        "resolution_status": "needs_review",
    }


def run_tier0(csv_path: str, name_col: str = "Company Name - Cleaned") -> pd.DataFrame:
    """
    Loads the raw CSV and runs Tier-0 classification on every company group.
    Returns a DataFrame, one row per company, ready for DB upsert.
    """
    df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    results = []
    for name, group in df.groupby(name_col):
        result = classify_company_group(name, group)
        result["name"] = name
        result["name_cleaned"] = name
        result["n_contacts"] = len(group)
        results.append(result)
    return pd.DataFrame(results)


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "data/leads.csv"
    out = run_tier0(path)
    print(out["company_type"].value_counts())
    print(out["resolution_status"].value_counts())
    out.to_csv("tier0_output.csv", index=False)
    print("Written to tier0_output.csv")
