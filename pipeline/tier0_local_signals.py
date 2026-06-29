"""
tier0_local_signals.py — free, local-only company/person resolution.

Now takes the already-merged, already-tagged dataframe from merge_leads.py
(specifically its true_conflict column) instead of re-reading the raw CSV,
so duplicate detection is precise: true_conflict only fires when 2+ distinct
people claim the same singular top title at one company, not just "more
than one contact."

New output columns used for the final deliverable:
    CEO T/F               — "true" for the confirmed real lead, "false" for
                             a duplicate claimant once Tier 3 confirms who's
                             real, blank only while still pending Tier 3
    Duplicate              — "duplicate" on the false rows, blank otherwise
    Company Structure Flag — the company_type value, surfaced so franchises
                             and law-firm/multi-partner companies are visible

CEO T/F only applies to rows whose title_type is "singular_top" (the actual
CEO/Owner/President/Founder claimants). Other staff at the same company are
not part of the conflict and are left blank, since they were never claiming
the role being disputed.

If true_conflict is False for a company, its singular_top row (there's only
one) is auto-confirmed instantly: CEO T/F = true, no search needed.
If true_conflict is True, CEO T/F stays blank here on purpose — Tier 3 is
responsible for searching and filling in which claimant is real.
"""

import re
import pandas as pd
import numpy as np
from pipeline.config import STAFF_BUCKETS

FRANCHISE_KEYWORDS = re.compile(r"franchise", re.IGNORECASE)
LAW_FIRM_NAME_PATTERN = re.compile(r"llp|llc|& associates|law office|law firm", re.IGNORECASE)
LAW_FIRM_TITLE_PATTERN = re.compile(r"partner|lawyer|counsel", re.IGNORECASE)
ORG_NAME_PATTERN = re.compile(r"chapter|fbla|deca|association|network|society", re.IGNORECASE)

NAME_COL = "Company Name - Cleaned"


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
    Uses the true_conflict column already computed by merge_and_tag to
    decide whether this company needs Tier 3 or can be auto-confirmed now.
    """
    n_contacts = len(group)
    staff_bucket = pd.to_numeric(group.get("Company Staff Count"), errors="coerce").iloc[0] \
        if "Company Staff Count" in group.columns else np.nan
    titles_lower = group.get("Title", pd.Series(dtype=str)).fillna("").str.lower()
    has_true_conflict = bool(group.get("true_conflict", pd.Series([False])).any())

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
            "ceo_tf": "", "duplicate_flag": "", "structure_flag": "data_garbage",
        }

    # --- Trigger 2: franchise keyword in title text ---
    # Franchises still get resolved/searched if there's a real conflict,
    # just tagged "franchise_unit" so it's visible in Company Structure Flag.
    if titles_lower.str.contains(FRANCHISE_KEYWORDS).any():
        ceo_tf = "true" if not has_true_conflict else ""
        return {
            "company_type": "franchise_unit",
            "usable_lead": True,
            "usable_lead_reason": "title text contains explicit 'franchise' reference",
            "classification_confidence": 0.75,
            "classification_source": "tier0_signal",
            "classification_evidence": "franchise_keyword_in_title",
            "resolution_status": "auto_resolved" if not has_true_conflict else "needs_review",
            "ceo_tf": ceo_tf, "duplicate_flag": "", "structure_flag": "franchise_unit",
        }

    # --- Trigger 3: multi-partner firm (law-firm-shaped name + partner-type titles) ---
    # Per explicit instruction: never auto-resolve or search these, just flag
    # and leave CEO T/F and Duplicate blank for manual review.
    if LAW_FIRM_NAME_PATTERN.search(name or "") and titles_lower.str.contains(LAW_FIRM_TITLE_PATTERN).any():
        return {
            "company_type": "multi_partner_firm",
            "usable_lead": True,
            "usable_lead_reason": "law-firm-shaped name with multiple partner/lawyer titles",
            "classification_confidence": 0.7,
            "classification_source": "tier0_signal",
            "classification_evidence": "law_firm_name_and_partner_titles",
            "resolution_status": "auto_resolved",
            "ceo_tf": "", "duplicate_flag": "", "structure_flag": "multi_partner_firm",
        }

    # --- Trigger 4: membership / chapter org ---
    # Same as law firms: flag only, no auto-resolve, no search.
    if ORG_NAME_PATTERN.search(name or ""):
        return {
            "company_type": "membership_or_chapter_org",
            "usable_lead": False,
            "usable_lead_reason": "company name matches membership/chapter organization pattern",
            "classification_confidence": 0.65,
            "classification_source": "tier0_signal",
            "classification_evidence": "org_name_keyword_match",
            "resolution_status": "auto_resolved",
            "ceo_tf": "", "duplicate_flag": "", "structure_flag": "membership_or_chapter_org",
        }

    # --- Trigger 5: explicit job change event resolves which contact is current ---
    if has_true_conflict and "Job Change Type" in group.columns:
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
                "ceo_tf": "true", "duplicate_flag": "", "structure_flag": "standalone_business",
            }

    # --- No genuine conflict: auto-confirm instantly, no search needed ---
    if not has_true_conflict:
        return {
            "company_type": "standalone_business",
            "usable_lead": True,
            "usable_lead_reason": "no genuine top-title conflict, auto-confirmed",
            "classification_confidence": 0.9,
            "classification_source": "tier0_signal",
            "classification_evidence": "no_true_conflict",
            "resolution_status": "auto_resolved",
            "ceo_tf": "true", "duplicate_flag": "", "structure_flag": "standalone_business",
        }

    # --- Genuine duplicate situation, no free signal resolves it: send to Tier 3 ---
    # CEO T/F and Duplicate stay blank here on purpose — Tier 3 fills them in
    # once it searches and confirms who the real CEO is among these contacts.
    return {
        "company_type": "unresolved",
        "usable_lead": None,
        "usable_lead_reason": "multiple distinct people claiming the same top title — needs Tier 3 search",
        "classification_confidence": 0.0,
        "classification_source": "tier0_signal",
        "classification_evidence": "true_conflict",
        "resolution_status": "needs_review",
        "ceo_tf": "", "duplicate_flag": "", "structure_flag": "",
    }


def run_tier0(tagged_df: pd.DataFrame, name_col: str = NAME_COL) -> pd.DataFrame:
    """
    Takes the already-merged, already-tagged dataframe (from merge_and_tag,
    which includes the true_conflict column) and runs Tier-0 classification
    on every company group. Returns a DataFrame, one row per company,
    ready for DB upsert.
    """
    results = []
    for name, group in tagged_df.groupby(name_col):
        result = classify_company_group(name, group)
        result["name"] = name
        result["name_cleaned"] = name
        result["n_contacts"] = len(group)
        results.append(result)
    return pd.DataFrame(results)


if __name__ == "__main__":
    import sys
    from pipeline.merge_leads import merge_and_tag
    path = sys.argv[1] if len(sys.argv) > 1 else "data/leads.csv"
    tagged = merge_and_tag([path])
    out = run_tier0(tagged)
    print(out["company_type"].value_counts())
    print(out["resolution_status"].value_counts())
    print(out["ceo_tf"].value_counts())
    out.to_csv("tier0_output.csv", index=False)
    print("Written to tier0_output.csv")