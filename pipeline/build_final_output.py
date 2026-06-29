"""
build_final_output.py — the actual final deliverable script.

Reads the tagged merged CSV (one row per contact, already has true_conflict
and title_type per row from merge_leads.py) and Tier 3's per-contact CEO
resolution results (saved to JSON by pipeline.py), then produces ONE output
CSV with every original column intact plus three new columns:

    CEO T/F                — "true" for the confirmed real lead, "false"
                              for a duplicate claimant, blank if not a
                              singular-top-title claimant at all (not part
                              of any conflict to begin with)
    Duplicate               — "duplicate" on the false rows, blank otherwise
    Company Structure Flag  — the detected company_type (e.g. franchise_unit,
                              multi_partner_firm), blank if not yet determined

Logic per row:
    - If title_type != "singular_top": this contact never claimed the
      disputed title at all, leave all three columns blank.
    - If title_type == "singular_top" and true_conflict == False: only
      one person claims this role at this company, auto-confirmed,
      CEO T/F = true, Duplicate blank, structure_flag = standalone_business
      (no search was needed or run).
    - If title_type == "singular_top" and true_conflict == True: look up
      this exact company + contact name in Tier 3's per_contact_flags.
      If found, use those real values. If the company hasn't been resolved
      by Tier 3 yet (e.g. quota ran out, or law firm/membership org which
      Tier 3 deliberately leaves blank), columns stay blank for manual review.

Email/phone columns get reordered (best candidate first, do_not_mail
flagged) using the same logic as reorder_contacts.py.

Usage:
    python -m pipeline.build_final_output data/leads.csv tier3_results.json outputs/final_leads.csv
"""

import sys
import json
import pandas as pd

from pipeline.reorder_contacts import process_dataframe

NAME_COL = "Company Name - Cleaned"
CONTACT_NAME_COL = "Contact Full Name"
TITLE_TYPE_COL = "title_type"
TRUE_CONFLICT_COL = "true_conflict"


def load_tier3_results(json_path: str) -> dict:
    """
    Returns {company_name: {contact_name: {ceo_tf, duplicate_flag,
    structure_flag}}}. Returns an empty dict if the file doesn't exist
    yet (e.g. no conflicts were found, or Tier 3 hasn't run).
    """
    try:
        with open(json_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"WARNING: {json_path} not found — no Tier 3 results to merge in. "
              f"Conflicted companies will have blank CEO T/F columns.")
        return {}


def resolve_row_flags(row, tier3_results: dict) -> dict:
    title_type = row.get(TITLE_TYPE_COL, "other")
    true_conflict = bool(row.get(TRUE_CONFLICT_COL, False))
    company_name = row.get(NAME_COL, "")
    contact_name = row.get(CONTACT_NAME_COL, "")

    if title_type != "singular_top":
        # Never claimed the disputed top title — not part of this at all.
        return {"CEO T/F": "", "Duplicate": "", "Company Structure Flag": ""}

    if not true_conflict:
        # Only one person claims this role here — auto-confirmed, no search needed.
        return {"CEO T/F": "true", "Duplicate": "", "Company Structure Flag": "standalone_business"}

    # Genuine conflict — look up Tier 3's resolution for this exact contact.
    company_flags = tier3_results.get(company_name, {})
    contact_flags = company_flags.get(contact_name)
    if contact_flags is None:
        # Not yet resolved (quota ran out, law firm/membership org left
        # blank on purpose, or this run hasn't reached it yet).
        structure = company_flags.get("_structure_fallback", "")
        return {"CEO T/F": "", "Duplicate": "", "Company Structure Flag": structure}

    return {
        "CEO T/F": contact_flags.get("ceo_tf", ""),
        "Duplicate": contact_flags.get("duplicate_flag", ""),
        "Company Structure Flag": contact_flags.get("structure_flag", ""),
    }


def append_ceo_columns(df: pd.DataFrame, tier3_results: dict) -> pd.DataFrame:
    ceo_tf_vals, dup_vals, structure_vals = [], [], []
    for _, row in df.iterrows():
        flags = resolve_row_flags(row, tier3_results)
        ceo_tf_vals.append(flags["CEO T/F"])
        dup_vals.append(flags["Duplicate"])
        structure_vals.append(flags["Company Structure Flag"])

    df["CEO T/F"] = ceo_tf_vals
    df["Duplicate"] = dup_vals
    df["Company Structure Flag"] = structure_vals
    return df


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python -m pipeline.build_final_output <tagged_csv> <tier3_results_json> <output_csv>")
        sys.exit(1)

    input_path = sys.argv[1]
    tier3_json_path = sys.argv[2]
    output_path = sys.argv[3]

    print(f"Reading {input_path} ...")
    df = pd.read_csv(input_path, dtype=str, low_memory=False)
    print(f"Loaded {len(df)} contact rows.")

    for required_col in (NAME_COL, CONTACT_NAME_COL, TITLE_TYPE_COL, TRUE_CONFLICT_COL):
        if required_col not in df.columns:
            print(f"ERROR: expected column '{required_col}' not found — "
                  f"make sure this is the tagged output from merge_and_tag, not the raw source CSV.")
            sys.exit(1)

    print("Reordering email/phone columns (best candidate first)...")
    df = process_dataframe(df)

    print(f"Loading Tier 3 results from {tier3_json_path} ...")
    tier3_results = load_tier3_results(tier3_json_path)
    print(f"Loaded resolutions for {len(tier3_results)} conflicted companies.")

    print("Appending CEO T/F, Duplicate, and Company Structure Flag columns...")
    df = append_ceo_columns(df, tier3_results)

    confirmed = (df["CEO T/F"] == "true").sum()
    duplicates = (df["Duplicate"] == "duplicate").sum()
    print(f"{confirmed} rows confirmed CEO T/F = true, {duplicates} rows flagged as duplicate.")

    df.to_csv(output_path, index=False)
    print(f"Wrote {len(df)} rows -> {output_path}")
    print("ONE file, all original columns preserved, email/phone reordered, "
          "CEO T/F / Duplicate / Company Structure Flag appended per-row.")