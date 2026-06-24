"""
merge_leads.py — combines multiple Seamless-style export CSVs into one
dataframe, preserving every row (nothing is ever deleted here), and tags
each row with:

    merge_status   : single_contact | duplicate_person | conflict_check_needed
    title_type     : singular_top | shared_role | other
    true_conflict  : True/False — genuine 2+ different people claiming a
                      singular top title (Owner/President/CEO/Founder...)
                      at the same company. This is the group that actually
                      needs paid verification; shared roles like "Partner"
                      are correctly excluded.

This logic mirrors what was validated by hand against real companies
before any paid verification step runs.
"""

import pandas as pd

SINGULAR_TITLES = [
    "owner", "president", "chief executive officer", "ceo", "business owner",
    "founder", "co-founder", "company owner", "president & ceo", "franchise owner",
    "president/ceo", "founder & ceo", "proprietor", "owner operator",
    "president and ceo",
]

SHARED_TITLES = [
    "partner", "co-owner", "venture partner", "managing partner", "senior partner",
]

LI_COL = "Contact LI Profile URL"
TITLE_COL = "Title"
SENIORITY_COL = "Seniority"


def load_and_merge(csv_paths: list[str]) -> pd.DataFrame:
    dfs = []
    for path in csv_paths:
        df = pd.read_csv(path, dtype=str, low_memory=False)
        df["source_file"] = path.split("/")[-1]
        dfs.append(df)
    combined = pd.concat(dfs, ignore_index=True)
    return combined


def tag_conflicts(combined: pd.DataFrame) -> pd.DataFrame:
    combined = combined.copy()

    combined["company_key"] = combined.get("Company Website Domain")
    if "Company Name - Cleaned" in combined.columns:
        combined["company_key"] = combined["company_key"].fillna(
            combined["Company Name - Cleaned"]
        )

    distinct_people = combined.groupby("company_key")[LI_COL].transform("nunique")
    row_count = combined.groupby("company_key")[LI_COL].transform("count")
    combined["company_distinct_people"] = distinct_people
    combined["company_row_count"] = row_count

    def classify(row):
        if row["company_row_count"] == 1:
            return "single_contact"
        elif row["company_distinct_people"] == 1:
            return "duplicate_person"
        return "conflict_check_needed"

    combined["merge_status"] = combined.apply(classify, axis=1)
    combined["is_c_level"] = (
        combined.get(SENIORITY_COL, "").fillna("").str.contains("C-Level", case=False)
    )

    t = combined.get(TITLE_COL, "").fillna("").str.lower().str.strip()
    combined["title_type"] = "other"
    combined.loc[t.isin(SINGULAR_TITLES), "title_type"] = "singular_top"
    combined.loc[t.isin(SHARED_TITLES), "title_type"] = "shared_role"

    singular_rows = combined[combined["title_type"] == "singular_top"]
    distinct_top_people = singular_rows.groupby("company_key")[LI_COL].nunique()
    true_conflict_companies = distinct_top_people[distinct_top_people >= 2].index

    combined["true_conflict"] = combined["company_key"].isin(true_conflict_companies)

    return combined


def merge_and_tag(csv_paths: list[str]) -> pd.DataFrame:
    combined = load_and_merge(csv_paths)
    tagged = tag_conflicts(combined)
    return tagged
