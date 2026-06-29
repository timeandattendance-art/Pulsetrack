"""
reorder_contacts.py — post-processing pass on the merged leads CSV.

Does NOT touch the Tier 0/1/3 company resolution pipeline or Supabase.
Operates purely on the merged contact data, reordering (never deleting)
email and phone columns so the single best candidate always sits in the
"Email 1" / "Contact Phone 1" position, with everything else preserved
in its original form, just swapped to a different slot.

Email winner selection, in priority order:
    1. Exclude any candidate marked "invalid" outright (never useful)
    2. Among remaining candidates, prefer one whose domain matches the
       company's website domain
    3. Among whatever's left, highest "Total AI" score wins
    4. Ties broken by original column order (left to right)

Phone winner selection:
    Same as email but no validity/domain check available, purely
    highest "Total AI" score wins, ties broken by column order.

A new email_mailability column records whether the winning email was
marked "valid" or "do not mail" in the source data, so do-not-mail
addresses are clearly flagged for snail mail rather than excluded.

Usage:
    python -m pipeline.reorder_contacts data/leads.csv outputs/leads_reordered.csv
"""

import sys
import re
import pandas as pd

# Email candidate groups: (value_col, validation_col, score_col)
EMAIL_GROUPS = [
    ("Contact Email", "Contact Email Validation", "Contact Email Total AI"),
    ("Contact Email 2", "Contact Email 2 Validation", "Contact Email 2 Total AI"),
    ("Contact Email 3", "Contact Email 3 Validation", "Contact Email 3 Total AI"),
] + [
    (f"Email {i}", f"Email {i} Validation", f"Email {i} Total AI")
    for i in range(1, 11)
]

# Phone candidate groups: (value_col, score_col) — no validation column exists
PHONE_GROUPS = [
    (f"Contact Phone {i}", f"Contact Phone {i} Total AI")
    for i in range(1, 11)
]

DOMAIN_COL = "Company Website Domain"


def parse_score(val) -> float:
    """Score columns are strings like '98%'. Returns 0.0 if missing/unparseable."""
    if pd.isna(val):
        return 0.0
    s = str(val).strip().replace("%", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def extract_domain_from_email(email: str) -> str:
    if not email or not isinstance(email, str) or "@" not in email:
        return ""
    return email.split("@")[-1].strip().lower()


def pick_best_email(row) -> tuple:
    """
    Returns (winning_group_index, mailability) where winning_group_index
    is the index into EMAIL_GROUPS of the best candidate, or (None, None)
    if no usable (non-invalid) candidate exists.
    """
    company_domain = str(row.get(DOMAIN_COL, "")).strip().lower()

    candidates = []
    for idx, (val_col, valid_col, score_col) in enumerate(EMAIL_GROUPS):
        email_val = row.get(val_col)
        validation = str(row.get(valid_col, "")).strip().lower()
        score = parse_score(row.get(score_col))

        if pd.isna(email_val) or not str(email_val).strip():
            continue
        if validation == "invalid":
            continue

        domain_match = (
            company_domain != "" and
            extract_domain_from_email(str(email_val)) == company_domain
        )
        candidates.append({
            "idx": idx,
            "score": score,
            "domain_match": domain_match,
            "validation": validation,
        })

    if not candidates:
        return None, None

    # Sort: domain match first (True > False), then score descending,
    # then original order (idx ascending) as final tiebreak
    candidates.sort(key=lambda c: (-c["domain_match"], -c["score"], c["idx"]))
    winner = candidates[0]

    mailability = "valid" if winner["validation"] == "valid" else "do_not_mail"
    return winner["idx"], mailability


def pick_best_phone(row) -> int:
    """Returns the index into PHONE_GROUPS of the best candidate, or None."""
    candidates = []
    for idx, (val_col, score_col) in enumerate(PHONE_GROUPS):
        phone_val = row.get(val_col)
        if pd.isna(phone_val) or not str(phone_val).strip():
            continue
        score = parse_score(row.get(score_col))
        candidates.append({"idx": idx, "score": score})

    if not candidates:
        return None

    candidates.sort(key=lambda c: (-c["score"], c["idx"]))
    return candidates[0]["idx"]


def swap_email_groups(row, winner_idx: int):
    """Swaps the winning email group's three columns with Email 1's slot.
    If the winner IS already Email 1's slot (index 3 in EMAIL_GROUPS,
    since Contact Email/2/3 come first), nothing to swap."""
    target_idx = 3  # index of ("Email 1", "Email 1 Validation", "Email 1 Total AI")
    if winner_idx == target_idx:
        return row

    win_cols = EMAIL_GROUPS[winner_idx]
    tgt_cols = EMAIL_GROUPS[target_idx]

    win_vals = [row.get(c) for c in win_cols]
    tgt_vals = [row.get(c) for c in tgt_cols]

    for col, val in zip(tgt_cols, win_vals):
        row[col] = val
    for col, val in zip(win_cols, tgt_vals):
        row[col] = val

    return row


def swap_phone_groups(row, winner_idx: int):
    """Swaps the winning phone group's two columns with Contact Phone 1's slot."""
    target_idx = 0  # index of ("Contact Phone 1", "Contact Phone 1 Total AI")
    if winner_idx == target_idx:
        return row

    win_cols = PHONE_GROUPS[winner_idx]
    tgt_cols = PHONE_GROUPS[target_idx]

    win_vals = [row.get(c) for c in win_cols]
    tgt_vals = [row.get(c) for c in tgt_cols]

    for col, val in zip(tgt_cols, win_vals):
        row[col] = val
    for col, val in zip(win_cols, tgt_vals):
        row[col] = val

    return row


def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    missing_email_cols = [c for group in EMAIL_GROUPS for c in group if c not in df.columns]
    missing_phone_cols = [c for group in PHONE_GROUPS for c in group if c not in df.columns]
    if missing_email_cols:
        print(f"WARNING: missing expected email columns, skipping those groups: {missing_email_cols}")
    if missing_phone_cols:
        print(f"WARNING: missing expected phone columns, skipping those groups: {missing_phone_cols}")

    mailability_values = []
    rows = []

    for _, row in df.iterrows():
        row = row.copy()

        email_winner_idx, mailability = pick_best_email(row)
        if email_winner_idx is not None:
            row = swap_email_groups(row, email_winner_idx)
        mailability_values.append(mailability if mailability else "")

        phone_winner_idx = pick_best_phone(row)
        if phone_winner_idx is not None:
            row = swap_phone_groups(row, phone_winner_idx)

        rows.append(row)

    result = pd.DataFrame(rows)
    result["email_mailability"] = mailability_values
    return result


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python -m pipeline.reorder_contacts <input_csv> <output_csv>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    print(f"Reading {input_path} ...")
    df = pd.read_csv(input_path, dtype=str, low_memory=False)
    print(f"Loaded {len(df)} rows.")

    result = process_dataframe(df)

    result.to_csv(output_path, index=False)
    print(f"Wrote {len(result)} rows -> {output_path}")
    print("All original columns preserved. Best email/phone swapped into "
          "Email 1 / Contact Phone 1 slots. email_mailability column added.")