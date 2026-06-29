"""
db.py — thin Postgres helper layer over the Supabase database.

Uses psycopg2 directly rather than an ORM, since the pipeline mostly does
batch upserts and simple lookups. Kept deliberately simple.

sanitize_value() converts pandas/numpy NaN, NaT, and None-like values into
a real Python None before anything reaches a SQL query. Without this,
pandas NaN (a float) gets passed to psycopg2 and Postgres rejects it when
compared against a text column (e.g. "operator does not exist: text =
double precision"), which crashed a real run on the domain field.
"""

import math
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from pipeline.config import SUPABASE_DB_URL


def sanitize_value(val):
    """Converts NaN/NaT/pandas-missing values to None. Passes everything
    else through unchanged."""
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    try:
        import pandas as pd
        if pd.isna(val):
            return None
    except (ImportError, TypeError, ValueError):
        pass
    return val


def sanitize_record(record: dict) -> dict:
    """Applies sanitize_value to every value in a dict, used right before
    any insert/update so NaN never reaches a SQL query."""
    return {k: sanitize_value(v) for k, v in record.items()}


@contextmanager
def get_conn():
    conn = psycopg2.connect(SUPABASE_DB_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_company(conn, company: dict) -> str:
    """
    Insert or update a company by (name_cleaned, domain). Returns the company id.
    """
    company = sanitize_record(company)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        select id from companies
        where name_cleaned = %s and (domain = %s or (%s is null and domain is null))
        limit 1
        """,
        (company.get("name_cleaned"), company.get("domain"), company.get("domain")),
    )
    row = cur.fetchone()

    if row:
        company_id = row["id"]
        cur.execute(
            """
            update companies set
                name = %(name)s,
                website = %(website)s,
                industry = %(industry)s,
                revenue_range = %(revenue_range)s,
                staff_bucket = %(staff_bucket)s,
                city = %(city)s,
                state = %(state)s,
                country = %(country)s,
                description = %(description)s,
                company_type = %(company_type)s,
                usable_lead = %(usable_lead)s,
                usable_lead_reason = %(usable_lead_reason)s,
                classification_confidence = %(classification_confidence)s,
                classification_source = %(classification_source)s,
                classification_evidence = %(classification_evidence)s,
                resolution_status = %(resolution_status)s,
                updated_at = now()
            where id = %(id)s
            """,
            {**company, "id": company_id},
        )
        return company_id
    else:
        cur.execute(
            """
            insert into companies (
                name, name_cleaned, domain, website, industry, revenue_range,
                staff_bucket, city, state, country, description,
                company_type, usable_lead, usable_lead_reason,
                classification_confidence, classification_source, classification_evidence,
                resolution_status
            ) values (
                %(name)s, %(name_cleaned)s, %(domain)s, %(website)s, %(industry)s, %(revenue_range)s,
                %(staff_bucket)s, %(city)s, %(state)s, %(country)s, %(description)s,
                %(company_type)s, %(usable_lead)s, %(usable_lead_reason)s,
                %(classification_confidence)s, %(classification_source)s, %(classification_evidence)s,
                %(resolution_status)s
            )
            returning id
            """,
            company,
        )
        return cur.fetchone()["id"]


def insert_person(conn, person: dict):
    person = sanitize_record(person)
    cur = conn.cursor()
    cur.execute(
        """
        insert into people (
            company_id, full_name, first_name, last_name, title, title_normalized,
            seniority, email, email_validation, phone, linkedin_url,
            is_current, resolution_confidence, resolution_source,
            time_in_role_months, time_at_company_months, job_change_type, raw_source_file
        ) values (
            %(company_id)s, %(full_name)s, %(first_name)s, %(last_name)s, %(title)s, %(title_normalized)s,
            %(seniority)s, %(email)s, %(email_validation)s, %(phone)s, %(linkedin_url)s,
            %(is_current)s, %(resolution_confidence)s, %(resolution_source)s,
            %(time_in_role_months)s, %(time_at_company_months)s, %(job_change_type)s, %(raw_source_file)s
        )
        """,
        person,
    )


def get_companies_by_status(conn, status: str, limit: int = None):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    query = "select * from companies where resolution_status = %s"
    params = [status]
    if limit:
        query += " limit %s"
        params.append(limit)
    cur.execute(query, params)
    return cur.fetchall()


def log_pipeline_run(conn, tier: str, processed: int, resolved: int, notes: str = "",
                      cost_usd: float = 0.0, api_calls_made: int = 0,
                      rate_limited_count: int = 0, failed_count: int = 0, status: str = "ok"):
    cur = conn.cursor()
    cur.execute(
        """
        insert into pipeline_runs (
            tier, finished_at, companies_processed, companies_resolved, notes,
            cost_usd, api_calls_made, rate_limited_count, failed_count, status
        )
        values (%s, now(), %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (tier, processed, resolved, notes, cost_usd, api_calls_made,
         rate_limited_count, failed_count, status),
    )


def log_tier3_search(conn, company_name: str, snippets: str, status: str):
    """
    Checkpoint a single Tier 3 search result immediately after it completes,
    so a crash mid-batch doesn't lose (or force re-paying for) work already done.
    """
    cur = conn.cursor()
    cur.execute(
        """
        insert into tier3_search_log (company_name, snippets, status)
        values (%s, %s, %s)
        """,
        (company_name, snippets, status),
    )


def get_already_searched_companies(conn) -> dict:
    """
    Returns a dict of {company_name: snippets} for every company that already
    has a successful Tier 3 search logged, so a resumed run can skip them
    instead of paying Serper again for the same company.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        select distinct on (company_name) company_name, snippets
        from tier3_search_log
        where status = 'success'
        order by company_name, searched_at desc
        """
    )
    rows = cur.fetchall()
    return {row["company_name"]: row["snippets"] for row in rows}