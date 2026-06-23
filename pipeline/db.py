"""
db.py — thin Postgres helper layer over the Supabase database.

Uses psycopg2 directly rather than an ORM, since the pipeline mostly does
batch upserts and simple lookups. Kept deliberately simple.
"""

import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from pipeline.config import SUPABASE_DB_URL


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


def log_pipeline_run(conn, tier: str, processed: int, resolved: int, notes: str = ""):
    cur = conn.cursor()
    cur.execute(
        """
        insert into pipeline_runs (tier, finished_at, companies_processed, companies_resolved, notes)
        values (%s, now(), %s, %s, %s)
        """,
        (tier, processed, resolved, notes),
    )
