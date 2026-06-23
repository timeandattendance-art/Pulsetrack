-- PulseTrack Enrichment Engine — Database Schema
-- Run this in Supabase SQL Editor (Project > SQL Editor > New Query)

-- =========================================================
-- COMPANIES
-- =========================================================
create table if not exists companies (
    id uuid primary key default gen_random_uuid(),
    name text not null,
    name_cleaned text,                    -- normalized name used for grouping/dedup
    domain text,
    website text,
    industry text,
    revenue_range text,
    staff_bucket integer,                 -- bucketed headcount ceiling from source data (30/125/350/10001)
    city text,
    state text,
    country text,
    description text,

    -- classification fields
    company_type text default 'unresolved',
        -- standalone_business | multi_partner_firm | franchise_unit | franchise_parent |
        -- membership_or_chapter_org | nonprofit_ngo | data_garbage | acquired_inactive | unresolved
    usable_lead boolean,                  -- null = undecided, true/false = decided
    usable_lead_reason text,
    classification_confidence numeric(3,2) default 0.0,
    classification_source text,           -- title_keyword | tier0_signal | tier1_website | tier3_search | manual
    classification_evidence text,         -- free text: what was found and where

    -- acquisition / parent tracking
    parent_company_id uuid references companies(id),
    acquired_by text,

    resolution_status text default 'needs_review',
        -- auto_resolved | needs_review | manually_reviewed
    last_verified_at timestamptz,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);

create index if not exists idx_companies_name_cleaned on companies(name_cleaned);
create index if not exists idx_companies_domain on companies(domain);
create index if not exists idx_companies_company_type on companies(company_type);
create index if not exists idx_companies_resolution_status on companies(resolution_status);

-- =========================================================
-- PEOPLE
-- =========================================================
create table if not exists people (
    id uuid primary key default gen_random_uuid(),
    company_id uuid references companies(id),

    full_name text not null,
    first_name text,
    last_name text,
    title text,
    title_normalized text,                -- e.g. "CEO", "Owner", "Partner" - cleaned
    seniority text,

    email text,
    email_validation text,                -- valid | accept_all | unknown
    phone text,
    linkedin_url text,

    -- currency / resolution
    is_current boolean default true,      -- false if superseded
    superseded_by_person_id uuid references people(id),
    resolution_confidence numeric(3,2) default 0.0,
    resolution_source text,               -- job_change_signal | tier1_website | tier3_search | manual

    time_in_role_months integer,
    time_at_company_months integer,
    job_change_type text,                 -- New Hire | New Promotion | null

    raw_source_file text,                 -- which original CSV this came from
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);

create index if not exists idx_people_company_id on people(company_id);
create index if not exists idx_people_email on people(email);
create index if not exists idx_people_is_current on people(is_current);

-- =========================================================
-- SNAPSHOTS (for future Engine-1-style monitoring)
-- =========================================================
create table if not exists snapshots (
    id uuid primary key default gen_random_uuid(),
    entity_type text not null,            -- 'person' | 'company'
    entity_id uuid not null,
    raw_data jsonb,
    summary text,
    captured_at timestamptz default now()
);

create index if not exists idx_snapshots_entity on snapshots(entity_type, entity_id);

-- =========================================================
-- SIGNALS (for future monitoring layer)
-- =========================================================
create table if not exists signals (
    id uuid primary key default gen_random_uuid(),
    entity_type text not null,            -- 'person' | 'company'
    entity_id uuid not null,
    signal_type text,                     -- job_change, acquisition, funding_round, post, etc.
    description text,
    detected_at timestamptz default now(),
    source_snapshot_id uuid references snapshots(id),
    previous_snapshot_id uuid references snapshots(id)
);

-- =========================================================
-- OFFERS (what you're selling — used by future matching layer)
-- =========================================================
create table if not exists offers (
    id uuid primary key default gen_random_uuid(),
    name text not null,
    description text,
    pain_points text[],
    target_titles text[],
    tone_notes text,
    active boolean default true,
    created_at timestamptz default now()
);

-- =========================================================
-- MATCH ALERTS (future relevance-matching layer)
-- =========================================================
create table if not exists match_alerts (
    id uuid primary key default gen_random_uuid(),
    person_id uuid references people(id),
    signal_id uuid references signals(id),
    offer_id uuid references offers(id),
    match_confidence numeric(3,2),
    matched_pain_point text,
    reasoning text,
    suggested_angle text,
    suggested_lines jsonb,
    status text default 'new',            -- new | reviewed | used | dismissed
    created_at timestamptz default now()
);

-- =========================================================
-- PIPELINE RUN LOG (lets you track each batch run)
-- =========================================================
create table if not exists pipeline_runs (
    id uuid primary key default gen_random_uuid(),
    tier text not null,                   -- tier0 | tier1 | tier3
    started_at timestamptz default now(),
    finished_at timestamptz,
    companies_processed integer default 0,
    companies_resolved integer default 0,
    notes text
);
