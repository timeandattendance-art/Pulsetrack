"""
tier3_search_classify.py — final residual layer. For companies where Tier 0
found a genuine top-title conflict (2+ named people claiming the same role),
runs a real web search for "{Company Name} CEO" and asks Claude to identify
which named contact is the actual current leader, weighing recency so a
stepped-down former CEO doesn't override a more recent successor. Also
detects franchise and law-firm/membership-org structures.

Search calls run concurrently using ONE shared Postgres connection protected
by an asyncio.Lock. Each result is checkpointed to Supabase (tier3_search_log)
the moment it succeeds, so a crash or restart mid-batch does not lose
already-paid-for search results.

Quota exhaustion detection: Serper has been observed returning a plain 400
Bad Request for an exhausted account rather than 402/403, so any 4xx body
is inspected for credit/quota wording before deciding it's a real bad
request vs a disguised quota error. A shared stop signal halts all other
in-flight workers once quota exhaustion is confirmed.

Cost tracking: serper_cost_usd is an ESTIMATE ($0.001/call, based on the
$50-per-50,000-credit tier). claude_cost_usd is REAL, calculated from the
actual input/output token counts returned by every Claude API response
(Haiku 4.5 rates: $1/million input tokens, $5/million output tokens).

Output per conflicted company: a list of per-contact results, each with
ceo_tf ("true"/"false"), duplicate_flag ("duplicate" or ""), and
structure_flag (the detected company_type). For franchises, the same
search/resolve logic applies. For multi_partner_firm and
membership_or_chapter_org, NO search or auto-resolution is attempted —
those get structure_flag set and ceo_tf/duplicate_flag left blank,
per explicit instruction to leave them for manual review.
"""

import json
import asyncio
import httpx
import anthropic
from pipeline.config import ANTHROPIC_API_KEY, CLAUDE_MODEL, SERPER_API_KEY, TIER3_BATCH_SIZE
from pipeline.db import get_conn, log_tier3_search, get_already_searched_companies

SYSTEM_PROMPT = """You are a careful data-classification assistant for a B2B lead database.

You will be given a company name, a list of named contacts who each claim the
same top-level title (e.g. CEO, President, Owner, Founder) at that company,
and a set of web search result snippets about that company.

Your job, in order:

1. Determine if this is a FRANCHISE location, a LAW FIRM or other
   multi-partner professional firm, or a MEMBERSHIP/CHAPTER organization
   (e.g. a local chapter of a national nonprofit, club, or association).
   If it is a law firm or membership/chapter org, STOP THERE — do not
   attempt to identify a CEO. Set company_type accordingly and leave
   confirmed_name as null.

2. If it is a standalone business or franchise location, identify which
   ONE of the named contacts is the actual CURRENT leader (CEO, President,
   Owner, or Founder), based ONLY on evidence in the snippets.

   CRITICAL: weigh recency carefully. If the snippets show a leadership
   transition (e.g. one source says Person A was CEO, a more recent source
   says Person A stepped down or Person B is now CEO), identify the CURRENT
   leader as of the most recent evidence, not whoever appears first or most
   often. If sources conflict and you cannot tell which is more recent or
   reliable, set confidence below 0.5 and explain the conflict in evidence.

3. If the evidence is insufficient to confidently identify which named
   contact is current, you MUST set confirmed_name to null and confidence
   below 0.5, rather than guessing. Do not default to data_garbage merely
   for thin evidence — only use data_garbage if there is a concrete
   contradiction (e.g. the company itself doesn't appear to exist).

Respond ONLY with valid JSON, no preamble, matching this exact schema:
{
  "company_type": "standalone_business" | "franchise_unit" | "franchise_parent" |
                   "multi_partner_firm" | "membership_or_chapter_org" |
                   "nonprofit_ngo" | "acquired_inactive" | "data_garbage" |
                   "needs_manual_review",
  "confirmed_name": "exact name as given in the candidate list" | null,
  "confidence": 0.0-1.0,
  "evidence": "specific reasoning citing what was found in the snippets, including any recency/transition reasoning",
  "acquirer_name": "..." | null
}
"""

USER_TEMPLATE = """Company name: {company_name}

Named contacts claiming this same top title at this company:
{candidate_list}

Web search result snippets:
{snippets}

Identify the current leader per the rules above."""

SEARCH_CONCURRENCY = 8
MAX_RETRIES = 4
SERPER_COST_PER_CALL_ESTIMATE = 0.001  # based on $50 / 50,000 credits

# Real Claude Haiku 4.5 rates, per token (not per million) — used to
# calculate exact cost from response.usage on every call.
CLAUDE_INPUT_COST_PER_TOKEN = 1.00 / 1_000_000
CLAUDE_OUTPUT_COST_PER_TOKEN = 5.00 / 1_000_000

QUOTA_KEYWORDS = ("credit", "quota", "insufficient", "balance", "exceeded", "out of")

# These structure types get NO search, NO auto-resolution — flagged and left alone.
NO_RESOLVE_TYPES = {"multi_partner_firm", "membership_or_chapter_org"}


class SearchQuotaExceeded(Exception):
    """Raised when Serper confirms (via response body wording, not just
    status code) that the account is out of credits."""
    pass


def is_junk_name(name: str) -> bool:
    if not name or not isinstance(name, str):
        return True
    return len(name.strip()) < 2


def looks_like_quota_error(status_code: int, body_text: str) -> bool:
    if status_code not in (400, 401, 402, 403):
        return False
    lowered = (body_text or "").lower()
    return any(keyword in lowered for keyword in QUOTA_KEYWORDS)


async def search_company_async(client: httpx.AsyncClient, company_name: str,
                                 stop_event: asyncio.Event, call_counter: dict,
                                 n_results: int = 6, max_retries: int = MAX_RETRIES) -> tuple[str, str]:
    """
    Runs a Serper.dev search for "{company_name} CEO" and returns
    (status, snippet_text). status is "success", "failed", or "skipped_quota".
    """
    if stop_event.is_set():
        return "skipped_quota", "(skipped: quota already confirmed exhausted by another worker)"

    last_error = None
    for attempt in range(max_retries):
        if stop_event.is_set():
            return "skipped_quota", "(skipped mid-retry: quota confirmed exhausted by another worker)"

        try:
            call_counter["count"] += 1
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": f"{company_name} CEO", "num": n_results},
                timeout=20,
            )

            if resp.status_code >= 400:
                body_text = resp.text[:500]
                if looks_like_quota_error(resp.status_code, body_text):
                    stop_event.set()
                    raise SearchQuotaExceeded(
                        f"Serper confirmed quota exhaustion for '{company_name}': "
                        f"HTTP {resp.status_code}, body: {body_text}"
                    )
                if resp.status_code == 429:
                    last_error = f"429 rate-limited on attempt {attempt + 1}/{max_retries}, body: {body_text}"
                    await asyncio.sleep(2 ** attempt)
                    continue
                last_error = f"HTTP {resp.status_code} (non-quota), body: {body_text}"
                await asyncio.sleep(2 ** attempt)
                continue

            resp.raise_for_status()
            data = resp.json()
            results = data.get("organic", [])
            snippets = []
            for r in results[:n_results]:
                title = r.get("title", "")
                desc = r.get("snippet", "")
                date = r.get("date", "")
                date_part = f" [{date}]" if date else ""
                snippets.append(f"- {title}{date_part}: {desc}")
            text = "\n".join(snippets) if snippets else "(no search results found)"
            return "success", text

        except SearchQuotaExceeded:
            raise
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            last_error = f"connection error: {str(e)}"
            await asyncio.sleep(2 ** attempt)
            continue

    return "failed", f"(search failed after {max_retries} attempts: {last_error})"


def classify_with_claude(client: anthropic.Anthropic, company_name: str,
                          candidate_names: list[str], snippets: str,
                          claude_stats: dict) -> dict:
    """
    Single synchronous Claude call. Tracks REAL token usage and cost from
    response.usage into claude_stats (shared across all calls in this run).
    """
    candidate_list = "\n".join(f"- {name}" for name in candidate_names)
    user_msg = USER_TEMPLATE.format(
        company_name=company_name,
        candidate_list=candidate_list,
        snippets=snippets,
    )
    text = ""
    try:
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=400,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )

        # Real token counts, not an estimate — Anthropic returns exact
        # usage on every response.
        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens
        call_cost = (input_tokens * CLAUDE_INPUT_COST_PER_TOKEN +
                     output_tokens * CLAUDE_OUTPUT_COST_PER_TOKEN)

        claude_stats["calls_made"] += 1
        claude_stats["input_tokens"] += input_tokens
        claude_stats["output_tokens"] += output_tokens
        claude_stats["cost_usd"] += call_cost

        text = response.content[0].text
        return json.loads(text)
    except json.JSONDecodeError:
        return {"company_type": "needs_manual_review", "confirmed_name": None,
                 "confidence": 0.0, "evidence": f"failed_to_parse_model_output: {text[:200]}",
                 "acquirer_name": None}
    except Exception as e:
        return {"company_type": "needs_manual_review", "confirmed_name": None,
                 "confidence": 0.0, "evidence": f"claude_call_failed: {str(e)}",
                 "acquirer_name": None}


def build_per_contact_flags(candidate_names: list[str], classification: dict) -> dict:
    """
    Given the company-level classification, returns {contact_name: {ceo_tf,
    duplicate_flag, structure_flag}} for every candidate at this company.
    """
    structure = classification.get("company_type", "needs_manual_review")
    confirmed_name = classification.get("confirmed_name")

    flags = {}
    if structure in NO_RESOLVE_TYPES:
        for name in candidate_names:
            flags[name] = {"ceo_tf": "", "duplicate_flag": "", "structure_flag": structure}
        return flags

    if not confirmed_name:
        for name in candidate_names:
            flags[name] = {"ceo_tf": "", "duplicate_flag": "", "structure_flag": structure}
        return flags

    for name in candidate_names:
        if name == confirmed_name:
            flags[name] = {"ceo_tf": "true", "duplicate_flag": "", "structure_flag": structure}
        else:
            flags[name] = {"ceo_tf": "false", "duplicate_flag": "duplicate", "structure_flag": structure}
    return flags


async def run_conflict_resolution(companies: list[dict]) -> dict:
    """
    For each conflicted company (each dict must have 'name_cleaned' and
    'candidate_names'), searches concurrently, checkpoints to Supabase,
    classifies via Claude, and returns per-contact flags plus real
    cost/token stats for both Serper and Claude.
    """
    shared_conn_ctx = get_conn()
    shared_conn = shared_conn_ctx.__enter__()
    conn_lock = asyncio.Lock()

    already_done = get_already_searched_companies(shared_conn)
    print(f"Found {len(already_done)} companies already searched successfully in a prior run, skipping search for those.")

    seen = set()
    to_search = []
    for company in companies:
        name = company["name_cleaned"]
        if is_junk_name(name) or name in seen:
            continue
        seen.add(name)
        to_search.append(company)

    snippets_by_company = dict(already_done)
    semaphore = asyncio.Semaphore(SEARCH_CONCURRENCY)
    stop_event = asyncio.Event()
    counters = {"success": 0, "failed": 0, "skipped_quota": 0}
    call_counter = {"count": 0}
    quota_error_holder = {"error": None}

    async def worker(client, company):
        name = company["name_cleaned"]
        if name in already_done:
            return
        async with semaphore:
            try:
                status, snippets = await search_company_async(client, name, stop_event, call_counter)
            except SearchQuotaExceeded as e:
                quota_error_holder["error"] = e
                status, snippets = "skipped_quota", str(e)

        async with conn_lock:
            log_tier3_search(shared_conn, name, snippets, status)
            shared_conn.commit()

        snippets_by_company[name] = snippets
        counters[status] += 1

    try:
        async with httpx.AsyncClient() as client:
            tasks = [worker(client, c) for c in to_search]
            await asyncio.gather(*tasks)
    finally:
        shared_conn_ctx.__exit__(None, None, None)

    serper_cost_usd = round(call_counter["count"] * SERPER_COST_PER_CALL_ESTIMATE, 4)
    print(f"Tier 3 search complete: {counters['success']} succeeded, "
          f"{counters['failed']} failed, {counters['skipped_quota']} skipped due to quota. "
          f"{call_counter['count']} real Serper calls made (~${serper_cost_usd} estimated).")

    stats = {
        "success_count": counters["success"],
        "failed_count": counters["failed"],
        "skipped_quota_count": counters["skipped_quota"],
        "api_calls_made": call_counter["count"],
        "serper_cost_usd": serper_cost_usd,
    }

    if quota_error_holder["error"] is not None:
        quota_error_holder["error"].stats = stats
        raise quota_error_holder["error"]

    # Now classify each company with Claude — real token cost tracked here.
    claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    claude_stats = {"calls_made": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0}
    per_contact_flags = {}

    for company in to_search:
        name = company["name_cleaned"]
        candidate_names = company.get("candidate_names", [])
        snippets = snippets_by_company.get(name, "(no search results found)")
        classification = classify_with_claude(claude_client, name, candidate_names, snippets, claude_stats)
        per_contact_flags[name] = build_per_contact_flags(candidate_names, classification)

    claude_stats["cost_usd"] = round(claude_stats["cost_usd"], 4)
    print(f"Tier 3 classification complete: {claude_stats['calls_made']} Claude calls, "
          f"{claude_stats['input_tokens']} input tokens, {claude_stats['output_tokens']} output tokens, "
          f"${claude_stats['cost_usd']} real cost.")

    stats["claude_calls_made"] = claude_stats["calls_made"]
    stats["claude_input_tokens"] = claude_stats["input_tokens"]
    stats["claude_output_tokens"] = claude_stats["output_tokens"]
    stats["claude_cost_usd"] = claude_stats["cost_usd"]
    stats["total_cost_usd"] = round(stats["serper_cost_usd"] + claude_stats["cost_usd"], 4)

    return {"per_contact_flags": per_contact_flags, "stats": stats}


def submit_batch(companies: list[dict]) -> dict:
    """
    companies: list of dicts, each with 'name_cleaned' and 'candidate_names'
    (the list of contact names claiming the conflicted top title).

    Returns {"per_contact_flags": {...}, "stats": {...}} where stats now
    includes both serper_cost_usd (estimate) and claude_cost_usd (real,
    token-based), plus total_cost_usd.
    """
    return asyncio.run(run_conflict_resolution(companies))


if __name__ == "__main__":
    import sys
    import pandas as pd

    tier1_path = sys.argv[1] if len(sys.argv) > 1 else "tier1_output.csv"
    df = pd.read_csv(tier1_path)
    residual = df[df["resolution"].isin(["unresolved_tier1", "ambiguous_tier1"])]

    print(f"Resolving {len(residual)} conflicted companies via Tier 3 search...")
    companies = residual.to_dict("records")

    outcome = submit_batch(companies)
    print(f"Stats: {outcome['stats']}")
    print(f"Resolved {len(outcome['per_contact_flags'])} companies.")