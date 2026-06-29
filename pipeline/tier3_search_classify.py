"""
tier3_search_classify.py — final residual layer. For companies that Tier 0
and Tier 1 couldn't resolve, run a real web search and hand the results to
Claude Haiku with a strict, evidence-required classification prompt.

Search calls run concurrently (async) using ONE shared Postgres connection
protected by an asyncio.Lock, rather than opening a new connection per
worker per write, to protect Supabase's connection pooler at scale. Each
result is checkpointed to Supabase (tier3_search_log) the moment it
succeeds, so a crash or restart mid-batch does not lose already-paid-for
search results, and a resumed run will skip any company already searched
successfully.

Quota exhaustion detection: Serper does not reliably use 402/403 for an
exhausted account, it has been observed returning a plain 400 Bad Request
instead. So any 4xx response body is inspected for credit/quota wording
before deciding whether it's a real bad request or a disguised quota error.
Once quota exhaustion is confirmed, a shared stop signal halts all other
in-flight concurrent workers immediately.

Cost tracking: cost_usd is an ESTIMATE based on Serper's $50-per-50,000-
credit pricing tier ($0.001/call), not a real-time billing figure from
Serper's API, which does not return per-call cost in its response.

After the batch is submitted, this module polls the Anthropic Batch API
for up to BATCH_POLL_TIMEOUT_SECONDS. If it finishes within that window,
results are automatically applied back into Supabase via upsert_company.
If not, the batch ID is logged clearly for manual follow-up later, since
Batch API jobs can legitimately take hours and blocking the whole pipeline
run indefinitely isn't realistic.
"""

import json
import time
import asyncio
import httpx
import anthropic
from pipeline.config import ANTHROPIC_API_KEY, CLAUDE_MODEL, SERPER_API_KEY, TIER3_BATCH_SIZE
from pipeline.db import get_conn, log_tier3_search, get_already_searched_companies, upsert_company

SYSTEM_PROMPT = """You are a careful data-classification assistant for a B2B lead database.
You will be given a company name and a set of web search result snippets about that company.

Your job: determine the company_type and whether contacts under this company name in our
database are usable sales leads, based ONLY on the evidence in the snippets provided.

STRICT RULES, do not deviate:

1. NEVER classify as "data_garbage" unless the snippets contain a concrete, checkable
   contradiction (e.g. the company is clearly a single small business but our database
   lists an implausible number of "C-level" contacts; or the company name doesn't match
   any real business found in search results at all).

2. If the evidence is incomplete, ambiguous, or simply insufficient to decide confidently,
   you MUST return company_type = "needs_manual_review" with confidence below 0.5.
   Insufficient evidence is NOT the same as data_garbage. Defaulting to data_garbage when
   you are merely uncertain is a serious error, do not do this.

3. Valid company_type values:
   - standalone_business
   - multi_partner_firm
   - franchise_unit
   - franchise_parent
   - membership_or_chapter_org
   - nonprofit_ngo
   - acquired_inactive
   - data_garbage
   - needs_manual_review

4. If snippets reveal the company was acquired, set company_type = "acquired_inactive"
   and include the acquirer's name in "evidence" if mentioned.

5. Every response must include specific evidence quoted/paraphrased from the snippets,
   never assert a fact you cannot point to in the provided text.

Respond ONLY with valid JSON, no preamble, matching this exact schema:
{
  "company_type": "...",
  "usable_lead": true | false | null,
  "confidence": 0.0-1.0,
  "evidence": "specific reasoning citing what was found in the snippets",
  "acquirer_name": "..." | null
}
"""

USER_TEMPLATE = """Company name: {company_name}
Our database lists {n_contacts} contact(s) with C-level/leadership titles for this company.

Web search result snippets:
{snippets}

Classify this company per the rules above."""

SEARCH_CONCURRENCY = 8
MAX_RETRIES = 4
SERPER_COST_PER_CALL_ESTIMATE = 0.001  # based on $50 / 50,000 credits
BATCH_POLL_TIMEOUT_SECONDS = 600  # 10 minutes
BATCH_POLL_INTERVAL_SECONDS = 20

QUOTA_KEYWORDS = ("credit", "quota", "insufficient", "balance", "exceeded", "out of")


class SearchQuotaExceeded(Exception):
    """Raised when Serper confirms (via response body wording, not just
    status code) that the account is out of credits, signals the caller
    to stop immediately rather than keep burning calls against a dead key."""
    pass


def is_junk_name(name: str) -> bool:
    if not name or not isinstance(name, str):
        return True
    cleaned = name.strip()
    if len(cleaned) < 2:
        return True
    return False


def looks_like_quota_error(status_code: int, body_text: str) -> bool:
    if status_code not in (400, 401, 402, 403):
        return False
    lowered = (body_text or "").lower()
    return any(keyword in lowered for keyword in QUOTA_KEYWORDS)


async def search_company_async(client: httpx.AsyncClient, company_name: str,
                                 stop_event: asyncio.Event, call_counter: dict,
                                 n_results: int = 5, max_retries: int = MAX_RETRIES) -> tuple[str, str]:
    """
    Runs a Serper.dev Google search and returns (status, snippet_text).
    status is "success", "failed", or "skipped_quota".
    call_counter is a shared dict that gets incremented for every actual
    HTTP call made, regardless of outcome, so real call counts are tracked.
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
                json={"q": f"{company_name} CEO owner about", "num": n_results},
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
                snippets.append(f"- {title}: {desc}")
            text = "\n".join(snippets) if snippets else "(no search results found)"
            return "success", text

        except SearchQuotaExceeded:
            raise
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            last_error = f"connection error: {str(e)}"
            await asyncio.sleep(2 ** attempt)
            continue

    return "failed", f"(search failed after {max_retries} attempts: {last_error})"


async def run_searches_with_checkpointing(companies: list[dict]) -> dict:
    """
    Searches all companies concurrently using ONE shared Postgres connection
    protected by an asyncio.Lock, rather than one connection per worker per
    write. Returns a dict with 'results' (company name -> snippets) and
    'stats' (real counts for success/failed/skipped_quota/total_calls/cost_usd).
    """
    shared_conn_ctx = get_conn()
    shared_conn = shared_conn_ctx.__enter__()
    conn_lock = asyncio.Lock()

    try:
        already_done = get_already_searched_companies(shared_conn)
    finally:
        pass  # connection stays open for the duration of this function

    print(f"Found {len(already_done)} companies already searched successfully in a prior run, skipping those.")

    seen = set()
    to_search = []
    for company in companies:
        name = company["name_cleaned"]
        if is_junk_name(name):
            continue
        if name in seen:
            continue
        seen.add(name)
        if name in already_done:
            continue
        to_search.append(company)

    print(f"{len(to_search)} companies need a fresh search "
          f"(after removing junk names, duplicates, and already-searched companies).")

    results = dict(already_done)
    semaphore = asyncio.Semaphore(SEARCH_CONCURRENCY)
    stop_event = asyncio.Event()
    counters = {"success": 0, "failed": 0, "skipped_quota": 0}
    call_counter = {"count": 0}
    quota_error_holder = {"error": None}

    async def worker(client, company):
        name = company["name_cleaned"]
        async with semaphore:
            try:
                status, snippets = await search_company_async(client, name, stop_event, call_counter)
            except SearchQuotaExceeded as e:
                quota_error_holder["error"] = e
                status, snippets = "skipped_quota", str(e)

        async with conn_lock:
            log_tier3_search(shared_conn, name, snippets, status)
            shared_conn.commit()

        results[name] = snippets
        counters[status] += 1
        done = sum(counters.values())
        if done % 100 == 0:
            print(f"  Tier 3 search progress: {done}/{len(to_search)} "
                  f"({counters['success']} ok, {counters['failed']} failed, "
                  f"{counters['skipped_quota']} skipped_quota, "
                  f"{call_counter['count']} real calls made)")

    try:
        async with httpx.AsyncClient() as client:
            tasks = [worker(client, c) for c in to_search]
            await asyncio.gather(*tasks)
    finally:
        shared_conn_ctx.__exit__(None, None, None)

    cost_usd = round(call_counter["count"] * SERPER_COST_PER_CALL_ESTIMATE, 4)
    print(f"Tier 3 search complete: {counters['success']} succeeded, "
          f"{counters['failed']} failed, {counters['skipped_quota']} skipped due to quota. "
          f"{call_counter['count']} real Serper calls made (~${cost_usd} estimated).")

    stats = {
        "success_count": counters["success"],
        "failed_count": counters["failed"],
        "skipped_quota_count": counters["skipped_quota"],
        "api_calls_made": call_counter["count"],
        "cost_usd": cost_usd,
    }

    if quota_error_holder["error"] is not None:
        quota_error_holder["error"].stats = stats
        raise quota_error_holder["error"]

    return {"results": results, "stats": stats}


def build_batch_requests(companies: list[dict], search_results: dict) -> list[dict]:
    requests = []
    seen = set()
    for i, company in enumerate(companies):
        name = company["name_cleaned"]
        if is_junk_name(name) or name in seen:
            continue
        seen.add(name)
        snippets = search_results.get(name, "(no search results found)")
        user_msg = USER_TEMPLATE.format(
            company_name=name,
            n_contacts=company.get("n_contacts", "unknown"),
            snippets=snippets,
        )
        requests.append({
            "custom_id": f"company-{i}-{name[:40]}",
            "params": {
                "model": CLAUDE_MODEL,
                "max_tokens": 400,
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": user_msg}],
            },
        })
    return requests


def apply_classification_to_supabase(custom_id_to_company: dict, classification: dict):
    """
    Writes one classified company's result into Supabase via upsert_company.
    Raw Tier 3 findings are written as-is, not blended into a single
    subjective judgment, per the project's standing requirement.
    """
    name = custom_id_to_company.get(classification["custom_id"])
    if not name:
        return

    company_record = {
        "name": name,
        "name_cleaned": name,
        "domain": None, "website": None, "industry": None,
        "revenue_range": None, "staff_bucket": None,
        "city": None, "state": None, "country": None, "description": None,
        "company_type": classification.get("company_type", "needs_manual_review"),
        "usable_lead": classification.get("usable_lead"),
        "usable_lead_reason": classification.get("evidence"),
        "classification_confidence": classification.get("confidence"),
        "classification_source": "tier3_search_classify",
        "classification_evidence": classification.get("evidence"),
        "resolution_status": "auto_resolved" if classification.get("company_type") not in
                              (None, "needs_manual_review", "data_garbage") else "needs_review",
    }
    with get_conn() as conn:
        upsert_company(conn, company_record)


def poll_and_apply_batch_results(batch_id: str, custom_id_to_company: dict) -> dict:
    """
    Polls the Anthropic Batch API for up to BATCH_POLL_TIMEOUT_SECONDS.
    If the batch finishes within that window, applies every classification
    back into Supabase and returns {"completed": True, "applied_count": N}.
    If it times out first, returns {"completed": False, "batch_id": batch_id}
    so the caller can log it for manual follow-up rather than blocking forever.
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    elapsed = 0

    while elapsed < BATCH_POLL_TIMEOUT_SECONDS:
        batch = client.messages.batches.retrieve(batch_id)
        if batch.processing_status == "ended":
            applied = 0
            for result in client.messages.batches.results(batch_id):
                custom_id = result.custom_id
                if result.result.type == "succeeded":
                    text = result.result.message.content[0].text
                    try:
                        parsed = json.loads(text)
                    except json.JSONDecodeError:
                        parsed = {"company_type": "needs_manual_review", "usable_lead": None,
                                  "confidence": 0.0,
                                  "evidence": f"failed_to_parse_model_output: {text[:200]}"}
                else:
                    parsed = {"company_type": "needs_manual_review", "usable_lead": None,
                              "confidence": 0.0,
                              "evidence": f"batch_request_failed: {result.result.type}"}

                parsed["custom_id"] = custom_id
                apply_classification_to_supabase(custom_id_to_company, parsed)
                applied += 1

            print(f"Batch {batch_id} completed. Applied {applied} classifications to Supabase.")
            return {"completed": True, "applied_count": applied}

        time.sleep(BATCH_POLL_INTERVAL_SECONDS)
        elapsed += BATCH_POLL_INTERVAL_SECONDS

    print(f"Batch {batch_id} did not complete within {BATCH_POLL_TIMEOUT_SECONDS}s. "
          f"It is still processing on Anthropic's side. Check back later with: "
          f"client.messages.batches.retrieve('{batch_id}')")
    return {"completed": False, "batch_id": batch_id}


def submit_batch(companies: list[dict]) -> dict:
    """
    Runs all Tier 3 searches concurrently with checkpointing, submits a
    batch job to the Anthropic Batch API, then attempts to poll and apply
    results within BATCH_POLL_TIMEOUT_SECONDS. Returns a dict with the
    batch_id, real search stats, and whether results were applied.
    """
    search_outcome = asyncio.run(run_searches_with_checkpointing(companies))
    search_results = search_outcome["results"]
    stats = search_outcome["stats"]

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    requests = build_batch_requests(companies, search_results)
    custom_id_to_company = {r["custom_id"]: r["custom_id"].split("-", 2)[-1] for r in requests}

    batch = client.messages.batches.create(requests=requests)
    print(f"Submitted batch {batch.id} with {len(requests)} requests. Status: {batch.processing_status}")

    batch_outcome = poll_and_apply_batch_results(batch.id, custom_id_to_company)

    return {
        "batch_id": batch.id,
        "stats": stats,
        "batch_completed": batch_outcome["completed"],
        "batch_applied_count": batch_outcome.get("applied_count", 0),
    }


if __name__ == "__main__":
    import sys
    import pandas as pd

    tier1_path = sys.argv[1] if len(sys.argv) > 1 else "tier1_output.csv"
    df = pd.read_csv(tier1_path)
    residual = df[df["resolution"].isin(["unresolved_tier1", "ambiguous_tier1"])]

    print(f"Submitting {len(residual)} companies to Tier 3 (search + Claude Haiku batch)...")
    companies = residual.to_dict("records")

    outcome = submit_batch(companies)
    print(f"Batch submitted: {outcome['batch_id']}")
    print(f"Search stats: {outcome['stats']}")
    if outcome["batch_completed"]:
        print(f"Applied {outcome['batch_applied_count']} classifications to Supabase already.")
    else:
        print("Batch still processing, check back later.")