"""
tier3_search_classify.py — final residual layer. For companies that Tier 0
and Tier 1 couldn't resolve, run a real web search and hand the results to
Claude Haiku with a strict, evidence-required classification prompt.

Search calls run concurrently (async) and each result is checkpointed to
Supabase (tier3_search_log) the moment it succeeds. This means a crash or
restart mid-batch does not lose already-paid-for search results, and a
resumed run will skip any company already searched successfully.

Uses the Anthropic Batch API (50% cheaper, async) since none of this needs
to be real-time, it's a queue of leftover ambiguous companies.

IMPORTANT: the classifier is instructed to default to needs_manual_review
whenever evidence is thin, and is explicitly forbidden from flagging
data_garbage without a concrete contradiction.
"""

import json
import asyncio
import httpx
import anthropic
from pipeline.config import ANTHROPIC_API_KEY, CLAUDE_MODEL, SERPER_API_KEY, TIER3_BATCH_SIZE
from pipeline.db import get_conn, log_tier3_search, get_already_searched_companies

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


class SearchQuotaExceeded(Exception):
    """Raised when Serper returns a quota/payment error that persists
    after retries, signals the caller to stop immediately rather than
    keep burning calls against an exhausted/blocked key."""
    pass


def is_junk_name(name: str) -> bool:
    if not name or not isinstance(name, str):
        return True
    cleaned = name.strip()
    if len(cleaned) < 2:
        return True
    return False


async def search_company_async(client: httpx.AsyncClient, company_name: str,
                                 n_results: int = 5, max_retries: int = MAX_RETRIES) -> tuple[str, str]:
    """
    Runs a Serper.dev Google search and returns (status, snippet_text).
    status is "success" or "failed". Retries on timeouts, connection errors,
    and 429s with exponential backoff. Raises SearchQuotaExceeded on 402/403.
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": f"{company_name} CEO owner about", "num": n_results},
                timeout=20,
            )
            if resp.status_code in (402, 403):
                raise SearchQuotaExceeded(
                    f"Serper returned {resp.status_code} (payment/quota issue) for '{company_name}'."
                )
            if resp.status_code == 429:
                last_error = f"429 rate-limited on attempt {attempt + 1}/{max_retries}"
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
        except (httpx.TimeoutException, httpx.ConnectError, httpx.HTTPStatusError) as e:
            last_error = str(e)
            await asyncio.sleep(2 ** attempt)
            continue

    return "failed", f"(search failed after {max_retries} attempts: {last_error})"


async def run_searches_with_checkpointing(companies: list[dict]) -> dict:
    """
    Searches all companies concurrently, skipping any already searched
    successfully in a prior run, and checkpoints each result to Supabase
    immediately as it completes. Returns {company_name: snippets} for all
    companies, combining cached results with newly searched ones.
    """
    with get_conn() as conn:
        already_done = get_already_searched_companies(conn)

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
    counters = {"success": 0, "failed": 0}

    async def worker(client, company):
        name = company["name_cleaned"]
        async with semaphore:
            status, snippets = await search_company_async(client, name)
        with get_conn() as conn:
            log_tier3_search(conn, name, snippets, status)
        results[name] = snippets
        counters[status] += 1
        done = counters["success"] + counters["failed"]
        if done % 100 == 0:
            print(f"  Tier 3 search progress: {done}/{len(to_search)} "
                  f"({counters['success']} ok, {counters['failed']} failed)")

    async with httpx.AsyncClient() as client:
        tasks = [worker(client, c) for c in to_search]
        await asyncio.gather(*tasks)

    print(f"Tier 3 search complete: {counters['success']} succeeded, {counters['failed']} failed.")
    return results


def build_batch_requests(companies: list[dict], search_results: dict) -> list[dict]:
    """
    companies: list of dicts with 'name_cleaned' and 'n_contacts'
    search_results: {name_cleaned: snippets} already gathered
    Returns a list of Anthropic Message Batches API request objects.
    """
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


def submit_batch(companies: list[dict]):
    """
    Runs all Tier 3 searches concurrently with checkpointing, then submits
    a batch job to the Anthropic Batch API. Returns the batch id.
    """
    search_results = asyncio.run(run_searches_with_checkpointing(companies))

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    requests = build_batch_requests(companies, search_results)

    batch = client.messages.batches.create(requests=requests)
    print(f"Submitted batch {batch.id} with {len(requests)} requests. Status: {batch.processing_status}")
    return batch.id


def retrieve_batch_results(batch_id: str) -> list[dict]:
    """
    Polls/retrieves completed batch results. Call this after the batch
    has finished processing (check status via client.messages.batches.retrieve).
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    results = []

    for result in client.messages.batches.results(batch_id):
        custom_id = result.custom_id
        if result.result.type == "succeeded":
            text = result.result.message.content[0].text
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = {"company_type": "needs_manual_review", "usable_lead": None,
                          "confidence": 0.0, "evidence": f"failed_to_parse_model_output: {text[:200]}"}
        else:
            parsed = {"company_type": "needs_manual_review", "usable_lead": None,
                      "confidence": 0.0, "evidence": f"batch_request_failed: {result.result.type}"}

        results.append({"custom_id": custom_id, **parsed})

    return results


if __name__ == "__main__":
    import sys
    import pandas as pd

    tier1_path = sys.argv[1] if len(sys.argv) > 1 else "tier1_output.csv"
    df = pd.read_csv(tier1_path)
    residual = df[df["resolution"].isin(["unresolved_tier1", "ambiguous_tier1"])]

    print(f"Submitting {len(residual)} companies to Tier 3 (search + Claude Haiku batch)...")
    companies = residual.to_dict("records")

    batch_id = submit_batch(companies)
    print(f"Batch submitted: {batch_id}")
    print("Check status with: client.messages.batches.retrieve(batch_id)")
    print("Once status is 'ended', run retrieve_batch_results(batch_id) to get classifications.")