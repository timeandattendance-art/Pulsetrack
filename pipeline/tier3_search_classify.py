"""
tier3_search_classify.py — final residual layer. For companies that Tier 0
and Tier 1 couldn't resolve, run a real web search and hand the results to
Claude Haiku with a strict, evidence-required classification prompt.

Uses the Anthropic Batch API (50% cheaper, async) since none of this needs
to be real-time — it's a queue of leftover ambiguous companies.

IMPORTANT: the classifier is instructed to default to needs_manual_review
whenever evidence is thin, and is explicitly forbidden from flagging
data_garbage without a concrete contradiction. This mirrors the rule we
proved out by hand on the Union Yoga case earlier.
"""

import json
import time
import httpx
import anthropic
from pipeline.config import ANTHROPIC_API_KEY, CLAUDE_MODEL, BRAVE_API_KEY, TIER3_BATCH_SIZE

SYSTEM_PROMPT = """You are a careful data-classification assistant for a B2B lead database.
You will be given a company name and a set of web search result snippets about that company.

Your job: determine the company_type and whether contacts under this company name in our
database are usable sales leads, based ONLY on the evidence in the snippets provided.

STRICT RULES — do not deviate:

1. NEVER classify as "data_garbage" unless the snippets contain a concrete, checkable
   contradiction (e.g. the company is clearly a single small business but our database
   lists an implausible number of "C-level" contacts; or the company name doesn't match
   any real business found in search results at all).

2. If the evidence is incomplete, ambiguous, or simply insufficient to decide confidently,
   you MUST return company_type = "needs_manual_review" with confidence below 0.5.
   Insufficient evidence is NOT the same as data_garbage. Defaulting to data_garbage when
   you are merely uncertain is a serious error — do not do this.

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

5. Every response must include specific evidence quoted/paraphrased from the snippets —
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


class BraveQuotaExceeded(Exception):
    """Raised when Brave Search returns a rate-limit/quota error that
    persists after retries — signals the caller to stop immediately
    rather than keep burning calls against an exhausted/blocked key."""
    pass


def search_company(company_name: str, n_results: int = 5, max_retries: int = 3) -> str:
    """
    Runs a Brave Search API query and returns formatted snippet text.
    Swap this function out if using a different search provider.

    Transient rate limits (HTTP 429) are retried with backoff, since
    Brave's free/low tiers often rate-limit per-second rather than being
    truly out of monthly quota. If it's still failing after retries, or
    the response indicates the quota itself is exhausted (402), this
    raises BraveQuotaExceeded so the run stops cleanly instead of
    silently burning through (and potentially still being billed for)
    further failed calls.
    """
    if not BRAVE_API_KEY:
        raise RuntimeError("BRAVE_API_KEY not set — required for Tier 3 search step")

    last_error = None
    for attempt in range(max_retries):
        try:
            resp = httpx.get(
                "https://api.search.brave.com/res/v1/web/search",
                headers={"X-Subscription-Token": BRAVE_API_KEY, "Accept": "application/json"},
                params={"q": f"{company_name} CEO owner about", "count": n_results},
                timeout=15,
            )
            if resp.status_code == 402:
                raise BraveQuotaExceeded(
                    f"Brave Search returned 402 (payment/quota exhausted) for '{company_name}'. "
                    f"Stopping Tier 3 — no further search calls will be made this run."
                )
            if resp.status_code == 429:
                last_error = f"429 rate-limited on attempt {attempt + 1}/{max_retries}"
                time.sleep(2 ** attempt)  # 1s, 2s, 4s backoff
                continue
            resp.raise_for_status()
            data = resp.json()
            results = data.get("web", {}).get("results", [])
            snippets = []
            for r in results[:n_results]:
                title = r.get("title", "")
                desc = r.get("description", "")
                snippets.append(f"- {title}: {desc}")
            return "\n".join(snippets) if snippets else "(no search results found)"
        except BraveQuotaExceeded:
            raise
        except httpx.HTTPStatusError as e:
            last_error = str(e)
            break

    raise BraveQuotaExceeded(
        f"Brave Search failed for '{company_name}' after {max_retries} attempts "
        f"({last_error}). Stopping Tier 3 rather than continuing to burn calls "
        f"against a key that may be rate-limited or out of quota."
    )


def build_batch_requests(companies: list[dict]) -> list[dict]:
    """
    companies: list of dicts with 'name_cleaned' and 'n_contacts'
    Returns a list of Anthropic Message Batches API request objects.
    """
    requests = []
    for i, company in enumerate(companies):
        snippets = search_company(company["name_cleaned"])
        user_msg = USER_TEMPLATE.format(
            company_name=company["name_cleaned"],
            n_contacts=company.get("n_contacts", "unknown"),
            snippets=snippets,
        )
        requests.append({
            "custom_id": f"company-{i}-{company['name_cleaned'][:40]}",
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
    Submits a batch job to the Anthropic Batch API. Returns the batch id.
    Batch jobs are ~50% cheaper than live calls and process within 24h
    (often much faster) — appropriate since this is non-urgent cleanup work.
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    requests = build_batch_requests(companies)

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
