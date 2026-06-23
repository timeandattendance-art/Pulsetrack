"""
tier1_website_scrape.py — checks a company's own website for current
leadership info, to confirm/resolve contacts that Tier 0 couldn't.

Runs async (httpx) so hundreds of companies can be checked concurrently
rather than one at a time. This MUST run on infrastructure with open
internet egress (Railway, a VPS, your own machine) — it will NOT work
inside a sandboxed code-execution environment with a domain allowlist.

Strategy per company:
  1. Try a short list of likely page paths (/about, /team, /leadership, /company, /contact)
  2. Pull text content, lowercase it
  3. For each ambiguous contact's full name, check if it appears on any fetched page
  4. If exactly one contact's name is found -> resolved, confidence based on which page matched
  5. If zero or multiple names found -> leave unresolved, pass to Tier 3
"""

import asyncio
import httpx
from bs4 import BeautifulSoup
from pipeline.config import TIER1_CONCURRENCY, TIER1_TIMEOUT_SECONDS

CANDIDATE_PATHS = ["", "/about", "/about-us", "/team", "/our-team", "/leadership", "/company", "/contact"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PulseTrackBot/1.0; +contact@yourdomain.com)"
}


async def fetch_page_text(client: httpx.AsyncClient, url: str) -> str:
    try:
        resp = await client.get(url, headers=HEADERS, timeout=TIER1_TIMEOUT_SECONDS, follow_redirects=True)
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text, "lxml")
        # strip script/style noise
        for tag in soup(["script", "style", "nav", "footer"]):
            tag.decompose()
        return soup.get_text(separator=" ", strip=True).lower()
    except Exception:
        return ""


async def check_company(client: httpx.AsyncClient, domain: str, candidate_names: list[str]) -> dict:
    """
    Fetches a handful of candidate pages for one company and checks which
    (if any) of the candidate contact names appear on the site.
    """
    if not domain:
        return {"domain": domain, "resolution": "unresolved_tier1", "matched_name": None,
                "confidence": 0.0, "evidence": "no_domain_available"}

    base = domain if domain.startswith("http") else f"https://{domain}"
    combined_text = ""
    pages_fetched = 0

    for path in CANDIDATE_PATHS:
        text = await fetch_page_text(client, base.rstrip("/") + path)
        if text:
            combined_text += " " + text
            pages_fetched += 1
        if pages_fetched >= 3:  # don't hammer a single site, 3 pages is enough signal
            break

    if not combined_text:
        return {"domain": domain, "resolution": "unresolved_tier1", "matched_name": None,
                "confidence": 0.0, "evidence": "site_unreachable_or_no_content"}

    matches = [name for name in candidate_names if name and name.lower() in combined_text]

    if len(matches) == 1:
        return {"domain": domain, "resolution": "resolved_tier1", "matched_name": matches[0],
                "confidence": 0.7, "evidence": f"name_found_on_company_website:{matches[0]}"}
    elif len(matches) > 1:
        return {"domain": domain, "resolution": "ambiguous_tier1", "matched_name": None,
                "confidence": 0.0, "evidence": f"multiple_names_found_on_site:{','.join(matches)}"}
    else:
        return {"domain": domain, "resolution": "unresolved_tier1", "matched_name": None,
                "confidence": 0.0, "evidence": "no_candidate_names_found_on_site"}


async def run_tier1_batch(companies: list[dict]) -> list[dict]:
    """
    companies: list of dicts, each with 'domain' and 'candidate_names' (list of str)
    Returns the same list with resolution results merged in.
    """
    semaphore = asyncio.Semaphore(TIER1_CONCURRENCY)
    results = []

    async with httpx.AsyncClient() as client:
        async def bound_check(company):
            async with semaphore:
                res = await check_company(client, company["domain"], company["candidate_names"])
                return {**company, **res}

        tasks = [bound_check(c) for c in companies]
        for coro in asyncio.as_completed(tasks):
            results.append(await coro)

    return results


if __name__ == "__main__":
    import sys
    import pandas as pd

    tier0_path = sys.argv[1] if len(sys.argv) > 1 else "tier0_output.csv"
    raw_csv_path = sys.argv[2] if len(sys.argv) > 2 else "data/leads.csv"

    tier0_df = pd.read_csv(tier0_path)
    raw_df = pd.read_csv(raw_csv_path, dtype=str, low_memory=False)

    unresolved = tier0_df[tier0_df["resolution_status"] == "needs_review"]
    print(f"Running Tier 1 against {len(unresolved)} unresolved companies...")

    companies_to_check = []
    for _, row in unresolved.iterrows():
        group = raw_df[raw_df["Company Name - Cleaned"] == row["name_cleaned"]]
        domain = group["Website"].dropna().iloc[0] if not group["Website"].dropna().empty else None
        names = group["Contact Full Name"].dropna().unique().tolist()
        companies_to_check.append({
            "name_cleaned": row["name_cleaned"],
            "domain": domain,
            "candidate_names": names,
        })

    results = asyncio.run(run_tier1_batch(companies_to_check))
    out = pd.DataFrame(results)
    out.to_csv("tier1_output.csv", index=False)
    print(out["resolution"].value_counts())
    print("Written to tier1_output.csv")
