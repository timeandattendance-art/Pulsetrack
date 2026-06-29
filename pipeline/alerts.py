"""
alerts.py — sends run-completion and hard-failure emails via Resend's
HTTPS API, not SMTP. Railway blocks outbound SMTP (port 587), which is
why the original smtplib-based version silently failed with
"Network is unreachable" — switching to an HTTPS API call avoids that
restriction entirely.

Required env vars:
    RESEND_API_KEY      - your Resend API key (re_xxxxx...)
    ALERT_EMAIL_FROM     - sending address (onboarding@resend.dev is fine
                          for Resend's free tier, no domain verification needed)
    ALERT_EMAIL_TO       - destination address for alerts
"""

import os
import httpx

RESEND_API_URL = "https://api.resend.com/emails"


def _send(subject: str, body: str) -> None:
    api_key = os.environ.get("RESEND_API_KEY")
    sender = os.environ.get("ALERT_EMAIL_FROM")
    recipient = os.environ.get("ALERT_EMAIL_TO")

    if not all([api_key, sender, recipient]):
        print("[alerts] Email env vars not fully set — skipping alert, printing instead:")
        print(f"  SUBJECT: {subject}\n  BODY:\n{body}")
        return

    try:
        resp = httpx.post(
            RESEND_API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": sender,
                "to": [recipient],
                "subject": subject,
                "text": body,
            },
            timeout=15,
        )
        if resp.status_code in (200, 201):
            print(f"[alerts] Sent: {subject}")
        else:
            print(f"[alerts] FAILED to send email (HTTP {resp.status_code}: {resp.text[:300]}) — continuing without alert.")
    except Exception as e:
        # Never let an alerting failure crash the pipeline itself.
        print(f"[alerts] FAILED to send email ({e}) — continuing without alert.")


def send_run_completed(run_summary: dict) -> None:
    subject = f"PulseTrack run completed — {run_summary.get('tier', '?')}"
    body = (
        f"Companies processed: {run_summary.get('companies_processed', 0)}\n"
        f"Companies resolved:  {run_summary.get('companies_resolved', 0)}\n"
        f"Needs manual review: {run_summary.get('needs_review_count', 0)}\n"
        f"Failed:               {run_summary.get('failed_count', 0)}\n"
        f"Rate-limited (retried): {run_summary.get('rate_limited_count', 0)}\n"
        f"API calls made:       {run_summary.get('api_calls_made', 0)}\n"
        f"Total cost:           ${run_summary.get('cost_usd', 0):.2f}\n"
        f"People inserted:      {run_summary.get('people_inserted', 0)}\n"
        f"Companies written to Supabase: {run_summary.get('companies_written_to_supabase', 0)}\n"
    )
    _send(subject, body)


def send_run_failed(tier: str, error: str) -> None:
    subject = f"PulseTrack run FAILED — {tier}"
    body = f"The pipeline hard-failed during {tier}.\n\nError:\n{error}"
    _send(subject, body)


def send_budget_exceeded(provider: str, detail: str) -> None:
    subject = f"PulseTrack STOPPED — {provider} budget/quota exhausted"
    body = (
        f"The pipeline stopped itself rather than keep spending against "
        f"{provider} once it hit a quota/rate-limit error it couldn't recover from.\n\n"
        f"Detail:\n{detail}\n\n"
        f"No further {provider} calls were made after this point. "
        f"Top this up and re-run if you want the remaining companies processed."
    )
    _send(subject, body)


def send_rate_limit_warning(provider: str, company_key: str) -> None:
    subject = f"PulseTrack rate limit hit — {provider}"
    body = (
        f"Rate limit reached on {provider} while processing '{company_key}'.\n"
        f"The pipeline will automatically retry with backoff."
    )
    _send(subject, body)