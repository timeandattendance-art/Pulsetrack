"""
alerts.py — sends run-completion and hard-failure emails via Gmail SMTP,
using an app password (not the real account password).

Required env vars:
    GMAIL_APP_PASSWORD  - the 16-character app password (no spaces)
    ALERT_EMAIL_FROM    - sending address (must be the Gmail account the
                          app password belongs to)
    ALERT_EMAIL_TO      - destination address for alerts
"""

import os
import smtplib
from email.mime.text import MIMEText

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587


def _send(subject: str, body: str) -> None:
    password = os.environ.get("GMAIL_APP_PASSWORD")
    sender = os.environ.get("ALERT_EMAIL_FROM")
    recipient = os.environ.get("ALERT_EMAIL_TO")

    if not all([password, sender, recipient]):
        print("[alerts] Email env vars not fully set — skipping alert, printing instead:")
        print(f"  SUBJECT: {subject}\n  BODY:\n{body}")
        return

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, [recipient], msg.as_string())
        print(f"[alerts] Sent: {subject}")
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
