"""Phase 2.18 production timing measurement.

Reads:
  MYCHAT_BACKEND_URL     (required) e.g. https://backend-production-...
  MYCHAT_AUTH_TOKEN      (optional) Bearer token for authenticated probes
                         (e.g. /auth/me, /dashboard/summary, /automations/summary)

Writes timings to stdout as a markdown-friendly table. Never logs the
token. Never stores credentials anywhere.

Usage:
  MYCHAT_BACKEND_URL=https://backend-production-a1a3.up.railway.app \\
  python backend/scripts/measure_production_timings.py

  # With token (will probe authenticated routes too):
  MYCHAT_BACKEND_URL=...  MYCHAT_AUTH_TOKEN=$(cat ~/.mychat_token) \\
  python backend/scripts/measure_production_timings.py
"""
from __future__ import annotations

import json
import os
import statistics
import sys
import time
import urllib.request
import urllib.error
from typing import Optional


def _redact_token(value: Optional[str]) -> str:
    if not value:
        return "<unset>"
    return f"<set, len={len(value)}>"


def _probe(
    url: str, token: Optional[str], *, label: str, warmups: int = 1, samples: int = 5
) -> dict:
    """Run `warmups + samples` requests; report timings on samples only.

    Token (if provided) is sent as Authorization: Bearer. Token value is
    never written to stdout/stderr — only its presence + length.
    """
    timings_ms = []
    statuses = []
    last_error = None
    for i in range(warmups + samples):
        req = urllib.request.Request(url)
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        req.add_header("User-Agent", "mychat-perf-probe/2.18")
        t0 = time.monotonic()
        status = 0
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:  # noqa: S310 (https URL)
                status = resp.status
                # Drain the body so the connection completes
                _ = resp.read()
        except urllib.error.HTTPError as e:
            status = e.code
            try:
                _ = e.read()
            except Exception:
                pass
        except Exception as e:  # network failure
            last_error = type(e).__name__
            status = 0
        dt_ms = int((time.monotonic() - t0) * 1000)
        if i >= warmups:
            timings_ms.append(dt_ms)
            statuses.append(status)
        # Small spacing so we don't hammer the host
        time.sleep(0.15)
    if not timings_ms:
        return {
            "label": label,
            "url": url,
            "samples": 0,
            "error": last_error or "no_samples",
        }
    timings_ms.sort()
    return {
        "label": label,
        "url": url,
        "samples": len(timings_ms),
        "statuses": statuses,
        "min_ms": timings_ms[0],
        "p50_ms": int(statistics.median(timings_ms)),
        "p95_ms": int(timings_ms[int(0.95 * (len(timings_ms) - 1))]),
        "max_ms": timings_ms[-1],
        "all_ms": timings_ms,
    }


def main() -> int:
    backend_url = (os.environ.get("MYCHAT_BACKEND_URL") or "").rstrip("/")
    token = os.environ.get("MYCHAT_AUTH_TOKEN") or ""
    if not backend_url:
        print("ERROR: MYCHAT_BACKEND_URL is required", file=sys.stderr)
        return 2

    print(f"# MyChat production timing probe (Phase 2.18)\n")
    print(f"- backend_url: {backend_url}")
    print(f"- auth_token: {_redact_token(token)}")
    print(f"- timestamp_utc: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
    print()

    probes = [
        # Cold/warm public health: 0 warmups so the first call counts as cold.
        {
            "url": f"{backend_url}/api/",
            "label": "health_first_call_cold",
            "warmups": 0,
            "samples": 1,
            "needs_token": False,
        },
        {
            "url": f"{backend_url}/api/",
            "label": "health_warm",
            "warmups": 1,
            "samples": 8,
            "needs_token": False,
        },
        {
            "url": f"{backend_url}/api/plans",
            "label": "plans_public_warm",
            "warmups": 1,
            "samples": 5,
            "needs_token": False,
        },
        {
            "url": f"{backend_url}/api/auth/google/config",
            "label": "auth_google_config_warm",
            "warmups": 1,
            "samples": 5,
            "needs_token": False,
        },
        {
            "url": f"{backend_url}/api/auth/me",
            "label": "auth_me_warm",
            "warmups": 1,
            "samples": 5,
            "needs_token": True,
        },
        {
            "url": f"{backend_url}/api/dashboard/summary",
            "label": "dashboard_summary_warm",
            "warmups": 1,
            "samples": 5,
            "needs_token": True,
        },
        {
            "url": f"{backend_url}/api/automations/summary",
            "label": "automations_summary_warm",
            "warmups": 1,
            "samples": 5,
            "needs_token": True,
        },
        {
            "url": f"{backend_url}/api/instagram/accounts",
            "label": "instagram_accounts_warm",
            "warmups": 1,
            "samples": 5,
            "needs_token": True,
        },
    ]

    results = []
    for p in probes:
        use_token = token if p["needs_token"] else None
        if p["needs_token"] and not token:
            results.append({
                "label": p["label"],
                "url": p["url"],
                "samples": 0,
                "error": "skipped_no_token",
            })
            continue
        results.append(
            _probe(
                p["url"],
                use_token,
                label=p["label"],
                warmups=p["warmups"],
                samples=p["samples"],
            )
        )

    # Print markdown table
    print("| Probe | Samples | Status(es) | min | p50 | p95 | max | notes |")
    print("|---|---:|---|---:|---:|---:|---:|---|")
    for r in results:
        if r.get("error"):
            print(
                f"| {r['label']} | {r.get('samples', 0)} | — | — | — | — | — | {r['error']} |"
            )
            continue
        statuses_compact = "/".join(sorted({str(s) for s in r["statuses"]}))
        print(
            f"| {r['label']} | {r['samples']} | {statuses_compact} | "
            f"{r['min_ms']} | {r['p50_ms']} | {r['p95_ms']} | {r['max_ms']} | "
            f"all_ms={r['all_ms']} |"
        )
    print()

    # Machine-readable dump for CI logs
    print("```json")
    print(json.dumps({"results": results}, indent=2))
    print("```")
    return 0


if __name__ == "__main__":
    sys.exit(main())
