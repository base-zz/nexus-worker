"""nexus-worker/pricing_worker.py — Pricing extraction worker for the IE.

Uses ExtractionOrchestrator for queue I/O, Pydantic validation, and sync queue push.
Domain-specific extraction logic (DeepSeek API or local Ollama) is injected as the
extract_fn.  Raw LLM output is validated against PricingExtraction, then normalized
into the PricingLog DB schema.

Environment variables (strict, no defaults):
    REDIS_HOST               — Redis hostname
    REDIS_PORT               — Redis port
    PRICING_INBOUND_QUEUE    — Redis list name for inbound pricing jobs
    OUTBOUND_SYNC_QUEUE      — Redis list name for VPS sync
    USE_DEEPSEEK             — "true" to use Fireworks DeepSeek, else Ollama
    FIREWORKS_API_KEY        — Required when USE_DEEPSEEK=true
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any

try:
    import ollama
except ImportError:
    ollama = None

try:
    from fuel_extractor.app.markdown_convert import fetch_full_site_markdown, prune_marina_markdown
except ImportError:
    fetch_full_site_markdown = None
    prune_marina_markdown = None

from .extraction_orchestrator import ExtractionOrchestrator
from .models import PricingExtraction, PricingLog


class PricingWorkerError(Exception):
    """Backward-compat alias for scripts that import it directly."""
    pass


# ---------------------------------------------------------------------------
# Strict env loading — no defaults
# ---------------------------------------------------------------------------
REDIS_HOST = os.environ["REDIS_HOST"]
REDIS_PORT = int(os.environ["REDIS_PORT"])
PRICING_INBOUND_QUEUE = os.environ["PRICING_INBOUND_QUEUE"]
OUTBOUND_SYNC_QUEUE = os.environ["OUTBOUND_SYNC_QUEUE"]
PRICING_DLQ = os.environ.get("PRICING_DLQ", f"{PRICING_INBOUND_QUEUE}:dlq")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _extraction_hash(payload: dict[str, Any]) -> str:
    digest_input = {
        "marina_name": payload.get("marina_name"),
        "monthly_base": payload.get("monthly_base"),
        "is_per_ft": payload.get("is_per_ft"),
        "catamaran_multiplier": payload.get("catamaran_multiplier"),
        "liveaboard_fee": payload.get("liveaboard_fee"),
        "min_air_draft_ft": payload.get("min_air_draft_ft"),
        "min_depth_ft": payload.get("min_depth_ft"),
        "lift_max_beam_ft": payload.get("lift_max_beam_ft"),
        "lift_max_tons": payload.get("lift_max_tons"),
    }
    serialized = json.dumps(digest_input, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


PRICING_SYSTEM_PROMPT = """
You are a Marine Facility Auditor. Your goal is to extract technical and financial data from marina website markdown.

RULES OF EXTRACTION:
1. DO NOT CALCULATE. Only extract the raw numbers and units as written.
2. LOOK FOR THE 'WELL': If a travel lift is mentioned, search specifically for the maximum BEAM (width) it can accommodate. This is distinct from the marina's slip width.
3. WATERWAY GUIDE CLUE: On Waterway Guide pages, look for sections titled "Repair Services at [marina name]" which contain "Haulout Capabilities" subsections with travel lift specifications.
4. CRITICAL: Distinguish between LIFT CAPACITY (tons) and BEAM WIDTH (feet).
   - "70 ton max" refers to lift capacity (max_tons)
   - "25 ft max beam" refers to beam width (max_beam_ft)
   - DO NOT confuse these values. If only tons are mentioned, set max_beam_ft to null.
5. IDENTIFY THE 'CATAMARAN MULTIPLIER': Look for surcharges applied to catamarans (e.g., 1.5x length or double-slip pricing).
6. NAVIGATIONAL GATEKEEPING: Identify the most restrictive AIR DRAFT (bridge height) and WATER DEPTH (at MLW/Low Tide) required to reach the marina.
7. DIY PERMISSIONS: Determine if boat owners are permitted to perform their own maintenance (e.g., 'DIY allowed' vs 'Yard-only labor').

EDGE CASE RULES:
- If a value is not found, use null (not 0 or empty string)
- If multiple values are found (e.g., seasonal rates), report the highest rate
- If units are unclear, preserve the unit as written (e.g., "$/ft/mo" or "$/season")
- If depth is given at MLW vs MHW, report the more restrictive (shallower) value

OUTPUT SCHEMA:
Return ONLY valid JSON with this exact structure:
{
  "marina_name": "string",
  "rates": {
    "daily": {"value": number, "unit": "string", "is_per_foot": boolean},
    "monthly": {"value": number, "unit": "string", "is_per_foot": boolean},
    "annual": {"value": number, "unit": "string", "is_per_foot": boolean}
  },
  "surcharges": {
    "catamaran_multiplier": number,
    "liveaboard_fee": number,
    "liveaboard_unit": "string"
  },
  "navigational_limits": {
    "min_air_draft_ft": number,
    "air_draft_source": "string",
    "min_depth_ft": number,
    "depth_source": "string"
  },
  "haulout_specs": {
    "has_travel_lift": boolean,
    "max_beam_ft": number,
    "max_tons": number,
    "diy_allowed": boolean
  },
  "utility_policies": {
    "electricity_metered": boolean,
    "water_metered": boolean,
    "liveaboard_permitted": boolean
  },
  "source_quotes": ["string"]
}

EXAMPLE INPUT:
"Monthly rates: $18/ft for monohulls, $27/ft for catamarans. Liveaboard fee: $50/mo. Bridge clearance: 45ft at mean high water. Channel depth: 8ft at MLW. 50-ton travel lift, max beam 24ft. DIY work permitted in designated area."

EXAMPLE OUTPUT:
{
  "marina_name": null,
  "rates": {
    "daily": null,
    "monthly": {"value": 18, "unit": "$/ft/mo", "is_per_foot": true},
    "annual": null
  },
  "surcharges": {
    "catamaran_multiplier": 1.5,
    "liveaboard_fee": 50,
    "liveaboard_unit": "$/mo"
  },
  "navigational_limits": {
    "min_air_draft_ft": 45,
    "air_draft_source": "mean high water",
    "min_depth_ft": 8,
    "depth_source": "MLW"
  },
  "haulout_specs": {
    "has_travel_lift": true,
    "max_beam_ft": 24,
    "max_tons": 50,
    "diy_allowed": true
  },
  "utility_policies": {
    "electricity_metered": null,
    "water_metered": null,
    "liveaboard_permitted": null
  },
  "source_quotes": [
    "$18/ft for monohulls, $27/ft for catamarans",
    "Liveaboard fee: $50/mo",
    "Bridge clearance: 45ft at mean high water",
    "Channel depth: 8ft at MLW",
    "50-ton travel lift, max beam 24ft",
    "DIY work permitted in designated area"
  ]
}
"""


# ---------------------------------------------------------------------------
# Extraction backend
# ---------------------------------------------------------------------------
def extract_pricing(
    base_url: str,
    timeout_seconds: int = 45,
    max_pages: int = 20,
    html_content: str | None = None,
) -> dict[str, Any]:
    """Extract pricing via local Ollama (qwen2.5:7b).

    Fetches marina website content, sends it to the local LLM with the
    PRICING_SYSTEM_PROMPT, validates the raw JSON output, and normalizes
    to the pricing_logs database schema.
    """
    if ollama is None:
        raise RuntimeError("ollama package not installed")

    ollama_host = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    client = ollama.Client(host=ollama_host)

    # 1. Get content
    if html_content:
        full_markdown = html_content
        if prune_marina_markdown is not None:
            full_markdown = prune_marina_markdown(full_markdown)
        else:
            full_markdown = full_markdown[:10000]
    elif fetch_full_site_markdown is not None:
        full_markdown = fetch_full_site_markdown(base_url, timeout_seconds, max_pages)
        full_markdown = prune_marina_markdown(full_markdown)
    else:
        raise RuntimeError("html_content not provided and fuel_extractor module not available")

    # 2. Call Ollama
    try:
        response = client.chat(
            model="qwen2.5:7b",
            messages=[
                {"role": "system", "content": PRICING_SYSTEM_PROMPT},
                {"role": "user", "content": full_markdown},
            ],
            options={
                "temperature": 0.1,
                "num_ctx": 16384,
            },
            format="json",
        )
    except Exception as exc:
        raise RuntimeError(f"Local Ollama inference failed: {exc}") from exc

    # 3. Parse JSON
    try:
        content = response["message"]["content"]
        result = json.loads(content)
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise RuntimeError(f"Failed to parse local model response as JSON: {exc}") from exc

    # 4. Validate raw LLM output
    PricingExtraction.model_validate(result)

    # 5. Normalize to DB schema
    normalized = _normalize_pricing_result(result)
    normalized["extraction_hash"] = _extraction_hash(normalized)
    normalized["fetched_at_utc"] = _utc_now_iso()
    return normalized


def _normalize_pricing_result(llm_output: dict[str, Any]) -> dict[str, Any]:
    """Normalize LLM output to pricing_logs database schema."""
    rates = llm_output.get("rates", {})
    surcharges = llm_output.get("surcharges", {})
    nav_limits = llm_output.get("navigational_limits", {})
    haulout = llm_output.get("haulout_specs", {})
    utilities = llm_output.get("utility_policies", {})

    monthly_rate = rates.get("monthly", {})
    monthly_base = monthly_rate.get("value") if monthly_rate else None
    is_per_ft = monthly_rate.get("is_per_foot", False) if monthly_rate else None

    max_beam_ft = haulout.get("max_beam_ft")
    max_tons = haulout.get("max_tons")

    if max_beam_ft is not None and max_beam_ft > 50:
        print(f"[WARNING] Rejecting impossible max_beam_ft: {max_beam_ft}ft")
        max_beam_ft = None

    if max_tons is not None and max_tons > 500:
        print(f"[WARNING] Rejecting impossible max_tons: {max_tons}")
        max_tons = None

    return {
        "marina_name": llm_output.get("marina_name"),
        "monthly_base": monthly_base,
        "is_per_ft": 1 if is_per_ft else 0 if is_per_ft is not None else None,
        "catamaran_multiplier": surcharges.get("catamaran_multiplier"),
        "liveaboard_fee": surcharges.get("liveaboard_fee"),
        "min_air_draft_ft": nav_limits.get("min_air_draft_ft"),
        "air_draft_source": nav_limits.get("air_draft_source"),
        "min_depth_ft": nav_limits.get("min_depth_ft"),
        "depth_source": nav_limits.get("depth_source"),
        "lift_max_beam_ft": max_beam_ft,
        "lift_max_tons": max_tons,
        "diy_allowed": 1 if haulout.get("diy_allowed") else 0 if haulout.get("diy_allowed") is not None else None,
        "electricity_metered": 1 if utilities.get("electricity_metered") else 0 if utilities.get("electricity_metered") is not None else None,
        "water_metered": 1 if utilities.get("water_metered") else 0 if utilities.get("water_metered") is not None else None,
        "liveaboard_permitted": 1 if utilities.get("liveaboard_permitted") else 0 if utilities.get("liveaboard_permitted") is not None else None,
        "source_quotes": llm_output.get("source_quotes", []),
    }


# ---------------------------------------------------------------------------
# Domain extraction function (injected into ExtractionOrchestrator)
# ---------------------------------------------------------------------------
def extract_pricing_job(job_input: dict[str, Any]) -> dict[str, Any]:
    """Run Ollama extraction, return normalized dict for PricingLog validation.

    The returned dict is validated against PricingLog by the orchestrator.
    """
    url = job_input.get("url") or job_input.get("source_url", "")
    marina_uid = job_input.get("marina_uid", "")
    if not url:
        raise ValueError("job_input missing url or source_url")

    html_content = job_input.get("content") or None

    result = extract_pricing(url, html_content=html_content)

    result["marina_uid"] = marina_uid
    result["sync_dirty"] = 1
    result["created_at_utc"] = result.get("fetched_at_utc", _utc_now_iso())
    return result


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main() -> None:
    orchestrator = ExtractionOrchestrator(
        inbound_queue=PRICING_INBOUND_QUEUE,
        sync_queue=OUTBOUND_SYNC_QUEUE,
        dlq=PRICING_DLQ,
        model_class=PricingLog,
        extract_fn=extract_pricing_job,
        fetch_content=False,  # extractors handle their own fetching
    )
    orchestrator.run_forever()


# Backward compatibility for scripts that import the old name
extract_pricing_locally = extract_pricing


if __name__ == "__main__":
    main()
