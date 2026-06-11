"""nexus-worker/fuel_worker.py — Fuel extraction worker for the IE.

Uses ExtractionOrchestrator for queue I/O, DLQ handling, Pydantic validation,
and sync queue push. Domain-specific extraction logic (Dockwa pre-check +
LLM fallback via fuel_extractor) is injected as the extract_fn.

Environment variables (strict, no defaults):
    REDIS_HOST              — Redis hostname
    REDIS_PORT              — Redis port
    FUEL_INBOUND_QUEUE      — Redis list name for inbound fuel jobs
    OUTBOUND_SYNC_QUEUE     — Redis list name for VPS sync
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any

try:
    from fuel_extractor.app.main import extract_fuel
    from fuel_extractor.app.markdown_convert import fetch_dockwa_fuel_snapshot
    from fuel_extractor.app.schemas import ExtractRequest, ExtractResponse
except ImportError:
    extract_fuel = None
    fetch_dockwa_fuel_snapshot = None
    ExtractRequest = None
    ExtractResponse = None

from .extraction_orchestrator import ExtractionOrchestrator
from .models import FuelExtraction

# ---------------------------------------------------------------------------
# Strict env loading — no defaults
# ---------------------------------------------------------------------------
REDIS_HOST = os.environ["REDIS_HOST"]
REDIS_PORT = int(os.environ["REDIS_PORT"])
FUEL_INBOUND_QUEUE = os.environ["FUEL_INBOUND_QUEUE"]
OUTBOUND_SYNC_QUEUE = os.environ["OUTBOUND_SYNC_QUEUE"]
FUEL_DLQ = os.environ.get("FUEL_DLQ", f"{FUEL_INBOUND_QUEUE}:dlq")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _as_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _choose_source_url(job_input: dict[str, Any]) -> str:
    dockwa_url = job_input.get("dockwa_url")
    if isinstance(dockwa_url, str) and dockwa_url.strip():
        return dockwa_url.strip()

    marinas_url = job_input.get("marinas_url")
    if isinstance(marinas_url, str) and marinas_url.strip():
        normalized = marinas_url.strip()
        if not normalized.lower().startswith("https://marinas.com/map/"):
            return normalized

    website_url = job_input.get("website_url")
    if isinstance(website_url, str) and website_url.strip():
        return website_url.strip()

    raise ValueError("job requires at least one source URL: dockwa_url, marinas_url, or website_url")


def _to_extract_request(job_input: dict[str, Any]) -> ExtractRequest:
    marina_uid = _as_non_empty_string(job_input.get("marina_uid"), "marina_uid")
    name = _as_non_empty_string(job_input.get("name"), "name")

    lat = job_input.get("lat")
    lon = job_input.get("lon")
    if not isinstance(lat, (int, float)):
        raise ValueError("lat must be numeric")
    if not isinstance(lon, (int, float)):
        raise ValueError("lon must be numeric")

    source_url = _choose_source_url(job_input)

    request_payload = {
        "job_id": f"fuel-{marina_uid}-{_utc_now_iso()}",
        "fuel_source_id": None,
        "name": name,
        "website_url": source_url,
        "phone": None,
        "lat": float(lat),
        "lon": float(lon),
        "max_discovery_depth": 2,
        "max_pages": 8,
        "prefer_pdfs": True,
        "timeout_seconds": 45,
        "skip_if_verified_within_hours": 24,
    }
    return ExtractRequest(**request_payload)


def _try_dockwa_extraction(job_input: dict[str, Any], timeout_seconds: int) -> dict[str, Any] | None:
    dockwa_url = job_input.get("dockwa_url")
    if not isinstance(dockwa_url, str) or not dockwa_url.strip():
        return None

    try:
        snapshot = fetch_dockwa_fuel_snapshot(dockwa_url.strip(), timeout_seconds)
    except Exception:
        return None

    if not isinstance(snapshot, dict):
        return None

    has_price = snapshot.get("diesel_price") is not None or snapshot.get("gasoline_price") is not None
    if not has_price:
        return None

    return {
        "diesel_price": snapshot.get("diesel_price"),
        "gasoline_price": snapshot.get("gasoline_price"),
        "fuel_dock": True,
        "is_non_ethanol": snapshot.get("is_non_ethanol"),
        "last_updated": snapshot.get("last_updated"),
        "source_text": snapshot.get("source_text"),
        "source_url": dockwa_url.strip(),
        "confidence": 1.0,
    }


def _build_output_from_dockwa(job_input: dict[str, Any], dockwa_result: dict[str, Any]) -> dict[str, Any]:
    marina_uid = _as_non_empty_string(job_input.get("marina_uid"), "marina_uid")
    fetched_at_utc = _utc_now_iso()

    diesel_price = dockwa_result.get("diesel_price")
    gasoline_price = dockwa_result.get("gasoline_price")
    source_url = dockwa_result.get("source_url")
    source_text = dockwa_result.get("source_text")

    provenance: dict[str, Any] = {}
    if diesel_price is not None:
        provenance["diesel_price"] = {"source": "dockwa_json", "seen_at": fetched_at_utc}
    if gasoline_price is not None:
        provenance["gasoline_price"] = {"source": "dockwa_json", "seen_at": fetched_at_utc}
    provenance["fuel_dock"] = {"source": "dockwa_json", "seen_at": fetched_at_utc}

    return {
        "marina_uid": marina_uid,
        "outcome_state": "has_public_price",
        "reason_tag": "dockwa_price_observed",
        "diesel_price": diesel_price,
        "gasoline_price": gasoline_price,
        "fuel_dock": dockwa_result.get("fuel_dock"),
        "last_updated": dockwa_result.get("last_updated"),
        "source_url": source_url,
        "source_text": source_text,
        "provenance": provenance,
        "fetched_at_utc": fetched_at_utc,
        "blocked_reason": None,
    }


def _map_blocked_reason(response: ExtractResponse) -> str | None:
    error_code = response.error_code
    reason = response.reason

    lowered_reason = ""
    if isinstance(reason, str):
        lowered_reason = reason.lower()

    if error_code == "DISCOVERY_ERROR":
        if "403" in lowered_reason:
            return "access_denied_403"
        if "401" in lowered_reason:
            return "access_denied_401"
        if "429" in lowered_reason:
            return "rate_limited_429"
        if "cloudflare" in lowered_reason:
            return "cloudflare_challenge"
        if "dns" in lowered_reason:
            return "dns_failure"
        if "ssl" in lowered_reason or "certificate" in lowered_reason:
            return "ssl_failure"
        if "timeout" in lowered_reason:
            return "timeout"

    if error_code == "CONVERSION_ERROR":
        if "timeout" in lowered_reason:
            return "timeout"
        if "ssl" in lowered_reason or "certificate" in lowered_reason:
            return "ssl_failure"
        if "dns" in lowered_reason:
            return "dns_failure"

    return None


def _derive_outcome(response: ExtractResponse) -> tuple[str, str, str | None]:
    diesel_price = response.extraction.diesel_price
    gasoline_price = response.extraction.gasoline_price
    fuel_dock = response.extraction.fuel_dock

    has_price = diesel_price is not None or gasoline_price is not None
    has_fuel_dock = fuel_dock is True

    if response.status == "error":
        blocked_reason = _map_blocked_reason(response)
        if blocked_reason is not None:
            return "fetch_blocked", "marina_site_blocked", blocked_reason
        return "fuel_unknown", "schema_validation_failed", None

    if has_price:
        source_url = response.extraction.source_url
        if isinstance(source_url, str) and "dockwa.com" in source_url:
            return "has_public_price", "dockwa_price_observed", None
        return "has_public_price", "price_observed_publicly", None

    if has_fuel_dock:
        return "fuel_available_price_hidden", "price_not_published_publicly", None

    reason = response.reason
    if isinstance(reason, str) and reason.strip():
        lowered_reason = reason.lower()
        if "candidate" in lowered_reason and "link" in lowered_reason:
            return "fuel_unknown", "no_dockwa_link", None

    return "fuel_unknown", "fuel_not_detected", None


def _price_source(outcome_state: str, source_url: str | None) -> str:
    if isinstance(source_url, str) and source_url.strip():
        lowered = source_url.lower()
        if "dockwa.com" in lowered:
            return "dockwa_json"
        if "marinas.com" in lowered:
            return "marinas_web"
        return "website_text"

    if outcome_state == "fuel_available_price_hidden":
        return "not_published_online"

    return "none"


def _normalize_confidence(value: Any) -> float:
    if not isinstance(value, (int, float)):
        return 0.0
    confidence = float(value)
    if confidence < 0.0:
        return 0.0
    if confidence > 1.0:
        return 1.0
    return confidence


def _build_provenance_payload(
    response: ExtractResponse,
    fetched_at_utc: str,
    price_source: str,
) -> dict[str, Any]:
    provenance: dict[str, Any] = {}

    if response.extraction.diesel_price is not None:
        provenance["diesel_price"] = {"source": price_source, "seen_at": fetched_at_utc}
    if response.extraction.gasoline_price is not None:
        provenance["gasoline_price"] = {"source": price_source, "seen_at": fetched_at_utc}
    if response.extraction.fuel_dock is not None:
        provenance["fuel_dock"] = {"source": price_source, "seen_at": fetched_at_utc}

    response_reason = response.reason
    if isinstance(response_reason, str) and response_reason.strip():
        provenance["reason"] = {"text": response_reason.strip(), "seen_at": fetched_at_utc}

    return provenance


def _extraction_hash(payload: dict[str, Any]) -> str:
    digest_input = {
        "marina_uid": payload.get("marina_uid"),
        "outcome_state": payload.get("outcome_state"),
        "reason_tag": payload.get("reason_tag"),
        "blocked_reason": payload.get("blocked_reason"),
        "diesel_price": payload.get("diesel_price"),
        "gasoline_price": payload.get("gasoline_price"),
        "fuel_dock": payload.get("fuel_dock"),
        "last_updated": payload.get("last_updated"),
        "source_url": payload.get("source_url"),
        "source_text": payload.get("source_text"),
    }
    serialized = json.dumps(digest_input, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _build_output_payload(job_input: dict[str, Any], response: ExtractResponse) -> dict[str, Any]:
    marina_uid = _as_non_empty_string(job_input.get("marina_uid"), "marina_uid")
    fetched_at_utc = _utc_now_iso()

    outcome_state, reason_tag, blocked_reason = _derive_outcome(response)

    source_url = response.extraction.source_url
    source_text = response.extraction.source_text

    if not isinstance(source_url, str) or not source_url.strip():
        source_url = response.evidence.source_url

    if not isinstance(source_url, str) or not source_url.strip():
        source_url = _choose_source_url(job_input)

    if not isinstance(source_text, str) or not source_text.strip():
        source_text = None

    price_source = _price_source(outcome_state, source_url)
    provenance = _build_provenance_payload(response, fetched_at_utc, price_source)

    return {
        "marina_uid": marina_uid,
        "outcome_state": outcome_state,
        "reason_tag": reason_tag,
        "diesel_price": response.extraction.diesel_price,
        "gasoline_price": response.extraction.gasoline_price,
        "fuel_dock": response.extraction.fuel_dock,
        "last_updated": response.extraction.last_updated,
        "source_url": source_url,
        "source_text": source_text,
        "provenance": provenance,
        "fetched_at_utc": fetched_at_utc,
        "blocked_reason": blocked_reason,
        "extraction_hash": _extraction_hash({
            "marina_uid": marina_uid,
            "outcome_state": outcome_state,
            "reason_tag": reason_tag,
            "blocked_reason": blocked_reason,
            "diesel_price": response.extraction.diesel_price,
            "gasoline_price": response.extraction.gasoline_price,
            "fuel_dock": response.extraction.fuel_dock,
            "last_updated": response.extraction.last_updated,
            "source_url": source_url,
            "source_text": source_text,
        }),
    }


# ---------------------------------------------------------------------------
# Domain extraction function (injected into ExtractionOrchestrator)
# ---------------------------------------------------------------------------
def extract_fuel_job(job_input: dict[str, Any]) -> dict[str, Any]:
    """Try Dockwa pre-check, fall back to LLM extraction.

    Returns a raw dict that FuelExtraction can validate.
    """
    # 1. Dockwa fast-path
    dockwa_url = job_input.get("dockwa_url")
    if dockwa_url and fetch_dockwa_fuel_snapshot is not None:
        result = _try_dockwa_extraction(job_input, 45)
        if result is not None:
            return _build_output_from_dockwa(job_input, result)

    # 2. LLM fallback
    if extract_fuel is None or ExtractRequest is None:
        raise RuntimeError("fuel_extractor dependencies not available")

    request = _to_extract_request(job_input)
    response = extract_fuel(request)
    return _build_output_payload(job_input, response)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main() -> None:
    orchestrator = ExtractionOrchestrator(
        inbound_queue=FUEL_INBOUND_QUEUE,
        sync_queue=OUTBOUND_SYNC_QUEUE,
        dlq=FUEL_DLQ,
        model_class=FuelExtraction,
        extract_fn=extract_fuel_job,
        fetch_content=False,  # extract_fuel handles its own fetching
    )
    orchestrator.run_forever()


if __name__ == "__main__":
    main()
