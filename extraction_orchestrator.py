"""app/extraction_orchestrator.py — Generic extraction worker orchestrator.

Encapsulates the shared "Queue-Pop → Dispatch → Parse → Validate → Push" loop.
Domain-specific workers (pricing, fuel) inject their own:
  - Pydantic model for validation
  - extract_fn that calls the LLM / API and returns raw dict output
  - Redis queue names and sync queue target

This eliminates duplicated queue I/O, DLQ handling, and sync-push logic
across all extraction worker types.
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections.abc import Callable
from typing import Any

import redis
from pydantic import BaseModel

from .parsers import fetch_and_parse
from .validation_engine import validate_extraction


# ---------------------------------------------------------------------------
# Strict env loading — no defaults
# ---------------------------------------------------------------------------
REDIS_HOST = os.environ["REDIS_HOST"]
REDIS_PORT = int(os.environ["REDIS_PORT"])


def _redis_client() -> redis.Redis:
    pool = redis.ConnectionPool(
        host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
    )
    return redis.Redis(connection_pool=pool)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------
class JobResult:
    """Outcome of processing a single job."""

    def __init__(
        self,
        *,
        success: bool,
        data: dict[str, Any] | None = None,
        errors: list[str] | None = None,
    ):
        self.success = success
        self.data = data or {}
        self.errors = errors or []


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
class ExtractionOrchestrator:
    """Generic worker that pops jobs from Redis, validates output, and syncs."""

    def __init__(
        self,
        *,
        inbound_queue: str,
        sync_queue: str,
        dlq: str,
        model_class: type[BaseModel],
        extract_fn: Callable[[dict[str, Any]], dict[str, Any]],
        fetch_content: bool = True,
        redis_client: redis.Redis | None = None,
    ):
        """
        Args:
            inbound_queue: Redis list name to pop jobs from.
            sync_queue: Redis list name to push valid results to.
            dlq: Redis list name for failed / invalid jobs.
            model_class: Pydantic model class used to validate extract_fn output.
            extract_fn: Domain-specific function taking job payload dict,
                returning raw dict output for validation.
            fetch_content: If True, the orchestrator calls `fetch_and_parse()`
                using the URL from the job payload before calling extract_fn.
            redis_client: Optional shared Redis client. Created internally if None.
        """
        self.inbound = inbound_queue
        self.processing = f"{inbound_queue}:processing"
        self.sync_queue = sync_queue
        self.dlq = dlq
        self.model_class = model_class
        self.extract_fn = extract_fn
        self.fetch_content = fetch_content
        self.r = redis_client or _redis_client()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run_once(self, timeout: int = 5) -> JobResult | None:
        """Pop and process a single job. Returns None if queue empty."""
        raw = self.r.blmove(
            self.inbound, self.processing, timeout, "RIGHT", "LEFT"
        )
        if not raw:
            return None
        return self._process(raw)

    def run_forever(self, poll_interval: int = 1) -> None:
        """Blocking loop: pop, process, repeat."""
        print(f"[*] Orchestrator active on '{self.inbound}' → '{self.sync_queue}'")
        while True:
            try:
                result = self.run_once(timeout=5)
                if result is None:
                    continue
                if result.success:
                    print(f"[✔] Synced job successfully")
                else:
                    print(f"[X] Job failed: {result.errors}")
            except redis.ConnectionError:
                print("[X] Redis offline. Retrying...")
                time.sleep(5)
            except Exception as exc:
                print(f"[!] Critical loop error: {exc}", file=sys.stderr)
                time.sleep(5)

    # ------------------------------------------------------------------
    # Internal pipeline
    # ------------------------------------------------------------------
    def _process(self, raw_job: str) -> JobResult:
        try:
            payload = json.loads(raw_job)
        except json.JSONDecodeError as exc:
            self._dlq(raw_job, f"malformed JSON: {exc}")
            self._ack(raw_job)
            return JobResult(success=False, errors=[f"malformed JSON: {exc}"])

        # ---- Step 1: Fetch content (if configured) ------------------
        content = ""
        fetch_method = ""
        if self.fetch_content:
            url = payload.get("url") or payload.get("source_url", "")
            if url:
                try:
                    parsed = fetch_and_parse(url)
                    content = parsed.input_for_llm
                    fetch_method = parsed.fetch_method
                except Exception as exc:
                    self._dlq(raw_job, f"fetch failed: {exc}")
                    self._ack(raw_job)
                    return JobResult(
                        success=False, errors=[f"fetch failed: {exc}"]
                    )
            else:
                content = payload.get("content", "")
                fetch_method = payload.get("fetch_method", "")

        # Merge content into payload for extract_fn
        job_input = {**payload, "content": content}

        # ---- Step 2: Domain-specific extraction ----------------------
        try:
            raw_output = self.extract_fn(job_input)
        except Exception as exc:
            self._dlq(raw_job, f"extraction failed: {exc}")
            self._ack(raw_job)
            return JobResult(
                success=False, errors=[f"extraction failed: {exc}"]
            )

        # ---- Step 3: Pydantic validation -----------------------------
        vresult = validate_extraction(self.model_class, raw_output)
        if not vresult:
            self._dlq(raw_job, f"validation failed: {vresult.errors}")
            self._ack(raw_job)
            return JobResult(success=False, errors=vresult.errors)
        validated = vresult.data

        # ---- Step 4: Push to sync queue -----------------------------
        sync_payload = {
            "target_table": self._infer_table(),
            "fetch_method": fetch_method,
            "data": validated.model_dump(mode="json"),
        }
        self.r.lpush(self.sync_queue, json.dumps(sync_payload, default=str))
        self._ack(raw_job)
        return JobResult(success=True, data=sync_payload)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _dlq(self, raw_job: str, reason: str) -> None:
        envelope = {"original": raw_job, "reason": reason, "dlq": self.dlq}
        self.r.lpush(self.dlq, json.dumps(envelope, default=str))

    def _ack(self, raw_job: str) -> None:
        """Remove job from the processing queue (atomic ack)."""
        self.r.lrem(self.processing, 1, raw_job)

    def _infer_table(self) -> str:
        """Infer target table from model class name (convention over config)."""
        name = self.model_class.__name__
        if "Pricing" in name:
            return "pricing_logs"
        if "Fuel" in name:
            return "fuel_logs"
        return "extractions"
