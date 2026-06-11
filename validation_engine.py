"""fuel_extractor_v2/app/validation_engine.py — Centralized Pydantic validation.

Shared helper for validating raw extraction dicts against Pydantic models.
Used by ExtractionOrchestrator and standalone scripts (reextract_week, etc.).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ValidationError


class ValidationResult:
    """Outcome of a Pydantic validation attempt."""

    def __init__(
        self,
        *,
        success: bool,
        data: BaseModel | None = None,
        errors: list[str] | None = None,
    ):
        self.success = success
        self.data = data
        self.errors = errors or []

    def __bool__(self) -> bool:
        return self.success


def validate_extraction(
    model_class: type[BaseModel],
    raw_output: dict[str, Any],
) -> ValidationResult:
    """Validate a raw extraction dict against a Pydantic model.

    Args:
        model_class: The Pydantic model to validate against.
        raw_output: The raw dict output from an extraction function.

    Returns:
        ValidationResult with ``.data`` set to the validated model on success,
        or ``.errors`` as a list of human-readable strings on failure.
    """
    try:
        validated = model_class.model_validate(raw_output)
        return ValidationResult(success=True, data=validated)
    except ValidationError as exc:
        errors = [
            f"{' → '.join(str(loc) for loc in e['loc'])}: {e['msg']}"
            for e in exc.errors()
        ]
        return ValidationResult(success=False, errors=errors)
