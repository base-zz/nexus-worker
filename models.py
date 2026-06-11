from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class PricingRate(BaseModel):
    value: float | None = None
    unit: str | None = None
    is_per_foot: bool | None = None


class PricingSurcharges(BaseModel):
    catamaran_multiplier: float | None = None
    liveaboard_fee: float | None = None
    liveaboard_unit: str | None = None


class NavigationalLimits(BaseModel):
    min_air_draft_ft: float | None = None
    air_draft_source: str | None = None
    min_depth_ft: float | None = None
    depth_source: str | None = None


class HauloutSpecs(BaseModel):
    has_travel_lift: bool | None = None
    max_beam_ft: float | None = None
    max_tons: float | None = None
    diy_allowed: bool | None = None

    @field_validator("max_beam_ft", mode="before")
    @classmethod
    def reject_impossible_beam(cls, v: Any) -> Any:
        if isinstance(v, (int, float)) and v > 50:
            raise ValueError(f"max_beam_ft {v} exceeds reasonable limit (50ft)")
        return v

    @field_validator("max_tons", mode="before")
    @classmethod
    def reject_impossible_tons(cls, v: Any) -> Any:
        if isinstance(v, (int, float)) and v > 500:
            raise ValueError(f"max_tons {v} exceeds reasonable limit (500)")
        return v


class UtilityPolicies(BaseModel):
    electricity_metered: bool | None = None
    water_metered: bool | None = None
    liveaboard_permitted: bool | None = None


class PricingExtraction(BaseModel):
    """Raw LLM output for pricing extraction."""

    model_config = {"extra": "allow"}

    marina_name: str | None = None
    rates: dict[str, Any] | None = None
    surcharges: dict[str, Any] | None = None
    navigational_limits: dict[str, Any] | None = None
    haulout_specs: dict[str, Any] | None = None
    utility_policies: dict[str, Any] | None = None
    source_quotes: list[str] = Field(default_factory=list)

    @field_validator("rates", mode="before")
    @classmethod
    def validate_rates(cls, v: Any) -> Any:
        if v is None:
            return None
        if not isinstance(v, dict):
            raise ValueError("rates must be a dict")
        for key in ("daily", "monthly", "annual"):
            if key in v and v[key] is not None:
                PricingRate.model_validate(v[key])
        return v

    @field_validator("surcharges", mode="before")
    @classmethod
    def validate_surcharges(cls, v: Any) -> Any:
        if v is None:
            return None
        if not isinstance(v, dict):
            raise ValueError("surcharges must be a dict")
        PricingSurcharges.model_validate(v)
        return v

    @field_validator("navigational_limits", mode="before")
    @classmethod
    def validate_nav_limits(cls, v: Any) -> Any:
        if v is None:
            return None
        if not isinstance(v, dict):
            raise ValueError("navigational_limits must be a dict")
        NavigationalLimits.model_validate(v)
        return v

    @field_validator("haulout_specs", mode="before")
    @classmethod
    def validate_haulout(cls, v: Any) -> Any:
        if v is None:
            return None
        if not isinstance(v, dict):
            raise ValueError("haulout_specs must be a dict")
        HauloutSpecs.model_validate(v)
        return v

    @field_validator("utility_policies", mode="before")
    @classmethod
    def validate_utilities(cls, v: Any) -> Any:
        if v is None:
            return None
        if not isinstance(v, dict):
            raise ValueError("utility_policies must be a dict")
        UtilityPolicies.model_validate(v)
        return v


class PricingLog(BaseModel):
    """Normalized pricing data for the pricing_logs table."""

    marina_uid: str
    fetched_at_utc: str
    marina_name: str | None = None
    monthly_base: float | None = None
    is_per_ft: int | None = None
    catamaran_multiplier: float | None = None
    liveaboard_fee: float | None = None
    liveaboard_unit: str | None = None
    min_air_draft_ft: float | None = None
    air_draft_source: str | None = None
    min_depth_ft: float | None = None
    depth_source: str | None = None
    lift_max_beam_ft: float | None = None
    lift_max_tons: float | None = None
    diy_allowed: int | None = None
    electricity_metered: int | None = None
    water_metered: int | None = None
    liveaboard_permitted: int | None = None
    source_quotes: list[str] = Field(default_factory=list)
    extraction_hash: str | None = None
    provenance_json: str | None = None
    sync_dirty: int = 1
    created_at_utc: str | None = None


class FuelExtraction(BaseModel):
    """Output payload for fuel extraction (what goes into fuel_logs)."""

    marina_uid: str
    outcome_state: str
    reason_tag: str
    diesel_price: float | None = None
    gasoline_price: float | None = None
    fuel_dock: bool | None = None
    last_updated: str | None = None
    source_url: str | None = None
    source_text: str | None = None
    provenance: dict[str, Any] = Field(default_factory=dict)
    fetched_at_utc: str
    blocked_reason: str | None = None

    @field_validator("outcome_state")
    @classmethod
    def validate_outcome_state(cls, v: str) -> str:
        valid = {"has_public_price", "fuel_available_price_hidden", "fuel_unknown", "fetch_blocked"}
        if v not in valid:
            raise ValueError(f"outcome_state must be one of {valid}")
        return v

    @field_validator("blocked_reason")
    @classmethod
    def validate_blocked_reason(cls, v: str | None) -> str | None:
        if v is None:
            return v
        valid = {
            "access_denied_401",
            "access_denied_403",
            "rate_limited_429",
            "cloudflare_challenge",
            "dns_failure",
            "ssl_failure",
            "timeout",
        }
        if v not in valid:
            raise ValueError(f"blocked_reason must be one of {valid}")
        return v

    @field_validator("diesel_price", "gasoline_price", mode="before")
    @classmethod
    def reject_string_prices(cls, v: Any) -> Any:
        if isinstance(v, str):
            raise ValueError("price must be numeric or null, not a string")
        return v
