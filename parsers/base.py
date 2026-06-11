"""app/parsers/base.py — Abstract base class for content parsers.

All site-specific parsers must inherit from BaseParser and register themselves
in the parser registry so the dispatcher can route URLs to the correct handler.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class FetchOutput:
    """Raw content + metadata from a fetch operation."""

    raw: str
    method: str = ""
    metadata: dict[str, Any] | None = None


@dataclass
class ParseResult:
    """Structured result from any parser."""

    text: str
    haulout_section: str | None = None
    source_url: str = ""
    fetch_method: str = ""
    metadata: dict[str, Any] | None = None

    @property
    def input_for_llm(self) -> str:
        """Combine haulout section with full text for LLM ingestion."""
        if self.haulout_section:
            return f"Haulout Capabilities:\n{self.haulout_section}\n\n{self.text}"
        return self.text


class ParserError(Exception):
    pass


class ContentFetcherError(Exception):
    """Raised by the dispatcher when fetch or parse fails."""
    pass


class BaseParser(ABC):
    """Base class for all site-specific content parsers.

    Each parser declares which URLs it can handle via `can_handle()`.
    The dispatcher selects the first parser that returns True.
    """

    name: str = ""

    @staticmethod
    @abstractmethod
    def can_handle(url: str) -> bool:
        """Return True if this parser handles the given URL."""
        ...

    @abstractmethod
    def fetch(self, url: str, *, browser: Any | None = None) -> FetchOutput:
        """Fetch raw content (HTML, JSON, etc.) from the URL.

        Returns a FetchOutput containing the raw content and metadata
        such as the fetch method used (requests, playwright, etc.).
        """
        ...

    @abstractmethod
    def parse(self, raw: str, url: str, *, fetch_method: str = "") -> ParseResult:
        """Parse raw content into structured text for LLM ingestion."""
        ...

    def process(self, url: str, *, browser: Any | None = None) -> ParseResult:
        """Convenience method: fetch then parse in one call."""
        output = self.fetch(url, browser=browser)
        return self.parse(output.raw, url, fetch_method=output.method)
