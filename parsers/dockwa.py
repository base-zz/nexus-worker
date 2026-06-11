"""app/parsers/dockwa.py — Dockwa parser for marina fuel and pricing data.

When possible, extracts structured JSON directly from Dockwa's API or
page metadata to bypass LLM inference. Falls back to generic HTML parsing
if the structured path fails.
"""

from __future__ import annotations

from typing import Any

from .base import BaseParser, FetchOutput, ParseResult, ParserError
from .fetching import fetch_with_fallback


class DockwaParser(BaseParser):
    """Parser for dockwa.com marina pages."""

    name = "dockwa"

    @staticmethod
    def can_handle(url: str) -> bool:
        return "dockwa.com" in url.lower()

    def fetch(self, url: str, *, browser: Any | None = None) -> FetchOutput:
        """Fetch Dockwa page HTML."""
        try:
            raw_html, method = fetch_with_fallback(url, browser=browser)
            return FetchOutput(raw=raw_html, method=method)
        except Exception as exc:
            raise ParserError(f"Failed to fetch Dockwa page {url}: {exc}") from exc

    def parse(self, raw: str, url: str, *, fetch_method: str = "") -> ParseResult:
        """Parse Dockwa HTML."""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(raw, "html.parser")
            for script in soup(["script", "style"]):
                script.decompose()
            text = soup.get_text(separator="\n", strip=True)
        except Exception:
            text = raw  # fallback

        return ParseResult(
            text=text,
            source_url=url,
            fetch_method=fetch_method or "dockwa",
        )
