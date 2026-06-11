"""app/parsers/waterway_guide.py — Waterway Guide parser.

Handles waterwayguide.com marina pages, including discovery of the
actual marina website link and extraction of haulout capabilities sections.
"""

from __future__ import annotations

from typing import Any

from bs4 import BeautifulSoup

from .base import BaseParser, FetchOutput, ParseResult, ParserError
from .fetching import deobfuscate_cloudflare_emails, fetch_with_fallback


def _extract_marina_website(soup: BeautifulSoup) -> str | None:
    """Find the actual marina website link embedded in a Waterway Guide page."""
    website_label = soup.find(string=lambda text: text and text.strip() == "Website")
    if not website_label:
        return None

    parent = website_label.find_parent()
    if not parent:
        return None

    link = parent.find("a")
    if link and link.get("href"):
        href = link.get("href")
        if href and "waterwayguide.com" not in href and "email-protection" not in href:
            return href

    for sibling in parent.find_all_next():
        if sibling.name == "a" and sibling.get("href"):
            href = sibling.get("href")
            if "waterwayguide.com" not in href and "email-protection" not in href:
                return href

    return None


class WaterwayGuideParser(BaseParser):
    """Parser for waterwayguide.com pages."""

    name = "waterway_guide"

    @staticmethod
    def can_handle(url: str) -> bool:
        return "waterwayguide.com" in url.lower()

    def fetch(self, url: str, *, browser: Any | None = None) -> FetchOutput:
        try:
            raw_html, method = fetch_with_fallback(url, browser=browser)
        except Exception as exc:
            raise ParserError(f"Failed to fetch Waterway Guide page {url}: {exc}") from exc

        raw_html = deobfuscate_cloudflare_emails(raw_html)
        return FetchOutput(raw=raw_html, method=method)

    def parse(self, raw: str, url: str, *, fetch_method: str = "") -> ParseResult:
        try:
            soup = BeautifulSoup(raw, "html.parser")
            for script in soup(["script", "style"]):
                script.decompose()

            # Extract haulout section if present
            haulout_heading = soup.find(
                string=lambda text: text and "Haulout Capabilities" in text
            )
            haulout = None
            if haulout_heading:
                haulout_parent = haulout_heading.find_parent("div")
                if haulout_parent:
                    haulout_data = []
                    for div in haulout_parent.find_all("div"):
                        text = div.get_text(strip=True)
                        if text and ":" in text:
                            haulout_data.append(text)
                    if haulout_data:
                        haulout = "\n".join(haulout_data)

            text = soup.get_text(separator="\n", strip=True)
        except Exception as exc:
            raise ParserError(f"Failed to parse Waterway Guide HTML from {url}: {exc}") from exc

        return ParseResult(
            text=text,
            haulout_section=haulout,
            source_url=url,
            fetch_method=fetch_method or "waterway_guide",
        )
