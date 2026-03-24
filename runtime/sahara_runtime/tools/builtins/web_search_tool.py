"""web_search 工具 — 搜索引擎查询，支持多 provider。

参考 nanobot/agent/tools/web.py WebSearchTool:
- DuckDuckGo (免费，无需 API key，默认)
- Brave Search (需要 BRAVE_API_KEY)
- Tavily (需要 TAVILY_API_KEY)
"""

from __future__ import annotations

import asyncio
import html
import os
import re
from typing import Any

import structlog

from sahara_runtime.tools.base import Tool

logger = structlog.get_logger(__name__)

_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/537.36"


def _strip_tags(text: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", "", text, flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", "", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    return html.unescape(text).strip()


def _normalize(text: str) -> str:
    text = re.sub(r"[ \t]+", " ", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _format_results(query: str, items: list[dict[str, Any]], n: int) -> str:
    if not items:
        return f"No results for: {query}"
    lines = [f"Results for: {query}\n"]
    for i, item in enumerate(items[:n], 1):
        title = _normalize(_strip_tags(item.get("title", "")))
        snippet = _normalize(_strip_tags(item.get("content", "")))
        url = item.get("url", "")
        lines.append(f"{i}. {title}\n   {url}")
        if snippet:
            lines.append(f"   {snippet}")
    return "\n".join(lines)


class WebSearchTool(Tool):
    """搜索引擎查询工具，支持多个 provider。"""

    def __init__(self, default_provider: str = "duckduckgo") -> None:
        self._default_provider = default_provider

    @property
    def name(self) -> str:
        return "web_search"

    @property
    def description(self) -> str:
        return "Search the web. Returns titles, URLs, and snippets."

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query",
                },
                "count": {
                    "type": "integer",
                    "description": "Number of results (1-10, default 5)",
                    "minimum": 1,
                    "maximum": 10,
                },
            },
            "required": ["query"],
        }

    async def execute(
        self, query: str, count: int | None = None, **kwargs: Any,
    ) -> str:
        n = min(max(count or 5, 1), 10)
        provider = self._default_provider

        if provider == "brave" and os.environ.get("BRAVE_API_KEY"):
            return await self._search_brave(query, n)
        if provider == "tavily" and os.environ.get("TAVILY_API_KEY"):
            return await self._search_tavily(query, n)
        return await self._search_duckduckgo(query, n)

    async def _search_duckduckgo(self, query: str, n: int) -> str:
        try:
            from duckduckgo_search import DDGS
            ddgs = DDGS(timeout=10)
            raw = await asyncio.to_thread(ddgs.text, query, max_results=n)
            if not raw:
                return f"No results for: {query}"
            items = [
                {
                    "title": r.get("title", ""),
                    "url": r.get("href", ""),
                    "content": r.get("body", ""),
                }
                for r in raw
            ]
            return _format_results(query, items, n)
        except ImportError:
            return (
                "Error: duckduckgo_search package not installed. "
                "Run: pip install duckduckgo-search"
            )
        except Exception as e:
            logger.warning("duckduckgo_search_failed", error=str(e))
            return f"Error: DuckDuckGo search failed: {e}"

    async def _search_brave(self, query: str, n: int) -> str:
        try:
            import httpx
            api_key = os.environ.get("BRAVE_API_KEY", "")
            async with httpx.AsyncClient() as client:
                r = await client.get(
                    "https://api.search.brave.com/res/v1/web/search",
                    params={"q": query, "count": n},
                    headers={
                        "Accept": "application/json",
                        "X-Subscription-Token": api_key,
                    },
                    timeout=10.0,
                )
                r.raise_for_status()
            items = [
                {
                    "title": x.get("title", ""),
                    "url": x.get("url", ""),
                    "content": x.get("description", ""),
                }
                for x in r.json().get("web", {}).get("results", [])
            ]
            return _format_results(query, items, n)
        except Exception as e:
            logger.warning("brave_search_failed", error=str(e))
            return await self._search_duckduckgo(query, n)

    async def _search_tavily(self, query: str, n: int) -> str:
        try:
            import httpx
            api_key = os.environ.get("TAVILY_API_KEY", "")
            async with httpx.AsyncClient() as client:
                r = await client.post(
                    "https://api.tavily.com/search",
                    headers={"Authorization": f"Bearer {api_key}"},
                    json={"query": query, "max_results": n},
                    timeout=15.0,
                )
                r.raise_for_status()
            return _format_results(query, r.json().get("results", []), n)
        except Exception as e:
            logger.warning("tavily_search_failed", error=str(e))
            return await self._search_duckduckgo(query, n)
