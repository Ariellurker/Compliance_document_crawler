from typing import Any, Dict, Optional
from urllib.parse import urlparse

from .base import SiteAdapter
from .generic import GenericHtmlAdapter
from .playwright_rule import PlaywrightRuleAdapter


class SiteRegistry:
    def __init__(self):
        self._adapters: Dict[str, SiteAdapter] = {}

    def register(self, domain: str, adapter: SiteAdapter) -> None:
        self._adapters[domain.lower()] = adapter

    def get(self, url: str) -> Optional[SiteAdapter]:
        domain = urlparse(url).netloc.lower()
        if domain in self._adapters:
            return self._adapters[domain]
        return None

    def ensure_generic(
        self,
        url: str,
        timeout_seconds: int,
        user_agent: str,
        search_url_template: Optional[str] = None,
        adapter_name: Optional[str] = None,
        adapter_config: Optional[Dict[str, Any]] = None,
    ) -> SiteAdapter:
        domain = urlparse(url).netloc.lower()
        if domain in self._adapters:
            return self._adapters[domain]
        if adapter_name == "playwright":
            adapter = PlaywrightRuleAdapter(
                base_url=url,
                timeout_seconds=timeout_seconds,
                user_agent=user_agent,
                search_url_template=search_url_template,
                rules=adapter_config,
            )
        else:
            adapter = GenericHtmlAdapter(
                base_url=url,
                timeout_seconds=timeout_seconds,
                user_agent=user_agent,
                search_url_template=search_url_template,
                rules=adapter_config,
            )
        self.register(domain, adapter)
        return adapter
