from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional


@dataclass
class SearchResult:
    title: str
    url: str
    publish_time: Optional[datetime]


@dataclass
class DetailInfo:
    title: Optional[str]
    html: Optional[str]
    attachments: List[str]


class SiteAdapter:
    def __init__(self, base_url: str, timeout_seconds: int, user_agent: str):
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent

    def search(self, file_name: str) -> Iterable[SearchResult]:
        raise NotImplementedError

    def fetch_detail_info(self, result: SearchResult) -> DetailInfo:
        return DetailInfo(title=result.title, html=None, attachments=[])
