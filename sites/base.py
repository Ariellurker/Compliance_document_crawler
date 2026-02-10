"""
站点适配器基类与通用数据结构。

定义：
- SearchResult：搜索结果（标题、链接、发布时间）
- DetailInfo：详情页信息（标题、HTML、附件列表）
- SiteAdapter：抽象基类，约定 search() 与 fetch_detail_info() 接口
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional


@dataclass
class SearchResult:
    """单条搜索结果。"""

    title: str  # 标题
    url: str  # 链接
    publish_time: Optional[datetime]  # 发布时间（可为空）


@dataclass
class DetailInfo:
    """详情页解析结果。"""

    title: Optional[str]  # 页面标题
    html: Optional[str]  # 页面 HTML 内容
    attachments: List[str]  # 附件列表，元素为 dict {"url", "name"} 或 链接字符串


class SiteAdapter:
    def __init__(self, base_url: str, timeout_seconds: int, user_agent: str):
        """初始化站点适配器基础参数。"""
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent

    def search(self, file_name: str) -> Iterable[SearchResult]:
        """按文件名搜索站点，返回结果迭代器。"""
        raise NotImplementedError

    def fetch_detail_info(self, result: SearchResult) -> DetailInfo:
        """获取详情页信息与附件列表。"""
        return DetailInfo(title=result.title, html=None, attachments=[])
