"""
站点适配器模块对外导出。

提供：
- SiteRegistry：按域名管理并创建适配器
- GenericHtmlAdapter：通用 HTML 解析适配器
- PlaywrightRuleAdapter：基于 Playwright 与规则配置的适配器
"""

from .registry import SiteRegistry
from .generic import GenericHtmlAdapter
from .playwright_rule import PlaywrightRuleAdapter

__all__ = ["SiteRegistry", "GenericHtmlAdapter", "PlaywrightRuleAdapter"]
