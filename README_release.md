# 自动抓取程序操作指南（给使用人员）

本指南只讲“怎么用”，按步骤操作即可。

## 1) 先准备目录

在 `release` 目录中，确保下面文件存在：

- `launcher.exe`
- `config.yaml`
- `爬取信息总汇/需爬虫的网址汇总.xlsx`

## 2) 先改 Excel：`需爬虫的网址汇总.xlsx`

这份 Excel 决定“要搜什么、去哪个网站搜、以什么时间为起点判断是否更新”。

### 必须有 3 列

- `文件名`：搜索关键词（例如“证券公司合规管理办法”）
- `网址`：要爬的网站地址（例如 `https://www.csrc.gov.cn`）
- `发布时间`：你当前已知的发布时间（程序只下载比它更新的结果）

### 推荐模板

| 文件名 | 网址 | 发布时间 |
|---|---|---|
| 证券公司合规管理办法 | https://www.csrc.gov.cn | 2025-12-01 |
| 信息披露指引 | https://www.amac.org.cn | 2025/11/20 10:00:00 |

### Excel 填写注意

- `发布时间` 建议统一写成 `YYYY-MM-DD` 或 `YYYY-MM-DD HH:MM:SS`。
- 一行里只要有空值，或时间无法识别，这一行会被自动跳过。
- 不要在单元格里加额外说明文字（如“预计12月更新”）。

## 3) 再改配置：`config.yaml`

### 3.1 日常必改项（大多数用户只改这几项）

```yaml
excel_path: "./爬取信息总汇/需爬虫的网址汇总.xlsx"
download_root: "./downloads"
dry_run: false
```

- `excel_path`：Excel 路径；相对路径以 `release` 目录为基准。
- `download_root`：下载保存目录。
- `dry_run`：
  - `true` = 只测试检索，不下载文件。
  - `false` = 正常下载。

### 3.2 如果要“新增你们自己的网站”，重点改 `site_overrides`

程序能不能正确抓到某网站，关键看 `site_overrides` 里有没有该域名规则。

新增步骤：

1. 先确认 Excel 中 `网址` 的域名，例如 `https://abc.com/news` 的域名是 `abc.com`。
2. 在 `config.yaml -> site_overrides` 下新增 `abc.com` 的配置块。
3. `search_url` 必须包含 `{query}`（程序会把关键词替换进去）。
4. 先把 `dry_run` 设为 `true` 试跑，确认能搜到，再改回 `false`。

可复制模板（静态网站优先用 `requests`）：

```yaml
site_overrides:
  "abc.com":
    adapter: "playwright"
    search_url: "https://abc.com/search?q={query}"
    query_encoding: "single"
    fetch_mode: "requests"
    selectors:
      item: "div.result-item"
      title: "a.result-title"
      date: "span.date"
      wait_for: "div.result-item"
    detail_date:
      enabled: true
    detail_page:
      enabled: true
      fetch_mode: "requests"
      title_selectors:
        - "h1"
        - "title"
      attachment_selectors:
        - "a[href]"
      attachment_extensions:
        - "pdf"
        - "doc"
        - "docx"
        - "xls"
        - "xlsx"
```

字段含义（新增网站时最常用）：

- `search_url`：站内搜索地址模板，必须带 `{query}`。
- `query_encoding`：一般填 `single`，个别站点用 `double`。
- `fetch_mode`：
  - `requests`：页面是静态 HTML，速度更快。
  - `playwright`：页面是动态加载，用这个更稳。
- `selectors.item/title/date`：网页元素选择器，不对就抓不到。

## 4) 运行程序

1. 双击 `launcher.exe`。
2. 点击 `运行主流程（抓取下载）`。
3. 窗口日志中观察是否有“搜索到 X 条候选”“下载完成”等信息。

## 5) 怎么判断是否抓取成功

- 下载文件目录：`download_root`（默认 `./downloads`）。
- 成功记录：`./data/success.csv`。
- 失败记录：`./data/failures.csv`。
- 完整日志：`./data/run.log`（排错第一入口）。

## 6) 常见问题与处理

| 现象 | 处理方法 |
|---|---|
| 报错“Excel 缺少必要列” | 检查是否有“文件名 / 网址 / 发布时间”三列。 |
| 没有下载任何文件 | 先把 `dry_run` 设为 `true` 看能否搜到；再检查 Excel 里的 `发布时间` 是否写得过新。 |
| 某网站一直抓不到 | 检查该网站域名是否在 `site_overrides` 里，且 `search_url` 含 `{query}`。 |
| 日志有候选但下载失败 | 检查详情页选择器与附件选择器是否匹配当前网页结构。 |

## 7) 上传到飞书（可选）

如果需要上传到飞书，再填写 `uploader_config.yaml`：

```yaml
app_id: "你的 app_id"
app_secret: "你的 app_secret"
space_id: "你的知识库 space_id"
cloud_root_folder_token: "你的云空间目录 token"
local_root_path: "./downloads"
```

填好后在 `launcher.exe` 中点击 `运行本地同步（上传到飞书）`。
