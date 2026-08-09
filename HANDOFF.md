# V88 StockAI 项目交接

> 2026-08-09预算规则已升级：基础7元＋重点复核3元＝DeepSeek硬上限10元。
> 后续AI修改模型、调用频率或预算显示前，先读 `docs/V88_AI_BUDGET_POLICY.md`；
> 本文较早章节出现的旧成本口径不再有效。

> 交接基准：2026-07-11 15:35（Asia/Shanghai）  
> 主项目：`/Users/bluesteven/Desktop/StockAI`  
> 权威日报项目：`/Users/bluesteven/Desktop/ai-daily-report-v2`  
> 本文是当前状态说明，不代表可以覆盖两个仓库中尚未提交的用户改动。

## 1. 项目最终目标

V88 的最终目标是一个面向个人投资研究的三端一致系统：

1. Mac 桌面端 V88 是主操作台，提供中美港市场概览、宏观体制、行业轮动、个股搜索、趋势/量价/基本面分析、扫描、自选股和持仓框架。
2. Streamlit 云端版是轻量、只读、全天在线的查看器，展示公开且已脱敏的数据，不在云端公开持仓。
3. 飞书负责定时日报、周报、盘中预警和健康告警；桌面、云端、飞书必须使用同一份权威日报、同一冻结快照和同一质量门禁。
4. 日报目标不是模仿媒体文风，而是达到专业机构的证据链：事实与观点分离、来源可点击、价格可复核、动作有门槛、低置信时允许没有推荐。
5. 系统是研究和纪律辅助工具，不是自动交易系统；LLM 负责叙事和归纳，确定性引擎负责候选、分数、价格和硬质检。

## 2. 当前版本及运行状态

### 2.1 桌面与云端仓库

- 路径：`/Users/bluesteven/Desktop/StockAI`
- GitHub：`bluestevenr-lang/stockai-v88`
- 分支：`main`
- 当前提交：`3f68039`（全局个股可点击，DataFrame 使用原生 `LinkColumn`）
- 代码版本常量：`modules/config.py` 中 `APP_VERSION = "88.0"`
- 主程序：`app_v88_integrated.py`，约 16,428 行，仍是大型单体 Streamlit 文件。
- 当前进程：PID `53169`，已运行约 5.5 小时。
- 启动命令：`python -m streamlit run app_v88_integrated.py --server.address 0.0.0.0 --server.port 8501 --server.enableCORS false --server.enableXsrfProtection false ...`
- 当前本机地址：`http://127.0.0.1:8501`，HTTP 200。
- 手机仍通过局域网 IP 的 `:8501` 访问。
- 云端入口：`https://stockai-v88.streamlit.app`。当前未登录请求会跳转 Streamlit 登录页，说明部署级访问控制正在生效。
- 云端公开数据：同仓库 `data` 分支的 `pub/` 目录；交接时最新数据提交为 `0e73ed8`。

### 2.2 权威日报仓库

- 路径：`/Users/bluesteven/Desktop/ai-daily-report-v2`
- GitHub：`bluestevenr-lang/v88-daily-report`（私有）
- 分支：`main`
- 当前提交：`3d6bca4`（日报/周报/飞书双时区标注）
- 报告协议：`v88.report/2.0`
- 快照协议：`v88.snapshot/2.0`
- 排名协议：`v88.rank/2.0`
- 当前本地权威快照：`snap-28d3f2c976d3f934`
- 当前 `report_manifest.json`：`quality.status = passed`、50 条来源、26 行操作榜、无质检问题。
- 当前日报、快照、排名三者 Snapshot ID 一致；日报 SHA-256 与 manifest 一致。
- 最新 GitHub Actions 日报/周报运行成功：2026-07-11 07:55 北京时间左右（GitHub 时间 `2026-07-10T23:55Z`）。

### 2.3 VPS

- 用户展示过一台 RackNerd VPS：Ubuntu 22.04、1 GB RAM、25 GB 磁盘、公网 IP `107.173.25.109`。
- 截图时服务器 Online，但本会话没有 SSH 凭据，也没有执行 VPS 部署。
- 项目当前生产云端仍是 Streamlit Community Cloud + GitHub Actions，不要把 VPS 视作已部署节点。

### 2.4 工作区状态

两个仓库都不是完全干净的工作区。严禁为了“清爽”执行 reset、checkout 或批量删除。

`StockAI` 当前存在已修改文件，包括：

- `.streamlit/config.toml`
- `config.py`、`config.toml`
- `main.py`、`modules/data_fetch.py`
- `prompts/sector_analysis.txt`
- `run_app.sh`、`v88_master_launch.sh`、`快速启动_V88.12.sh`
- `scan_worker.py`、`scanner.py`、`ts_helper.py`
- `test_dingtalk.py.bak`、`完整功能总结.md`

并存在多个未跟踪文件/目录，包括 `data/`、`scripts/`、`v100/`、若干备份文件和本地工具。它们可能是用户或其他会话的工作成果，不得删除或覆盖。

`ai-daily-report-v2` 当前未提交：

- `data/weekly_report.md`
- `watchlist_v88.json`

## 3. 已完成的功能

### 3.1 桌面 V88

- 三市场宏观脉搏、Risk On/Neutral/Risk Off、仓位上限及宏观代理指标。
- 中美港个股搜索、中文名/代码规范化、趋势脉搏、量价、MACD、均线、水位、基本面和期限剧本。
- 深度作战室、猎手战位、Top30、AI 选股、自选股分析。
- 动态自选股持久化到 `watchlist.json`，并同步副本到日报仓 `watchlist_v88.json`。
- 持仓、自选和常搜标的预警；持仓读取唯一权威 `positions.json`。
- 今日导航、重点推荐 Top3、重点观察触发、推荐理由和买法判定。
- 页面中的股票名称/代码支持全局点击和 `?q=` 深链；DataFrame 使用 Streamlit 原生链接列。
- LRU/文件缓存、并发取数、页面性能提示和手机响应式布局。
- 权威日报优先从日报仓读取，校验 manifest、Snapshot ID 和正文 SHA；失败时停止显示交易建议，不再由桌面端写第二套日报。

### 3.2 日报、云端与飞书统一协议

- `report_contract.py` 建立统一报告协议和硬质检。
- 日报生成前冻结市场快照，排名、日报、云端和飞书绑定同一个 Snapshot ID。
- 操作榜由确定性引擎生成，LLM 不参与选股与报价。
- 允许每个市场/期限 0-3 只，不再为了版式强凑 Top3。
- 评分阈值：短线 60、中线 62、长线 62；建仓 75；跟进 68。
- “买入/建仓”必须同时满足：评分不低于 75、72 小时内直接个股催化、来源为 Tier A/B、存在原文链接。
- 无直接催化时自动降级为“中期跟进”或“观察”；观察标的不输出买入/目标价。
- `price_guard.py` 使用 yfinance 实价覆盖操作榜价格；非买入动作只显示跟踪区或现价。
- 来源台账最多收录 50 条，并按 Tier A/B/C 分类；硬门禁要求至少 6 条带链接来源和至少 3 条 A/B 来源。
- 报告正文、manifest、冻结快照和排名不一致时，云端及飞书均阻断交易建议。
- 公开版报告自动删除所有 `## 💼` 持仓段，并为脱敏后的正文重新计算公开版 SHA。
- 云端旧协议不再兼容展示交易建议；缺 manifest 时只允许查看实时行情。
- 飞书正文支持可靠长消息拆分，默认单段上限 3,500 字符。
- 飞书卡片与盘中预警使用 `data/push_state.json` 做 48 小时跨渠道去重。
- 日报、周报和飞书落款同时显示北京时间与美东时间，自动处理夏令时及跨日。

### 3.3 自动化与成本控制

- 私仓日报：北京时间 07:00、14:00、21:00；周末改发周报，`holidays.txt` 命中则跳过。
- 公共仓实时层：每小时第 7 分更新行情快照和新闻流到 `data` 分支。
- Top30 云端扫描：北京时间 09:00、15:00、21:00、03:00。
- 量化模拟：A/H 与美股各时段定时运行。
- 健康检查和盘中预警已迁到公共仓运行，以节省私仓 Actions 分钟。
- 新闻翻译已限制为少量固定时段和批量，目标月成本不超过用户明确红线约 ¥3；不得随意扩大翻译频率或批量。
- VPS 到期提醒逻辑已加入 `health_check.py`，计划在 2027-02-25、03-09 预告，03-11 确认切换全免费模式。

## 4. 修改过的重要文件及作用

### 4.1 `/Users/bluesteven/Desktop/StockAI`

| 文件 | 作用 |
|---|---|
| `app_v88_integrated.py` | 桌面端主程序、所有主要 UI、扫描、个股分析、宏观面板、权威日报读取。 |
| `streamlit_app.py` | Streamlit 云端查看器；校验报告质量、冻结快照和公开版 SHA；展示来源链接和深链。 |
| `cloud_engine.py` | 桌面/云端共享趋势、三期限评分、中文名映射、基本面和操作逻辑。 |
| `src/market_snapshot.py` | 公共仓实时快照生成，输出 `v88.snapshot/2.0` 和 Snapshot ID。 |
| `live_publish.py` | 每小时新闻/行情发布到公共仓 `data` 分支，并记录 `live_snapshot_id`。 |
| `modules/config.py` | `APP_VERSION=88.0`、模块级缓存及模型配置。 |
| `config.toml` | 当前运行参数：缓存、并发、超时、VIX/利率/美元阈值。 |
| `v88_master_launch.sh` | 当前主要启动脚本；加载 `.env`，以 `0.0.0.0:8501` 启动。 |
| `.github/workflows/live.yml` | 每小时实时数据。 |
| `.github/workflows/intraday.yml` | 公共仓盘中即时预警；当前有依赖缺失故障。 |
| `.github/workflows/health.yml` | 每 6 小时健康检查。 |
| `.github/workflows/scan.yml` | Top30 云端扫描。 |
| `.github/workflows/quant.yml` | 量化模拟任务。 |
| `tests/page_smoke.py` | 真实执行完整 Streamlit 脚本的页面级冒烟测试。 |

### 4.2 `/Users/bluesteven/Desktop/ai-daily-report-v2`

| 文件 | 作用 |
|---|---|
| `src/run_daily_report.py` | 日报总流水线：新闻→分析→冻结快照→排名→写稿→价格护栏→持仓/战绩→质检→公开发布→飞书。 |
| `src/report_contract.py` | 三端统一协议、来源台账、Snapshot/SHA/动作规则硬质检。 |
| `src/horizon_rank_cloud.py` | 三期限确定性排名、门槛、催化匹配、方向和买法。 |
| `src/price_guard.py` | 实价校准和观察/跟进/买入价位格式。 |
| `src/report_generator.py` | DeepSeek/Gemini 报告叙事提示词及报告结构。 |
| `src/market_snapshot.py` | 日报冻结快照生成。 |
| `src/publish_public.py` | 持仓脱敏、公开版 manifest 重算、原子发布到 `stockai-v88:data/pub`。 |
| `src/feishu_push.py` | 飞书摘要卡片、正文拆分、质量门禁、48 小时去重和双时区。 |
| `src/intraday_alert.py` | 盘中触发和共享去重状态。 |
| `src/weekly_report.py` | 周末深度周报和飞书推送。 |
| `src/health_check.py` | 数据/任务健康检查和 VPS 到期提醒。 |
| `FRAMEWORK.md` | 持仓分层、集中度、再平衡和纪律的最高业务规则。 |
| `positions.json` | 四账户持仓的唯一权威数据，禁止发布到公开分支。 |
| `watchlist_v88.json` | 从桌面同步来的自选/搜索关注池。 |
| `.github/workflows/report.yml` | 日报/周报定时任务及数据回仓。 |
| `tests/test_report_contract.py` | 报告协议、催化门槛、消息拆分、公开版 SHA 等单元测试。 |

## 5. 已确认且不能随意改变的规则

1. **不要收紧或改动局域网访问方式。** 用户明确删除了“增加访问令牌、恢复跨站保护”这一改进项。保持手机访问；不要擅自修改 `0.0.0.0`、CORS、XSRF 或加 LAN token，除非用户重新明确授权。
2. **不要丢失现有工作区改动。** 禁止 `git reset --hard`、`git checkout --`、批量清理未跟踪文件或覆盖用户文件。
3. **桌面、云端、飞书必须同源。** 不允许桌面端失败后另生成一份口径不同的 AI 日报。
4. **LLM 不负责确定选股和报价。** 候选、得分、现价、动作门槛由确定性代码给出；LLM 只写叙事、事实归纳和催化解释。
5. **无机会可以空缺。** 不能为了“每市场 Top3”强行推荐。
6. **买入必须可核验。** 不满足 75 分、72 小时直接催化、Tier A/B、原文链接，不能写买入/建仓。
7. **观察不输出交易指令。** 观察标的不能出现买入区、目标价或建仓措辞。
8. **公开云端不得含持仓。** `positions.json`、组合体检、持仓建议仅限桌面和飞书私域。
9. **质量失败宁可不发。** 质检失败时保留云端上一版有效日报，并阻止飞书正文发送。
10. **所有时间以明确时区展示。** 流水线判断使用北京时间；报告和飞书显示北京 + 美东 ET，不能再使用无时区的裸 `datetime.now()`。
11. **48 小时去重是跨渠道共享规则。** 飞书卡片和盘中预警不得各发一遍同一信号。
12. **持仓框架高于临时观点。** `FRAMEWORK.md` 的三层一池、主题集中度、涨 50% 减 1/3、短线风险和再平衡规则不能被 AI 文案覆盖。
13. **成本红线。** 不随意扩大 DeepSeek 新闻翻译次数、批量或定时任务频率；月成本目标约 ¥3 以内。
14. **密钥只从环境/Secrets 读取。** 不把任何 API key、Webhook、Gist token 或 GitHub token写入代码、日志、HANDOFF 或公开分支。

## 6. 当前数据来源、接口、路径和关键参数

### 6.1 行情和基本面

- 主行情/指数/行业/基本面：`yfinance` / Yahoo Finance。
- A 股辅助和名录：Tushare，密钥名 `TUSHARE_TOKEN`。
- 可选回退：Alpha Vantage，密钥名 `ALPHA_VANTAGE_KEY`。
- 桌面扫描结果可通过 GitHub Gist 同步，使用 `GIST_ID`，私密或提额读取时使用 `GIST_TOKEN`。
- 桌面端 DeepSeek OpenAI 兼容接口：`https://api.deepseek.com/v1`。

### 6.2 新闻 RSS

配置：`/Users/bluesteven/Desktop/ai-daily-report-v2/config/rss_sources.json`。

当前 12 个源：MarketWatch、CNBC Top News、CNBC Finance、FT Markets、Yahoo Finance、Investing.com、36氪、华尔街见闻、SCMP Business、Google News 中文市场、WSJ Markets、Google News EN Markets。

关键参数：

- `fetch_timeout = 10s`
- `max_retries = 2`
- `max_news_age_hours = 72`
- 本机默认外媒代理：`RSS_PROXY=socks5h://127.0.0.1:40000`；GitHub Actions 显式设为空，使用美国节点直连。
- 直接个股催化有效期：不超过 72 小时。

### 6.3 模型

- 桌面主模型标签：`deepseek-v4-flash`。
- 日报新闻分析/写稿默认模型：`deepseek-chat`，由 `DEEPSEEK_ANALYSIS_MODEL`、`DEEPSEEK_REPORT_MODEL` 覆盖。
- 日报 provider：`ANALYSIS_PROVIDER=deepseek`、`REPORT_PROVIDER=deepseek`。
- Gemini 回退：`gemini-2.5-flash`，使用 `GEMINI_API_KEY`。
- 不要在文档或日志中记录密钥值。

### 6.4 报告及公开数据路径

- 权威日报：`ai-daily-report-v2/data/daily_report.md`
- 周报：`ai-daily-report-v2/data/weekly_report.md`
- 冻结快照：`ai-daily-report-v2/data/market_snapshot.json`
- 排名：`ai-daily-report-v2/data/engine_rank.json`
- 质量清单：`ai-daily-report-v2/data/report_manifest.json`
- 来源台账：`ai-daily-report-v2/data/source_ledger.json`
- 推送去重：`ai-daily-report-v2/data/push_state.json`
- 复盘：`ai-daily-report-v2/journal/*.json`
- 公开读取基址：`https://raw.githubusercontent.com/bluestevenr-lang/stockai-v88/data/pub`
- 公开冻结快照：`pub/report_snapshot.json`
- 小时实时快照：`pub/market_snapshot.json`，可比日报冻结快照更新得更快。

### 6.5 关键量化和运行参数

- 短/中/长最低分：60 / 62 / 62。
- 建仓分：75；跟进分：68。
- 候选：每期限每市场 0-3 只。
- 买入催化：72 小时、Tier A/B、有 URL。
- 飞书单段：默认 3,500 字符。
- 飞书/盘中预警去重：48 小时。
- 桌面缓存：当前 `config.toml` 中除 `ttl_fast=900s` 外，主要缓存均为 3,600s；并发 `max_workers=12`，单任务超时 15s，请求超时 10s。
- VIX：15/20/30；10Y 美债宽松/紧缩：3.5/4.5；DXY 弱/强：100/105。

### 6.6 Secret 名称

本地或 GitHub Secrets 使用以下名称，交接只记录名称：

`DEEPSEEK_API_KEY`、`GEMINI_API_KEY`、`TUSHARE_TOKEN`、`ALPHA_VANTAGE_KEY`、`NEWS_API_KEY`、`GIST_ID`、`GIST_TOKEN`、`FEISHU_WEBHOOK`、`FEISHU_APP_ID`、`FEISHU_APP_SECRET`、`PUBLISH_TOKEN`、`PRIVATE_TOKEN`、`DINGTALK_WEBHOOK`、`DINGTALK_SECRET`、`DINGTALK_KEYWORD`。

## 7. 已验证成功的内容

1. 桌面 V88 当前进程存在，`127.0.0.1:8501` 返回 HTTP 200。
2. 桌面页面此前实际验证过：宏观标签、A/H 指数代理、仓位上限、权威日报和操作榜可正常渲染。
3. 最新权威日报本地 manifest 为 `passed`，Snapshot ID 和 SHA 校验通过。
4. 公共 `data` 分支实测：日报、manifest、report snapshot、source ledger、meta 全部存在。
5. 公共日报 SHA 与公开 manifest 一致，公开日报与 report snapshot 的 Snapshot ID 一致。
6. 公开日报中 `## 💼` 段落数量为 0，确认没有发布持仓段；manifest 标记 `visibility=public_redacted`。
7. 公共来源数为 50。
8. 最新私仓“V88 市场日报/周报 → 飞书”Action 成功。
9. 最新公共仓 `live-data` 与健康检查 Action 成功。
10. 报告协议曾通过 7 项单元测试；测试文件仍在 `tests/test_report_contract.py`。
11. `tests/page_smoke.py` 已建立完整页面冒烟测试，但本次交接只做检查，没有重新执行它。

## 8. 尚未解决的问题和已知风险

### P0：盘中即时预警工作流当前失败

- 公共仓 `.github/workflows/intraday.yml` 只安装 `yfinance pandas requests`。
- 私仓 `src/intraday_alert.py` 导入 `feishu_push.py`，后者需要 `python-dotenv`。
- 最近失败日志：`ModuleNotFoundError: No module named 'dotenv'`。
- 最近至少 3 次定时盘中预警失败；这是下一阶段最优先修复项。
- 最小修复应是在公共仓该 workflow 的依赖安装中加入 `python-dotenv`，随后手动触发验证。未经新会话确认，不要在本交接任务中修改。

### P1：买入方向字符串导致计划拼接条件失效

- `horizon_rank_cloud.py` 现在将方向写成 `买入/建仓（现价分批/回踩买/突破跟）`。
- 同函数后续仍使用精确判断 `direction == "买入/建仓"` 才追加期限计划，因此带括号的新方向不会进入该分支。
- `price_guard.py` 仍会给价位，但“依据”列可能缺少原设计的期限交易计划。
- 应改为 `direction.startswith("买入/建仓")` 或以动作枚举与显示文本分离，并增加单元测试。

### P1：主程序单体过大

- `app_v88_integrated.py` 超过 16,000 行，启动和回归成本高，重复函数/旧兼容代码仍多。
- 不宜一次大拆；应先为权威日报、导航、个股深链和宏观面板建立稳定测试，再按边界渐进迁移。

### P1：运行配置存在双源不一致

- `.streamlit/config.toml` 当前显示 `enableCORS=true`、`enableXsrfProtection=false`。
- 实际启动脚本通过命令行覆盖为 CORS false、XSRF false。
- 这是已知配置不一致，但局域网安全项是用户明确要求不改的内容；只能记录，不能擅自“修复”。

### P1：本地 `pipeline_summary.json` 不是云端真实状态

- 本地 summary 仍记录旧快照和 `FEISHU_WEBHOOK_missing`，是本地未配置 Webhook 的历史运行结果。
- GitHub Actions 最新日报任务成功且有 Secrets；不要据本地 summary 推断生产飞书失效。

### P2：数据源固有限制

- yfinance 会限流、返回延迟行情或指数零成交量；部分 A/H 指标是 ETF 代理，必须继续明确标注。
- RSS 中 Google News、36氪等属于聚合/二手来源，不能提升为 Tier A/B。
- 公司名标题模糊匹配可能产生催化误关联；优先依赖 `affected_tickers`，模糊匹配应继续收紧。
- 当前操作榜仍基于有限候选池，不是全市场逐股扫描。

### P2：部署和基础设施

- Streamlit 云端当前要求登录；是否符合最终分享范围需要用户确认，但不要自行更改访问权限。
- RackNerd VPS 尚未部署、未加固、未接入域名/HTTPS/备份/监控；1 GB 内存不适合直接承载完整桌面单体和高并发扫描。
- 公共仓从私仓拉取代码依赖 `PRIVATE_TOKEN`；令牌失效会同时影响健康检查和盘中预警。

### P2：仓库卫生和回归

- 两仓均有未提交数据或用户改动，后续操作容易误提交/误覆盖。
- 当前最新桌面提交之后，本次交接未重新运行 `tests/page_smoke.py`。
- 应避免把 `.env`、positions、缓存、备份或本地搜索历史加入公开提交。

## 9. 下一阶段改进项目（按优先级）

1. **P0 修复公共仓 intraday 依赖**：加入 `python-dotenv`，手动触发 workflow，确认飞书、`alert_state.json` 和 `push_state.json` 回写成功。
2. **P0 增加 workflow 最小依赖测试**：公共仓 CI 至少 import `intraday_alert`、`feishu_push`，避免定时运行才发现缺包。
3. **P1 修复买入计划判断**：动作使用结构化字段，显示文本与逻辑判断分离；覆盖带买法后缀的测试。
4. **P1 运行完整回归**：日报仓单测、Python 编译、StockAI `tests/page_smoke.py`、桌面/移动视口及云端旧协议/新协议门禁。
5. **P1 收紧催化映射准确率**：优先 ticker 映射，模糊名称匹配增加边界、别名和冲突检查。
6. **P1 建立报告可观测性**：在健康检查中验证最新 manifest、SHA、Snapshot、来源数、报告年龄和飞书最后成功时间。
7. **P2 渐进拆分桌面单体**：先抽取权威日报 UI、宏观面板、深链路由和公共表格渲染；每步保持页面冒烟测试通过。
8. **P2 统一配置读取**：减少 `modules/config.py`、`config.toml`、`.streamlit/config.toml` 和启动参数的重复，但局域网访问行为不得改变。
9. **P2 清理工作区**：只能在用户逐项确认后整理备份、未跟踪工具和 v100，不得自动删除。
10. **P3 VPS 方案评估**：若用户决定迁移，先做容量评估、非 root 账户、SSH key、UFW、Caddy/Nginx HTTPS、systemd、备份与回滚方案；不要直接把当前单体裸露在公网。

## 10. 新会话需要首先读取的文件

按以下顺序读取：

1. `/Users/bluesteven/Desktop/StockAI/HANDOFF.md`
2. 两个仓库的 `git status -sb` 和最近 10 条 `git log --oneline`
3. `/Users/bluesteven/Desktop/ai-daily-report-v2/FRAMEWORK.md`
4. `/Users/bluesteven/Desktop/ai-daily-report-v2/src/report_contract.py`
5. `/Users/bluesteven/Desktop/ai-daily-report-v2/src/run_daily_report.py`
6. `/Users/bluesteven/Desktop/ai-daily-report-v2/src/horizon_rank_cloud.py`
7. `/Users/bluesteven/Desktop/ai-daily-report-v2/src/price_guard.py`
8. `/Users/bluesteven/Desktop/ai-daily-report-v2/src/feishu_push.py`
9. `/Users/bluesteven/Desktop/StockAI/.github/workflows/intraday.yml`
10. `/Users/bluesteven/Desktop/ai-daily-report-v2/.github/workflows/report.yml`
11. `/Users/bluesteven/Desktop/StockAI/streamlit_app.py`
12. `/Users/bluesteven/Desktop/StockAI/cloud_engine.py`
13. `/Users/bluesteven/Desktop/StockAI/app_v88_integrated.py` 中：权威日报加载、宏观面板、`?q=` 深链和 AI 市场简报相关区域；不要一开始通读全部 16k 行。
14. `/Users/bluesteven/Desktop/StockAI/tests/page_smoke.py`
15. 两边 `.env` 只核对 key 是否存在，不读取、打印或转述 value。

## 11. 给下一个 Claude Fable 5 的完整执行指令

```text
你正在接手 V88 StockAI。先不要修改任何文件。

主仓：/Users/bluesteven/Desktop/StockAI
日报仓：/Users/bluesteven/Desktop/ai-daily-report-v2

第一步必须完整阅读主仓 HANDOFF.md，然后分别运行 git status -sb、git log -10 --oneline。两个工作区都有用户/其他会话遗留改动，不得 reset、checkout、clean、删除备份或覆盖未提交内容。只处理本次用户明确授权的范围。

必须遵守这些硬规则：
1. 不修改局域网访问方式，不加访问令牌，不调整 0.0.0.0/CORS/XSRF，除非用户重新明确授权。
2. 桌面、云端、飞书必须读取同一权威日报、冻结快照和质量清单。
3. LLM 不得决定候选和价格；无达标机会允许空缺。
4. 买入必须满足评分>=75、72小时内直接个股催化、Tier A/B、原文 URL；观察不得给交易价位。
5. 公开云端绝不包含 positions/持仓段；失败时保留上一版有效公开日报。
6. 时间统一使用北京时间做任务判断，展示同时标注北京和美东 ET。
7. 飞书卡片与盘中预警共享 48 小时去重状态。
8. 不扩大新闻翻译频率/批量，月成本目标约 ¥3。
9. 不打印或提交任何密钥、Webhook、令牌。

接手后的首要技术任务（仅在用户授权修改后执行）：
A. 修复 /Users/bluesteven/Desktop/StockAI/.github/workflows/intraday.yml 缺少 python-dotenv，先做最小改动。
B. 手动触发公共仓 V88 盘中即时预警，确认不再出现 ModuleNotFoundError，并确认私仓去重状态回写；发送真实飞书消息前确认任务本身已获用户授权。
C. 修复 horizon_rank_cloud.py 中带买法后缀的 direction 与精确字符串比较不一致问题，把逻辑动作与显示文本分离，并补测试。

每次修改前先说明将改哪些文件。使用 apply_patch 做手工编辑。不要顺手重构 16k 行主文件。

最低验证要求：
- 日报仓：python3 -m unittest discover -s tests -v
- 两仓相关 Python：python3 -m py_compile <本次涉及文件>
- Shell：bash -n <本次涉及脚本>
- 主页面：python3 tests/page_smoke.py（允许较慢）
- 报告：manifest quality=passed；日报 SHA、report snapshot、rank snapshot 三者一致
- 公开版：无“## 💼”，公开正文 SHA 与公开 manifest 一致
- GitHub Actions：检查最新 run 的 conclusion 和失败日志

提交/推送规则：只暂存本次涉及文件，不夹带现有 dirty 文件。推送、部署、触发飞书或修改 GitHub Secrets 前，确认用户已经授权对应外部动作。完成后报告提交号、验证结果、仍存风险；不要声称未运行的测试已通过。
```
