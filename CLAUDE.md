# V88 · Win 镜像主机约定（历史文件名保留；GPT/Codex接管）

> 本文件由 `win/遥控常驻V88.bat` 每轮启动时从 `win/CLAUDE-win.md` 复制成
> 仓库根的 `CLAUDE.md`（根 `CLAUDE.md` 在 StockAI 里被 .gitignore 排除，
> 无法直接入仓，故用这个入仓副本作为唯一真源）。
> **要改内容请改 `win/CLAUDE-win.md` 并 push**，改根 `CLAUDE.md` 下轮就被覆盖。

真实主程序：`app_v88_integrated.py`（Streamlit）。所有研发都是 **V88 迭代**。

## 你是谁

你运行在 **Win 镜像主机**（`DESKTOP-4H6ES39`）上。旧Claude手机遥控入口已退出现役，
本机只承担代码镜像和前端备用；V88核心维护、分析和发布验收由Mac上的GPT/Codex负责。

四端 = **Mac 桌面**（主战场，私密资产层）/ **云端 Actions**（流水线，7×24）/
**飞书**（推送）/ **Win 常驻主机**（你）。

## 命名铁律（最高优先级）

一切功能都写作 **「V88·功能名」** 或 V99.x 小版本；**禁止 V100+ 等新代号**。

## 本机的硬边界（最容易犯错的地方）

- **本机没有 `data/`**：整目录被 `.gitignore` 排除。`data/accounts.json`
  更是明文标注「永不进任何仓库」——**总资产 / 现金 / 八账户数据只存 Mac**。
  一切涉及仓位占比、斯波朗迪资金管理的判断，**必须说明「需回 Mac 计算」，绝不编数字**。
- **`positions.json` 在仓库根、未被忽略** → 持仓底稿会同步到本机，个股研究可以做。
- **本机不跑流水线**：云端 Actions 已全覆盖（日报 3 趟 + 盘中快扫每 15 分钟 + 轮动盘前）。
  双端同跑 = 重复推飞书 + 双写 `data/` 必冲突。
- 开工先读私仓 `AGENTS.md`、`docs/CODEX_TAKEOVER.md` 与升级记录；
  `claude-memory/` 只作历史审计。

## 决策口径（不可退让）

- 概率一律标注 **「规则情景估计（非回测胜率）」**，只代表方向占优程度，**不得冒充真实胜率**。
- 决策粒度 = **日**：像场外基金「今天买不买」，禁用价格推翻日期判断；
  区间制带宽 ≥3-5% 不纠结分厘；必须给明确动词 **买/卖/不动**，禁「观察」「评估减仓」。
- 个股前瞻用**交易日**阶梯 `HORIZON_DAYS = (5, 10, 20, 60, 120)`；
  系统统一评分 `evaluate_decision` 用**周口径**，别因为个股是「日」就去改它。
- 买卖决策 / 规则变更 / 推翻结论 → 必须走 **V88规则闸 + GPT/Codex独立复核**，
  缺复核要明确显示。规则阈值已冻结 v1（`src/rules_version.py`），改阈值须 bump 版本。

## Windows 编码铁律（每条都是实测炸出来的）

- **`.bat` 必须【全文件 ASCII】，包括注释**（2026-07-30 升级版铁律）。
  只把命令行改 ASCII 不够：Run 键（自动登录时机）演练下，`遥控常驻V88.bat` 被 cmd
  **逐行错切**——中文 `REM` 注释被当命令执行、`claude` 被截成 `aude`，bridge 起不来。
  双击不复现，只有登录时机的控制台码页会炸 ⇒ 不能靠「双击没事」验收。
  根因同下条：UTF-8 无 BOM + `chcp 65001`，cmd 按字节读行、在多字节字符上偏移错位。
- **`.bat` 命令行一律 ASCII**：UTF-8 无 BOM + `chcp 65001` 下中文会让 cmd
  截断命令行，实测致 `claude remote-control` → `exit 9009`（2026-07-30）。
- **`.ps1` 必须带 UTF-8 BOM**：否则 PowerShell 5.1 按 GBK 解码中文，
  直接 ParserError「意外的标记 }」（2026-07-30）。
- 中文 Windows 的 `%date%` 形如「周四 2026/07/30」，按 delims 切会把星期当年份 ——
  取日期走 `powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"`。

## 三方会谈（2026-08-16 恢复·Kimi 接替 Claude 席位）
- **3A 必须三方会谈**：GPT + Kimi + 经典书理 全通过才可执行/推送。
- **2A 及以上必须共同认可（22:32 用户定纲）**：GPT 和 Kimi 双双明确「通过」
  才算数——「不否定」「不可用」一律不算认可。
- **其他层级主动精选**：允许「不否定」，但 Kimi/GPT 每方主动「通过」的精选票
  最多 2 只（Kimi 优先从 GPT 未否决的票里挑），宁缺毋滥；禁止一排不否定或
  各推一堆各自为战；分歧以 ⚔️ 逐只显式点名。
- Kimi 裁决落盘 `data/kimi_verify.json`（verdict + book_verdict + ts），
  由 Mac 上 Kimi 定时任务产出：交易日 19:52 评审、周日 20:47 周报。
- 读取端：`recommendation_gate.kimi_review_for()`；推送闸 `assess_value/assess_trend`
  已接入（Kimi 当日正向复核是 push_eligible 的必要条件），飞书推送经 value_zone/
  trend_shift 的 push_eligible 自动遵守；桌面 `_v88_buy_gate9` 同款双剑+3A三方。
- 徽章三色：C=V88规则(金/绿)、G=GPT(青)、K=Kimi(紫)；无记录不显示。
- **核实通道（2026-08-17 00:49 用户定纲，取代此前插件授权）**：评审核实只用
  ①GPT 实时会审（codex CLI）②Kimi 独立判断（V88 冻结数据+书理）。
  **禁用 iFinD/Yahoo Finance 等第三方数据源作为裁决依据**。拿不到证据就给
  「不否定」并写明缺什么，不许编。
- **全池同步鉴定（00:49 用户定纲）**：每班对 market_pool 全池（2241只）逐只过规则，
  覆盖数不得低于 GPT 复核数；不能只看 V88 推送的候选。
- **GPT 实时会审通道（2026-08-16 深夜上线）**：本机 codex CLI（复用 ~/.codex 登录态）。
  Kimi 评审 3A 候选时现场 `codex exec` 问 GPT，裁决原样中转写 gpt_verify.json
  （via=kimi-codex-relay 标记，ts 带时分秒）；超时/失败自动降级回异步文件裁决。
  时效语义：ts 必须带时分秒走严格 24h 窗口；仅日期一律失效（23:07 用户裁定）。

## 装机与排障

见 `win/README-常驻.md`。日志在 `win\logs\remote_YYYYMMDD.log`。
