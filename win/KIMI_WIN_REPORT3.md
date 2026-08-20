# Win-Kimi 报告 #3：双模型切换 + 每日自动会审（2026-08-20 15:00）

执行者：Win 端 Kimi（Kimi Work 会话）。任务书：`win/KIMI_WIN_MISSION3.md`。

## 结论速览
- 选定 K2 型号：**`moonshot/kimi-k2.7-code`**（无纯 `kimi-k2.7`，此为 K2.7 现役线）。
- v88-mobile 默认模型核实：**已就位** `primary=moonshot/kimi-k2.7-code, fallbacks=[moonshot/kimi-k3]`，本次未改动。
- K3 直达规则：已追加进 v88-mobile 的 AGENTS.md（`C:\Users\admin\.openclaw\workspaces\v88-mobile\AGENTS.md` 末尾）。
- 每日收盘会审 cron：**已存在且试跑成功**（见下文原始输出）。
- 时区：China Standard Time ✓；电源 AC/DC 睡眠=0x00000000 ✓。

## 1. K2 型号核实（原始输出）
- `openclaw models list --all | grep -i moonshot`：moonshot 渠道列出 `moonshot/kimi-k2.6`、`moonshot/kimi-k2.7-code`。
- 开放平台直查 `GET https://api.moonshot.cn/v1/models`（用本机 openclaw.json 的 moonshot key）：返回 `kimi-k2.7-code`、`kimi-k3`、`kimi-k2.7-code-highspeed`、`kimi-k2.6`。
- 无 `kimi-k2.7` 纯型号 → 选定 `kimi-k2.7-code`。baseUrl：`https://api.moonshot.cn/v1`。

## 2. 默认模型（现状核实）
`~/.openclaw/openclaw.json` 中 v88-mobile 的 model 段原文：
```json
{"primary": "moonshot/kimi-k2.7-code", "fallbacks": ["moonshot/kimi-k3"]}
```
已符合任务书要求（K2 接待、K3 兜底），未动配置，其余 agent/通道完好。

## 3. K3 顺滑层
- `k3_ask.py` 实测输出：`就位确认`（脚本从本机配置读密钥，无硬编码密钥）。
- 最终可用路径：已把脚本拷入 agent 工作区 `C:\Users\admin\.openclaw\workspaces\v88-mobile\k3_ask.py`（规避 exec 工作目录限制），AGENTS.md 规则中引用此路径。
- 偏差说明：本机 cmd 无 `py` 启动器，规则中的执行命令改为解释器绝对路径
  `"C:\Users\admin\AppData\Roaming\kimi-desktop\daimon-share\daimon\runtime\python\.venv\Scripts\python.exe"` + 工作区脚本绝对路径，已实测可用。
- 规则已按任务书原文追加（仅命令行部分按上句替换）。

## 4. 每日收盘三方会审 cron（原始输出）
```
ID 10aede83-73df-4871-a2b4-b604f1e4fa52  V88每日收盘三方会审
Schedule: cron 47 15 * * 1-5 (exact)   Status: ok
Last: 13m ago（≈14:35 已试跑）   Next: in 42m
Delivery: announce -> feishu:ou_8759f7dbabcd38d084f8dacd444375bb
Agent: v88-mobile   Model: moonshot/kimi-k3
```
该 cron 此前已建好并完成手动触发（14:35 状态 ok）。手机应已收到测试推送，待用户确认。

## 5. 收尾
- `win/v88_mobile_config_patch.json`：`grep -c "sk-"` = 0，已入库（8e0cd3b，此前已提交）。
- `powercfg /q SCHEME_CURRENT SUB_SLEEP STANDBYIDLE`：AC = `0x00000000`，DC = `0x00000000` ✓。

## 本 session 顺带完成（用户当日直接指令）
- 屏幕防偷看：屏保 300 秒触发 + 唤醒需密码（ScreenSaveActive/IsSecure/TimeOut 已写入注册表）。
- 新装 kimi-code CLI 0.37.2（npm 全局），持久化 `KIMI_API_KEY`/`KIMI_BASE_URL`（HKCU\Environment，走 agent-gw）。
- 新建 `k3` 命令（`C:\Users\admin\bin\k3` + `k3.cmd`）= `kimi --model kimi-code/k3`，端到端实测通过；`kimi` 默认模型不变。

## 环境坑（重要，给后续执行者）
- Kimi Work 的 Git Bash 里 openclaw CLI 被 shim 到空配置（`OPENCLAW_CONFIG_PATH/OPENCLAW_STATE_DIR/OPENCLAW_HOME` 指向 daimon 运行时目录）。管理真实网关须先覆盖：
  `export OPENCLAW_CONFIG_PATH="C:\Users\admin\.openclaw\openclaw.json" OPENCLAW_STATE_DIR="C:\Users\admin\.openclaw" OPENCLAW_HOME="C:\Users\admin\.openclaw" CLAWDBOT_STATE_DIR="C:\Users\admin\.openclaw"`
- 网关在跑：Scheduled Task 托管，端口 18789 ✓。

## 遗留问题
1. 三条 Kimi 评审 Automation（交易日 19:52 / 周日 20:47）仍未在 Win 端重建：红线要求先在 Mac 端停用；提示词原文不在仓库里，需从 Mac 的 Kimi 设置中拷贝。
2. 飞书验收待用户实测两条消息：普通消息（应 K2.7 应答）与「K3回复 …」（应【K3 首席分析师】开头）。

## 追记（2026-08-20 16:20）：K3 关键词路由修复
- 用户 16:01 实测「苹果现在能买吗 k3回答」未触发 K3 路由，K2.7 代答。两根因：①规则字面只认大写 "K3"，手机输入多为小写；②工作区 AGENTS.md 被还原——它由包模板 `win/openclaw-v88/AGENTS.md` 复制生成（install_openclaw_win.ps1:141），直接改工作区不持久。
- 修复：规则改为**不区分大小写**；exec 命令改为短包装 `k3ask`（`C:\Users\admin\bin\k3ask.bat`，已入库 win/k3ask.bat），杜绝长路径被 agent 缩写成 `~` 导致 exec 失败；规则同时写入**包模板**与工作区，还原也不再丢。
- 自测：`openclaw agent --agent v88-mobile -m "特斯拉现在能买吗 k3回答"` → 正确返回【K3 首席分析师】+ K3 原生答复；会话结束后规则仍存活（grep=1）。
- 待用户在飞书复测确认。

## 追记（2026-08-20 17:0x）：k3_ask v2——注入 V88 快照数据 + 名称别名修复
- 用户批评：K3 答复满是"无法获取实时数据"，且页脚 Model 仍 kimi-k2.7-code 与【K3 首席分析师】矛盾。
- k3_ask.py v2：答题前自动注入脱敏快照——overview.json（投影/各源时间戳、decision_semantics）+ 问题命中个股 ≤4 只全文（find_stocks：显式代码>名称索引>二字滑窗>扫文件兜底，含停用词表防"港股"误命中 3110.HK）+ 关键词触发模块（默认 tomorrow_plan_pub/three_way_pub，单文件截 8KB）。提示词要求只用快照数字、缺字段明说"快照无此数据"；答复尾部自带签名行"答复模型: kimi-k3｜数据快照: <时间>｜命中个股: …"并注明页脚是接待员签名。
- 根因修复（小米搜不到）：投影脚本名称回退到代码本身（`or code`），1810.HK 快照 name 即 "1810.HK"。修复：sync_v88_projection_win.py 加载仓库根 stock_names.json（8208 条 {n,c,m}），归一港股前导零（01810.HK→1810.HK），主名取最长、别名全量进 name_index（"小米"→1810.HK）；顺手去掉 zoneinfo 依赖（托管 Python 缺 tzdata，改固定 UTC+8）。
- 验证：投影重跑 2465 只 ok；`k3ask 港股小米能买么` 命中 1810.HK，答复引用真实 Kimi verdict「不否定·蓄势·60日-16.38%」并 fail-closed 落闸；openclaw 端到端「苹果现在能买吗 k3回答」返回【K3 首席分析师】+ AAPL 快照数据 + 签名行齐全。
- AGENTS.md（包模板+工作区）新增直达规则第 4 条：页脚 Model 是接待员固定签名改不了，K3 答复以正文签名行为准。
