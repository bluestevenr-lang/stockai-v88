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
