# Win Codex 紧急任务：恢复今天下午建立的 GPT OpenClaw

> 下达时间：2026-08-24 22:12（北京时间）  
> 执行主机：`DESKTOP-4H6ES39`  
> 执行者：Windows 上的 Codex；不要交给旧 K3 会话代答。  
> 用户现象：今天下午建立的 GPT OpenClaw 显示安装完成，但从飞书发消息没有回复。

## 边界

- 本任务只诊断和恢复**今天下午的 GPT/OpenAI OpenClaw**，不要把 8 月 20 日的 K3 验收当作当前状态。
- 不改 V88 引擎、评分、推荐或交易纪律；不碰持仓明文。
- 不输出或提交 App Secret、OAuth token、API key、完整 App ID。日志中只保留布尔状态、模型名和 App ID 后 6 位。
- 不新增付费 API，不切换到 Kimi。若 GPT 凭据或飞书凭据缺失，只报告缺口和唯一必要动作。
- 用户已授权进行一次恢复后的飞书测试消息；不要反复发测试。

## 一、先找“真正正在运行的那套配置”

不要假定交互终端的 `OPENCLAW_*` 环境变量正确。先从以下三处交叉确认：

1. `OpenClaw Gateway` 计划任务的 Action、运行账户、State、LastTaskResult；
2. 正在运行的 `node.exe` / `openclaw` 进程命令行；
3. `C:\Users\admin\.openclaw\gateway.cmd` 实际引用的 config/state/home。

随后显式设置真实环境再运行 CLI；避免 Kimi/Codex 临时运行时把 CLI 指到空配置。严禁在报告中打印凭据值。

## 二、把链路逐段验真

请保存原始但已脱敏的结果：

1. `openclaw --version`
2. `openclaw status --deep`
3. `openclaw channels status --probe`
4. `openclaw agents list --json`
5. `openclaw models list`，以及当前版本支持的 provider/auth 状态命令
6. `openclaw config validate`
7. 今天 14:00 以后 Gateway 日志中与 `feishu`、`received message`、`dispatching`、`model`、`auth`、`401/403/404/429`、`unavailable`、`error` 有关的行

必须明确回答以下问题：

- 网关有没有常驻；若没有，是任务没跑、立刻退出，还是进程被旧实例占端口？
- 飞书是否 `enabled + configured + running + connected + works`？
- GPT agent 是否真的绑定到 `feishu:default`？
- 当前发信人的配对是否仍批准？
- 消息有没有进入网关？若进入，停在分派、模型调用还是回信阶段？
- GPT OAuth 是否在**这台 Win 的真实 OpenClaw state** 中有效？
- 当前模型 ID 是否出现在本机 provider 实际可用列表；不要只看配置文件写了什么。
- 用户发的是普通文字还是语音。若文字可回、语音不回，要单列为“语音转写能力缺失”，不要误判为网关故障。

## 三、已知高概率断点（必须现场验证，不得直接照抄结论）

仓库中的 GPT 安装器目前存在两点：

1. `install_openclaw_win.ps1` 安装结束时故意保留“Win 飞书未启用”，需后续单独切换；
2. 安装器写死 `openai/gpt-5.6-sol`，而此前另一台机器出现过 provider unavailable。

因此优先检查“通道根本没启用”和“模型 ID 对当前 provider 不可用”，但最终结论必须以 Win 当前命令与当天日志为准。

## 四、允许的恢复动作

只在证据支持时执行：

- 修复计划任务并重启 Gateway；
- 若飞书凭据已存在，启用插件/通道并把 GPT agent 绑定到 `feishu:default`；
- 批准现有用户的待配对请求；
- 若当前 GPT 模型不可用，只能从本机 `models list/auth status` 明确列出的 OpenAI/Codex 可用模型中选择，不得猜模型名；
- 重启后运行 probe，并从 Win 主机向已配对用户只发一次：`Win GPT OpenClaw 已恢复，文字链路测试成功。`

若 App Secret 或 GPT OAuth 确实不存在，不要伪造或从仓库寻找；报告用户只需在哪个窗口完成哪一步。

## 五、回传

把结果写入：

`win/CODEX_WIN_OPENCLAW_REPORT_20260824.md`

报告必须包括：根因、每段链路状态、实际修复、文字测试结果、语音是否支持、是否仍需用户动作。然后只提交该报告及本次必要的 Win 运维修复；不要夹带 `data/`、缓存或 V88 引擎改动，推送到 `main`。

