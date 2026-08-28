# V88 Win 重装后「龙虾」恢复交接（2026-08-28）

> 本文件覆盖 GitHub 中 2026-08-24 的旧交接。给 Win Codex 时，应先让它完整读取本文件，再执行；不得只读旧报告或旧 `claude-memory`。

## 一句话目标

在全新 Windows 上恢复唯一一套 V88 生产链：

`V88确定性数据/规则 → GPT主审 → Kimi K3独立复核 → 经典书理 → 中央裁决 → 脱敏投影 → 飞书V88-GPT`

Win 是未来 7×24 主机；Mac 目前使用独立应用临时承接飞书。按用户已授权的主机归属，在最小安全聊天验收后恢复 Win 正式应用；若同一应用还有其他接收端，先停止那个接收端，避免抢消息。完整投资业务另行验收，不再要求额外输入 `CUTOVER` 口令，详见下方 2026-08-29 恢复步骤。

## 现行角色与纪律（8月24日后已改变）

- GPT/Codex：51% 主审与终审责任。
- Kimi K3：49% 独立证据官、反对票和漏审检查；不得看到 GPT 的票或理由后再投票。
- 经典书理：纪律闸；不能把未通过抬成通过。
- CS1~CS9 是确定性硬闸，不计模型票。
- 三方必须基于同一个冻结 `factpack_id`、同一周期、24小时内的新鲜记录。
- 短线（约1~20日）、中线（约2~8周）、长线（约3~12月）分别裁决。
- 质量、执行、系统健康、风险是四条独立轴。
- 缺席、过期、异包、未审是 `PENDING`，不是 `0A`。
- 三席同周期通过：质量为3A；合同已触发且无冻结才是 `3A_NOW`，未触发是 `3A_PREPARE`，系统/风险冻结是 `3A_BLOCKED`。
- GPT+K3同周期通过、书理待证/有保留：`2A_CONDITIONAL`；只有合同触发后才可条件执行。
- 单席支持：`1A_RESEARCH`，只研究不可执行。
- 唯一推荐权威是私仓 `data/triad_selection.json`。旧扫描榜、`rank_score`、历史徽章和 DeepSeek 字段没有推荐权。
- Claude、DeepSeek、Grok 均无现役裁决权；`src/claude_standard.py` 只是历史命名的确定性规则闸。

## 零新增费用铁律

只允许用户已有订阅：

- GPT：ChatGPT/Codex OAuth；目标模型 `gpt-5.6-sol`。
- K3：Kimi Code managed OAuth；原生 CLI 模型 `kimi-code/k3-256k`。
- 所有生产模型必须 `fallbacks=[]`。
- 禁止 Moonshot/Open Platform、DeepSeek、Gemini API、Extra Usage、API Key、PAYG、自定义付费 endpoint、静默 fallback。
- 订阅或模型不可用时，当前席位记 `PENDING` 并冻结动作；不得拿旧票冒充本轮完成。
- GitHub Actions 必须保持 `V88_DISABLE_LLM=1`，云端不调用模型。
- 登录只走官方交互 OAuth；任何 token、App Secret、API Key 都不得贴到聊天、日志或 GitHub。

## 仓库恢复基线

不要复制 Mac 的脏工作树；在 Win 重新克隆远端。

### StockAI（公开仓）

至少应包含以下提交或其后继版本：

- `962a2fe`：同包、24小时、K3正式晋升后才可声称双审完成。
- `8142c98`：Windows UTF-8 / PowerShell 原生输出安全。
- `26862d2`：三周期中央状态、投影v2与 `v88ctl review`。
- `442ea59`：历史 GPT OpenClaw 路由修复；不得盲跑其中旧一键逻辑，见下方禁令。

### ai-daily-report-v2（私仓）

至少应包含：

- `579fbbb`：GPT订阅成为主审，DeepSeek退出现役文字/裁决链。
- `b65fa4f`：中央 GPT×K3×书理状态机接管推荐出口。
- `dcffb96`：真实调用 Kimi Code OAuth 订阅。
- `c77d0d8`：K3独立审查，禁止读取GPT意见。
- `a3a98fd`：三周期、四轴、PENDING/1A/2A/3A新漏斗。

私仓恢复后先读（按优先级）：

1. `AGENTS.md`
2. `CLAUDE.md`（只是历史文件名，GPT/Codex已是现役核心）
3. `docs/CODEX_TAKEOVER.md`
4. `HANDOFF.md`

若旧 `claude-memory` 与以上文件冲突，以上四项优先；不得恢复“Claude核心”或“DeepSeek现役”的旧口径。

## Win 清洁重装的正确拓扑

### 1. 飞书接待代理

- 只保留一个绑定飞书的代理，建议 ID `v88-gpt`。
- 模型 `openai/gpt-5.6-sol`，ChatGPT OAuth，`fallbacks=[]`。
- 该代理只读取脱敏工作区，不执行任意命令、不改代码、不访问浏览器、不调用券商。
- 优先使用 OpenClaw embedded OAuth 运行时并保持 `exec=deny`；Mac 已于 2026-08-27 实测 HTTP 200 和真实飞书往返成功。不要为了 Codex runtime 而开放 full exec。
- 5.6不可用时停止并报告；不得静默切5.5或任何别的模型。

### 2. K3复核席

- K3不是第二个飞书接待员，不绑定同一机器人。
- 由私仓 `src/dual_cli_review.py` 调用本机 Kimi Code CLI OAuth。
- 必须证明 provider=`managed:kimi-code`、模型=`kimi-code/k3-256k`、`subscription_only=true`、`paid_fallback=false`、`fallbacks=[]`。
- 原始审计写 `kimi_cli_verify.json`；只有安全晋升器可原子更新正式 `kimi_verify.json`。

### 3. 后台双审

- `src/gpt_subscription.py` 调用本机 Codex CLI OAuth；不通过飞书代理执行。
- GPT与K3使用同一平衡论文池（A/港/美×短/中/长），独立盲审后才汇总。
- 飞书只读取已经完成的中央脱敏投影。若以后需要飞书触发双审，只能增加窄权限任务触发器，禁止开放任意 Shell。

## 隐私边界

- 飞书工作区只能有 `overview.json`、`name_index.json`、脱敏 `modules/*_pub.json` 和脱敏个股快照。
- 不得包含账户、总资产、金额、数量、成本、私人目标、券商信息或完整私人记忆。
- 必须删除：`%USERPROFILE%\.openclaw\workspaces\v88-gpt\knowledge\v88-claude-memory`。
- 永久禁止把私仓 `claude-memory/` 整目录复制到飞书工作区。
- `win/feishu_raw_log.jsonl` 是历史安全事故文件：不得恢复、不得提交；原始飞书/OpenClaw日志只能保留在本机临时目录。
- 公开 StockAI 仓禁止 `git add -A` 自动扫入运行时文件；提交必须显式列出代码文件。

## 旧脚本禁令（清洁恢复时不要直接运行）

在重新审计并修正前，不得直接运行：

- `win/同步V88.bat`：会对公开仓和私仓执行 `git add -A`。
- `win/遥控常驻V88.bat` 的旧版本：会复制私人记忆并自动运行旧repair。
- `win/repair_gpt_openclaw_once.ps1` 的旧版本：报告日期路径不一致、可能假成功并自动推报告。
- `win/setup_k3_remote.ps1` 的旧 full-exec 模式：会让飞书具备任意命令与越界读权限。
- `win/初始化V88.ps1` 中任何生成/写入API Key的旧分支。
- `k3ask`、Moonshot插件、K2.7旧接待与任何按量兼容入口。

现有安装器只创建/复用 `v88-mobile`，旧repair却假设 `v88-gpt` 已存在；clean install 必须显式创建唯一 `v88-gpt`，不能靠旧脚本拼接后假装恢复完成。

## 恢复步骤

**2026-08-29 运维修正：基础聊天恢复与投资认证分阶段验收。** 详见 [远程运维与监控约定](V88_REMOTE_MONITORING_PLAN_20260829.md)。K3待授权或三方未完成不能让基础诊断聊天永久关闭；未完成投资认证时仍不得发布投资推荐。

1. 安装受支持的 Git、Python 3.12+、Node/OpenClaw、Codex CLI、Kimi Code CLI。
2. 用当前 Windows 用户目录和 `$env:USERPROFILE` 自动发现路径；禁止硬编码 `C:\Users\admin`。
3. 克隆两仓并核对上述最小提交。
4. 先完成 GPT 官方 OAuth 与实际模型探针。Kimi Code managed OAuth 在业务认证阶段独立完成，不作为基础聊天的前置条件。需要浏览器授权时明确暂停让用户亲自完成，不冒充后台仍在运行。
5. 清除 OpenClaw 进程可见的所有 `*API_KEY`、`*ACCESS_TOKEN`、`*AUTH_TOKEN`、`*BASE_URL`、`*API_BASE`、`*ENDPOINT` 覆盖变量；不得删除其他软件的系统配置，只在V88/OpenClaw子进程中净化。
6. 创建唯一飞书GPT代理和只读工作区。业务投影尚未验收时，使用不含私人数据的最小诊断上下文，不暴露原始持仓和私人记忆。
7. 验证配置、权限隔离、指定GPT订阅模型的真实问答、无API付费或fallback，确认用户已指定的正式Feishu应用和主机归属。
8. 若同一Feishu应用尚有另一台接收端，必须先明确停止对应旧接收端再启Win；不同应用的Mac临时机器人不等于同一接收端。无需等待所有投资认证才启动最小安全聊天，也不再额外要求用户输入特定魔法口令。
9. Win最小聊天启用后，手机发一条自然语言测试并收到真实模型回答。`/new`仅是会话重置，不能替代问答验收。此时业务状态仍可为PENDING。
10. 安装独立健康检查并验证脱敏状态和告警；再检查关闭Codex后的回复能力、Win重启后的服务状态。未实测的项目明确标未验收。
11. 业务阶段：生成脱敏投影并通过投影/隐私测试，完成GPT与K3订阅路线实测，再运行 `dual_cli_review.py review --trigger user --limit-batches 5`。
12. 同事实包、中央文件和完整业务验收通过后才开放V88投资解释/推荐。未通过时回复诊断与缺口，不伪造认证，不静默断开基础聊天。

## 必跑测试

私仓：

```text
tests/test_gpt_subscription.py
tests/test_kimi_subscription.py
tests/test_dual_cli_core.py
tests/test_dual_cli_promotion.py
tests/test_horizon_triad.py
tests/test_review_pools.py
tests/test_triad_selection.py
tests/test_export_portfolio_pub.py
```

StockAI：

```text
win/openclaw-v88/projection_tests.py
tests/test_openclaw_projection_v2.py
tests/test_grade_card_safety.py
tests/triad_cloud_contract.py
```

## 运行验收闸

基础聊天恢复必须满足：配置与只读隔离正确、指定GPT订阅模型实测成功、唯一应用接收端、手机自然语言往返成功。任何缺项都不能宣称聊天恢复。

以下是**完整V88业务恢复**要求；K3/业务待审不关闭已经验收的安全诊断聊天：

- OpenClaw `config validate` 通过，Gateway仅监听loopback，且只有一个实例。
- GPT探针精确返回指定哨兵文本，provider/model正确，OAuth可用，无API key/billing/fallback痕迹。
- K3探针为 managed OAuth、K3-256K、无fallback。
- `dual_cli_status.json`：`state=completed`、`ok=true`、`promoted=true`、`kimi_official_promoted=true`。
- GPT和K3审查数均大于0，且与 `review_factpack.json`、`triad_selection.json` 的 `factpack_id` 完全一致。
- 两席均在24小时内；异常时不覆盖上一正式席位，本轮显示PENDING。
- 投影测试通过，不含资产和私人记忆；中央裁决缺失时失败关闭。
- 飞书只有一个绑定代理；`enabled/configured/running/connected/works` 全通过。
- 如为同一Feishu应用，旧接收端已明确停止后才完成Win切换；不要求无关的独立Mac临时应用一并停用。

## 当前可用于回归的安全基线（不是交易结论）

- 2026-08-28 Mac生产双审已成功晋升：GPT与K3均审49条、同一事实包，`promoted=true`。
- Mac临时飞书后台已于2026-08-27真实完成“同步V88”往返。
- 这些只用于判断新Win是否恢复到相同能力，不得把旧名单、旧价格或旧票当成新推荐。

## 给 Win Codex 的最终输出格式

```text
WIN_V88_REBUILD_20260828
generated_at=<北京时间>
result=PASS|BLOCKED

repos: stockai=<短提交> private=<短提交> required_commits=<true|false>
subscriptions: codex_oauth=<true|false> kimi_oauth=<true|false> paid_route_detected=<true|false>
models: gpt=<实际模型> k3=<实际模型> fallbacks_empty=<true|false>
openclaw: config_valid=<true|false> gateway_loopback=<true|false> bound_agents=<数量>
privacy: private_memory_mirrored=<true|false> raw_logs_tracked=<true|false> asset_fields_projected=<true|false>
review: state=<completed|pending|failed> same_factpack=<true|false> fresh_24h=<true|false>
feishu: mac_stopped=<true|false> win_enabled=<true|false> probe_works=<true|false> sync_roundtrip=<true|false>
remaining_actions: <NONE或唯一必要动作>
```

任何一项不通过都必须写 `BLOCKED`，不得用旧 `.done`、旧报告、旧票或“进程退出码0”冒充成功。
