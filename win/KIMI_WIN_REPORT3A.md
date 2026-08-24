# Win-Kimi 报告 #3A：Kimi 额度查询与用量统计（2026-08-24 11:4x 执行）

任务书：`win/KIMI_WIN_MISSION3A.md`（2026-08-20 14:55 下达，要求幂等可重跑）。
执行环境：Win 11 · Kimi Work 会话 · openclaw 网关 127.0.0.1:18789 正常。
执行前 `git pull`：本地 10 个文件的未提交改动（非本次任务产生）已 `git stash` 保存为 `win-local-pre-mission3a-20260824`，可恢复；拉取到 `fc56677`（任务书5 管线迁Win第一批）。

---

## 0. 重要架构发现（执行前必读）

本机代码在 08-20 晚～08-24 间已被另一端重构（commit `6e8e882 upgrade OpenClaw V88 decision context` 等）：

- **飞书 `v88-mobile` 主模型已固定为 Kimi Code 订阅 `kimi-coding/k3-256k`**，由 OpenClaw 会话直接回答；不再存在 K2.7 接待员，也不再按关键词切模型。
- AGENTS.md（模板+工作区）现行规则：**禁止调用历史 `k3ask` 或 Moonshot 按量 API**（避免重复回答和现金计费）。
- `win/k3_ask.py` 已重写为"历史兼容入口"，走 `kimi_subscription.chat_completion`（订阅 K3-256K），**仍保留本地记账**（`~/.openclaw/k3_usage.csv`）。
- 这解释了"K3-256"：它是 **K3 的 256K 上下文订阅档**，不是按量 API 的型号（按量 API 账号确实只有 k2.6/k2.7-code/k2.7-code-highspeed/k3 四个）。

本报告所有步骤按任务书执行，并与新架构对齐。

## 1. 验证额度脚本 `win/k3_quota.py`

命令：`python win/k3_quota.py`（注：本机无 `py` 启动器，`py -3` 在 cmd/git-bash 均不可用，详见第 3 节处理）。完整原始输出：

```
== Kimi Code 订阅 ==
认证状态: 已配置
V88默认模型: k3-256k
现金API支出: 0元（本系统不走按量接口）
共享订阅额度/重置时间: 请以 Kimi 会员中心显示为准

== K3 直达调用账本（本机记录）==
累计调用: 17 次 | 输入 53012 tokens | 输出 33200 tokens
最近5次:
  2026-08-20 16:56:00 | 入7092/出6446 | 我现在持仓里哪些个股需要重点关注
  2026-08-20 17:04:28 | 入5228/出4297 | 根据V88最新数据，今晚美股怎么操作建议 K3回复
  2026-08-20 17:06:42 | 入5228/出3483 | 根据V88最新数据，今晚美股怎么操作建议 K3回复
  2026-08-20 21:22:51 | 入6809/出3517 | 今晚我得持仓美股如何？k3回复
  2026-08-20 22:15:29 | 入4726/出2613 | 那今晚我要关注什么美股 k3回答
```

口径说明（与任务书预期输出的差异）：

- 任务书预期的 "== 账户余额 == 可用余额…元现金/代金券" 未出现——入库最新版脚本已改为**订阅口径**：本系统不走按量接口，现金 API 支出为 0。用户关心的"上次充值的 50 元"**未被本系统消耗**；共享订阅额度/重置时间需以 Kimi 会员中心显示为准（脚本如实标注，不编数）。
- 查询本身零 token 消耗（只读本地配置与账本，不调大模型），符合任务书要求。
- 脚本依赖 `kimi_subscription.py`（仓库根），因此**必须跑仓库路径副本**（不能拷进工作区单跑，parents[1] 导入会失败）。

## 2. 验证 K3 调用记账

命令：`python win/k3_ask.py "ping，回复一个字即可"`，输出（尾部）：

```
在

—
答复模型: k3-256k（Kimi Code订阅兼容入口）｜数据快照: 2026-08-21 19:45:31（北京时间）｜命中个股: 无
注：飞书主会话已经直接使用该模型，通常无需调用本脚本。
```

账本 `~/.openclaw/k3_usage.csv` 新增一行（原文）：

```
2026-08-24 11:37:07,k3-256k,4152,56,ping，回复一个字即可
```

记账字段 = 时间戳,模型,输入tokens,输出tokens,问题。✅ 符合预期。

## 3. 额度查询规则已写入 AGENTS.md（模板+工作区）

- 追加位置：`win/openclaw-v88/AGENTS.md`（包模板，常驻镜像每 10 分钟由此物化到工作区）+ `~/.openclaw/workspaces/v88-mobile/AGENTS.md`（即时生效）。两处内容一致。
- 规则原文照抄任务书，仅按任务书授权的路径处理条款替换执行路径：本机无 `py` 启动器，故仿照 `k3ask.bat` 先例创建包装命令 **`C:\Users\admin\bin\k3quota.bat`**（纯 ASCII，调用托管 venv Python 跑仓库路径脚本，已入库 `win/k3quota.bat` 备查）。规则中的执行行写为 `k3quota`。
- 包装命令实测输出（`cmd /c k3quota`）：订阅段同上；账本段已累计到 **18 次 | 输入 57164 | 输出 33256**（含第 2 节 ping）。
- 注意：现行 AGENTS.md 有"允许的遥控仅此四项（v88ctl.ps1）"限制；本次追加的额度规则是该限制之外由任务书明确授权的第 5 个 exec 例外，后续如再收紧需同步修订两处。

## 4. 内建用量统计探测（可选项，已做）

`openclaw status`（env：OPENCLAW_CONFIG_PATH/STATE_DIR/HOME 指向 `~/.openclaw`）关键输出：

```
| Sessions | 6 active · default kimi-k3 (262k ctx) · 2 stores |
| agent:v88-mobile:main | direct | 13h ago | k3-256k | 136k/262k (52%) · 🗄️ 50% cached |
```

`openclaw sessions list` 只列 main agent 2 个会话（gpt-5.5/OpenAI Codex，ctx 占用%）。

**结论：内建统计只有"会话上下文占用率/缓存率"，无费用、无账单口径。** 费用与用量统计以本地账本 `~/.openclaw/k3_usage.csv` + `k3quota` 为准（与任务书备选结论一致）。

## 5. 验收（留给用户在飞书发）

1. `查一下额度` —— 预期：蓝一执行 `k3quota`，回复订阅状态（已配置 / k3-256k / 现金支出 0 元）+ 账本概览（累计次数与 token 总量）。
2. 数字核对：账本 token 数为本机记录口径；订阅剩余额度请以 Kimi 会员中心为准（脚本已如实标注此边界）。充值的 50 元现金余额本系统未动用，可在 platform.moonshot.cn 控制台肉眼核对（应为未减少）。

## 附：本次变更清单

- `win/openclaw-v88/AGENTS.md`：追加"额度查询规则（2026-08-20 起）"一节（工作区副本已同步）。
- `win/k3quota.bat`：入库备查（实体在 `C:\Users\admin\bin\k3quota.bat`）。
- `~/.openclaw/k3_usage.csv`：新增 ping 测试行 1 条。
- stash：`win-local-pre-mission3a-20260824`（10 个文件的本地改动，与本任务无关，待用户定夺是否恢复）。
