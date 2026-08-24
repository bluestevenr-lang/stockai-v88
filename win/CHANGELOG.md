# Win 侧运行环境变更台账（铁律23·逐条登记）

## 2026-08-24

1. **win/v88_health.py 修两个兼容性 bug**（任务书#4 部署时发现）：
   - `parse_ts` 返回的时间带秒（如 16:41:45）导致 `strptime("%Y-%m-%d %H:%M")` 崩 ValueError → 截断到分钟；
   - kimi_verify reviewer 白名单过期：k3-256k 迁移后 reviewer 为 `kimi_k3_cli_guarded`，旧检查（字面 `kimi_cli`/大写 Kimi）误报"越权写入" → 改正则 `kimi.*cli`（忽略大小写）；
   - `git()` 子进程 GBK 解码 UTF-8 输出抛 UnicodeDecodeError → 显式 `encoding="utf-8", errors="replace"`。
   - 原因：脚本入库后首次在 Win 实跑，三处均为环境差异问题，不涉及私仓引擎。
2. **新建 `C:\Users\admin\bin\k3quota.bat`**（任务书#3A）：`py` 启动器本机不存在，仿 k3ask 先例做包装命令；实体在 bin，仓库存档 `win/k3quota.bat`。
3. **新建 `C:\Users\admin\bin\v88health.bat`**（任务书#4）：cron 隔离会话中 agent 多次把长路径缩写成 `~`（exec 后端不展开 ~，必失败；任务书#3 已记录同型故障），包装成一词命令根治；仓库存档 `win/v88health.bat`。
4. **cron「V88每周体检」模型由任务书指定的 `moonshot/kimi-k2.7-code` 改为 `kimi-coding/k3-256k`**：网关 allowlist 现仅放行 `[kimi-coding/k3-256k, moonshot/kimi-k3]`（订阅架构），k2.7-code 被 preflight 拒绝；选订阅档零现金消耗，符合任务书"措辞不需要 K3 满血"的省钱意图。
5. **cron 提示词两次加固**：①exec 命令改一词 `v88health` 后又改完整路径 `"C:\Users\admin\bin\v88health.bat"`——cron 会话 PATH 可能不含 `C:\Users\admin\bin`，一词命令在 PowerShell 下解析失败；②明确"严禁 ~ 符号"。第 7 次手动触发成功（48s，delivered）。

## 2026-08-24（预算专项，用户定纲：每月现金上限 10 元）

6. **新建 `win/k3_budget.py` 预算哨兵**（零 token）：直连 `/v1/users/me/balance` 取余额，快照落 `~/.openclaw/moonshot_balance.jsonl`，按相邻快照差分算本月已花（跳增自动计充值）；>8 ⚠️ / >10 🚨；附本地 k3_usage.csv 本月花费估算。包装命令 `C:\Users\admin\bin\k3budget.bat`，仓库存档 `win/k3budget.bat`。
7. **win/v88_health.py 余额段升级**：体检每次运行自动落余额快照并判级（>10 ❌ / >8 ⚠️ / 否则 ✅ 显示"本月 X/10 元"），周体检 cron 无需改动即获得预算监督。
8. **新建 cron「V88预算哨兵」** `7 21 * * *` @ Asia/Shanghai，kimi-coding/k3-256k（订阅零现金），announce → 飞书单聊；手动触发一次 ok（10.4s，delivered）。ID 17ce0d7a-f91e-42cd-a16c-778f5a234b6a。
9. **AGENTS.md（模板+工作区）追加「预算红线」规则**：按量调用默认禁止、发现按量 fallback 立即报告、问预算跑 k3budget。
10. **win/proposals/ai_budget_cap.md**：Mac 引擎级预算闸建议稿（铁律23 流程，不动私仓 src/）；含 65→35.41 消耗源核查请求。
