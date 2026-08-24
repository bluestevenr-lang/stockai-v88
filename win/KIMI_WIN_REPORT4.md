# Win-Kimi 报告 #4：V88 体检体系入驻龙虾（2026-08-24 14:5x 执行）

任务书：`win/KIMI_WIN_MISSION4.md`（2026-08-23 22:10 下达）。幂等，可重跑。
执行前 `git pull`：代理 7897 恢复后经代理拉取成功（直连当时不通）；main 已是最新，data 分支有更新。

---

## 1. 体检脚本部署与实测

`win/v88_health.py` 已拷入工作区 `~/.openclaw/workspaces/v88-mobile/v88_health.py`。部署中发现并修复 3 个 Win 环境兼容性 bug（明细见 `win/CHANGELOG.md` 2026-08-24 第 1 条）。修复后完整原始输出：

```
【V88体系体检】2026-08-24 14:13
私仓: C:\Users\admin\Desktop\ai-daily-report-v2
✅ market_pool.json: 2026-08-20 06:05（4天前, 行数2811）
✅ market_snapshot.json: 2026-08-21 18:23（2天前, 行数-）
✅ bottom_turn_pool.json: 2026-08-19 17:20（4天前, 行数-）
✅ value_zone.json: 2026-08-21 18:38（2天前, 行数15）
✅ trend_shift.json: 2026-08-21 18:38（2天前, 行数-）
✅ rotation_forecast.json: 2026-08-21 18:23（2天前, 行数-）
✅ intraday_decisions.json: 2026-08-20 02:21:15（4天前, 行数172）
✅ kimi_verify.json: 2026-08-21 19:30（2天前, 行数2453）
✅ gpt_verify.json: 2026-08-21 19:30（2天前, 行数506）
✅ tomorrow_plan.json: 2026-08-21 19:44（2天前, 行数-）
⚠️ data/fable_plan.json 无时间戳字段（无法判断新鲜度）
✅ kimi_verify reviewer 合规: kimi_k3_cli_guarded
✅ git 无冲突 | ## main...origin/main | stash堆积 3 个
💰 Moonshot 余额: 35.41 元（现金 35.41 / 代金券 0.00）
—— 汇总: ✅12 ⚠️1 ❌0 ——
```

注：reviewer 检查修复前误报 ❌「kimi_k3_cli_guarded 越权写入嫌疑」——实为 k3-256k 迁移后的新合规 reviewer 名，属脚本白名单过期，非真实越权。

## 2. AGENTS.md 规则追加（模板+工作区，两处一致）

- **体检规则（2026-08-23 起）**：原文照抄任务书，执行路径按任务书授权的路径处理条款改为包装命令 `v88health`（原因见第 3 节，同任务书#3 的 k3ask 先例）。
- **变更闸门（铁律23，2026-08-23 用户定纲）**：原文照抄，未改一字。
- 配套：新建 `win/CHANGELOG.md` 并按该闸门要求登记了本次全部 5 项环境微调。

## 3. cron「V88每周体检」

最终配置（`cron get` 实测）：

- ID `c1ccfffe-2f84-4545-81ff-bca58a0434ce`；schedule `17 21 * * 0` @ Asia/Shanghai（exact）；agent v88-mobile；session isolated
- 模型 **`kimi-coding/k3-256k`**（任务书指定 moonshot/kimi-k2.7-code，被网关 allowlist 拒绝：现仅放行 `[kimi-coding/k3-256k, moonshot/kimi-k3]`；选订阅档零现金，理由记 CHANGELOG 第 4 条）
- 投递 announce → feishu:ou_8759f7dbabcd38d084f8dacd444375bb；timeout 900s
- 提示词 = 任务书原文，仅两处授权性适配：①exec 命令最终为完整路径 `"C:\Users\admin\bin\v88health.bat"`；②私仓路径写明绝对路径；③加"严禁 ~ 符号"一句（exec 后端不展开 ~）

### 手动触发排障记录（共 7 次，最终成功）

| 次 | 结果 | 原因 |
|---|---|---|
| 1 | error 67ms | 模型 k2.7-code 被 allowlist 拒绝 |
| 2 | error 281s | nimble-shell 进程失败（长路径缩写 ~ 引发） |
| 3/4 | error | agent 把长 python 路径缩写成 `~`，exec 失败 |
| 5 | error | agent 用 `~` 路径探测 v88health，exec 失败 |
| 6 | error | `powershell -Command "v88health"`：cron 会话 PATH 无 bin 目录，找不到 |
| **7** | **ok 48s · delivered** | 完整路径 .bat + 严禁 ~，通过 |

第 7 次运行摘要（delivered 到飞书）：体检 ✅12⚠️1❌0；台账与今早已提交版本一致（私仓 `acf955b auto: 周体检台账更新 2026-08-24`），无重复提交；下周关注 Win 接管管线出池、Mac 会审首跑、GitHub 计费冻结三项。

### 台账首更核验（私仓侧）

私仓 `git log` 确认 `acf955b auto: 周体检台账更新 2026-08-24` 已存在（早前某次触发中完成）；成功运行判定证据一致未重复 commit。私仓工作区另有管线日常改动（AGENTS.md/HANDOFF.md/data/* 未提交），按"不动 data/"纪律未触碰。

## 4. 遗留问题

1. **exec 后端不展开 `~`** 是反复踩的坑（任务书#3/#4 共 5 次失败同根因）：后续所有给 agent 的命令一律用"完整路径 .bat"形态，已纳入 CHANGELOG 与提示词。
2. cron 会话 PATH 与交互会话不同（无 `C:\Users\admin\bin`）——包装命令在 cron 里必须带完整路径。
3. ⚠️ fable_plan.json 无时间戳字段：属私仓数据本身问题，台账 #5 跟踪中，Win 侧不越权修。
4. 网关 allowlist 已不含任何按量 K2.7 型号：任务书#3A 规则里"整理措辞用 K2.7"一句在现行架构下自动落空（主模型即 k3-256k），暂不造成故障，留待下次任务书统稿时修订。

## 附：本次提交文件

- `win/v88_health.py`（3 处修复）· `win/openclaw-v88/AGENTS.md`（+体检规则/+变更闸门）· `win/v88health.bat`（新，仓库存档）· `win/CHANGELOG.md`（新，铁律23 台账）· `win/KIMI_WIN_REPORT4.md`（本报告）
