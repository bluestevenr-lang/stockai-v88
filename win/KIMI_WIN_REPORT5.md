# Win-Kimi 报告 #5：管线命脉迁 Win·第一批（2026-08-24 16:3x 执行）

任务书：`win/KIMI_WIN_MISSION5.md`（2026-08-23 23:25 下达）。背景：GitHub Actions 因计费冻结，云端管线脑干已断，Win 7×24 接管。本批三件事：环境、网络、底部池。

---

## 1. V88 专用 python 环境（不污染 kimi-desktop 运行时）

- `C:\Users\admin\v88env`（Python 3.12.14 venv，基于任务书指定解释器创建）
- pip 清华镜像安装成功，尾部：`Successfully installed beautifulsoup4-4.15.0 certifi-2026.7.22 cffi-2.1.1 charset_normalizer-3.5.1 curl_cffi-0.16.0 idna-3.19 lxml-6.1.2 multitasking-0.0.13 numpy-2.5.2 pandas-3.0.5 peewee-4.3.0 platformdirs-4.11.3 protobuf-7.36.0 pycparser-3.0 python-dateutil-2.9.0.post1 pytz-2026.3.post1 requests-2.34.2 six-1.17.0 soupsieve-2.9.2 typing-extensions-4.16.0 tzdata-2026.3 urllib3-2.7.0 websockets-17.0.1 yfinance-1.6.0`
- `pip list` 核验：**numpy 2.5.2 / pandas 3.0.5 / yfinance 1.6.0**

## 2. Yahoo 网络实测（生死门）：**通，但必须走代理 7897**

- 直连：两次均 `YFRateLimitError(Too Many Requests)`（429），proxy 7897 端口在 LISTENING 但首次经代理也 429。
- 冷却 60s 后设 `HTTPS_PROXY=http://127.0.0.1:7897` 重试（任务书校验口径：非空+≥2行+重试一次）：

```
NETWORK OK (via proxy 7897)
Ticker         0700.HK    600519.SS        AAPL
Date
2026-08-21  457.000000  1272.829956  309.350006
2026-08-24  440.399994  1304.660034         NaN
```

（AAPL 第二行 NaN = 当日美股未开盘，正常。）**结论：Win 可达 Yahoo，前提 HTTP(S)_PROXY 指向 127.0.0.1:7897。** 已把代理环境写入 `win/bottomturn_job.bat`（CHANGELOG 第 11 条）。

## 3. 底部池迁移（台账#2）

### 3a. 前置排障（任务书未预料，已处理）

- **私仓 rebase 卡死**：`git pull --rebase --autostash` 报 unresolved conflict——interactive rebase 中途搁浅（15:41 的 win 同步提交重放时 5 个 data/*.json 三方冲突），与体检脚本预警的"上次卡死5天"同型。处置：冲突文件全部为生成数据，取本地重放侧（较新）解之，`git rebase --continue` 异常拒绝（索引已无未合并项但仍报冲突），改用 rebase 自存的 author-script/message 手动提交 + `update-ref` + `--quit` 完成；推送 `0ed722f`，镜像链恢复。
- **v88_json_merge.py --postpull 崩 `stashes=None`**：根因 = 脚本 `subprocess.run(text=True)` 按 GBK 解码含中文的 git 输出（stash 里有"win本地未提交数据备份"）。**按铁律23 未改私仓脚本**，以 `PYTHONUTF8=1` 环境变量方式通过：`[v88json] ✅ 全部追踪 JSON/JSONL 校验通过，索引无未解决冲突`。

### 3b. 全量试跑（手动首跑，245.4 秒）

```
💎 触底拐点池 2026-08-24 16:00（北京时间） · bottomturn-v1
   全池1888 → 取到1866 → 深水位1063 → 拐点+分数上榜330  用时245.4秒
```

提交推送：`895b8a9 data: 底部池Win首跑(管线迁Win第一批)`（私仓 main）。

### 3c. 三档回测分布验证（台账#1 顺带验收）

```
10 "hist_verdict": "已验证"
94 "hist_verdict": "样本不足"
76 "hist_verdict": "验证失败"
```

三档齐全 ✅。

## 4. 计划任务 V88-BottomTurn

- 注册：`成功: 成功创建计划任务 "V88-BottomTurn"。`（MON-FRI 17:07，/F 覆盖）
- `/Run` 两次验证：
  - 首跑：json 刷新（16:09）、commit `c7e8294` 推送成功，但 src 收尾打印 💎 在 GBK 控制台崩 `UnicodeEncodeError`（产物完好，退出码非零）。
  - 修复：`win/bottomturn_job.bat` 加 `set PYTHONUTF8=1`（Win 侧任务脚本，CHANGELOG 第 11 条）。
  - 重跑：**全程无 Traceback**，干净收尾——`全池1888 → 取到1861 → 深水位1057 → 上榜333 用时247.8秒`，generated_at 16:21，commit `cec00e2` 推送成功，日志 `bottomturn job end` 完整。
- push 失败留痕机制：任务书 GPT 补强已在 bat 内（`PUSH FAILED` 写日志），本次两次推送均成功。
- 注意：git-bash 里 `schtasks /Run /TN "名称"` 的引号会被吞（报"系统找不到指定的文件"），用 `/TN V88-BottomTurn` 无引号形式或临时 bat 执行。

## 5. 遗留问题

1. **Yahoo 必须走代理**：代理 7897 不在或限流时任务会失败——bat 已内置代理；若代理端口变更需同步改 bat。Yahoo 限流存在偶发 429，src 内重试机制兜底。
2. **私仓 stash 堆积 3 个**（win本地备份-20260816 + 2×autostash）：体检脚本已列为 🟡（台账#11），建议下周体检时人工清理，本次未动。
3. **git 中文输出 GBK 解码**是 Win 侧反复出现的坑（v88_health.py、v88_json_merge.py、bottom_turn 收尾打印三次踩中）：统一对策 `PYTHONUTF8=1`，已入 bat；私仓脚本本体未改（铁律23），建议 Mac 端把 `subprocess.run(..., encoding="utf-8", errors="replace")` 补丁过闸下发。
4. **台账#10（GitHub 计费冻结）未解**：云端 6 个工作流仍冻结，需用户本人处理计费；本批只接管了底部池，其余工作流按任务书排队。

## 附：本次变更（均已提交）

- StockAI 仓：`win/bottomturn_job.bat`（+代理环境 +PYTHONUTF8）、`win/CHANGELOG.md`（第 11-13 条）、`win/KIMI_WIN_REPORT5.md`（本报告）
- 私仓：`0ed722f`（rebase 解卡）、`895b8a9`（底部池 Win 首跑）、`c7e8294`/`cec00e2`（计划任务两次日跑产物）
