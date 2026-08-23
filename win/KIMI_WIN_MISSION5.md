# Win-Kimi 任务书 #5：管线命脉迁 Win·第一批（2026-08-23 23:25）

执行者：Win 上的 Kimi Code（yolo 会话）。幂等可重跑。纪律同前（密钥不入 git；bat 纯 ASCII；openclaw CLI 绝对路径）。

背景（Mac-Kimi 已确诊）：**GitHub Actions 因账户计费问题自 8-20 全部冻结**（6 个工作流全部 4 秒即败：payments failed / spending limit）。云端管线脑干已断，Win 7×24 主机接管。本任务只做第一批三件事：环境、网络、底部池。其余工作流在台账#10 排队。

## 1. 建 V88 专用 python 环境（不许污染 kimi-desktop 运行时）

```bash
PYAPP="/c/Users/admin/AppData/Roaming/kimi-desktop/daimon-share/daimon/runtime/python/.venv/Scripts/python.exe"
"$PYAPP" -m venv /c/Users/admin/v88env
/c/Users/admin/v88env/Scripts/python.exe -m pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pandas numpy yfinance
```

把 pip 输出尾部和 `pip list | grep -i "pandas\|numpy\|yfinance"` 结果记进报告。

## 2. 网络实测（Yahoo 可达性，决定整批迁移的生死）

（GPT 过闸补强：必须校验非空行情 + 超时重试一次，不许只看"没报错"）

```bash
/c/Users/admin/v88env/Scripts/python.exe -c "
import yfinance as yf, sys
for att in (1, 2):
    try:
        df = yf.download(['AAPL','600519.SS','0700.HK'], period='5d', progress=False, timeout=30)
        if df is not None and not df.empty and df['Close'].dropna(how='all').shape[0] >= 2:
            print('NETWORK OK'); print(df['Close'].tail(2)); sys.exit(0)
        print('attempt', att, 'empty result')
    except Exception as e:
        print('attempt', att, 'fail:', str(e)[:80])
sys.exit(1)
"
```

- **通了**（打印 NETWORK OK + 价格表）→ 继续第 3 步。
- **不通**：检查本机代理（netstat 看 7890/7897/10809 常见端口；或问用户 Win 代理端口一次），设 `HTTPS_PROXY=http://127.0.0.1:<端口>` 重试。还不通 → 报告红字写"Win 网络不达 Yahoo"，停止后续步骤，不要硬跑。

## 3. 底部池迁移（台账#2）

私仓在 `C:\Users\admin\Desktop\ai-daily-report-v2`（Win 的 safe_pull 有 python3 缺失旧坑，用下面的等价手法）：

```bash
cd /c/Users/admin/Desktop/ai-daily-report-v2
git pull --rebase --autostash origin main
/c/Users/admin/v88env/Scripts/python.exe scripts/v88_json_merge.py --postpull
# 全量试跑（约7-10分钟，耐心等）
/c/Users/admin/v88env/Scripts/python.exe src/bottom_turn.py
```

- 试跑成功（输出"触底拐点池 … 上榜 N 只"）→ 提交推送：

```bash
git add -f data/bottom_turn_pool.json
git commit -m "data: 底部池Win首跑(管线迁Win第一批)"
git push origin main
```

- push 被拒（凭据）→ 报告写明，不许多次重试。
- 然后**验证新字段**：`grep -o '"hist_verdict": "[^"]*"' data/bottom_turn_pool.json | sort | uniq -c` —— 应看到"已验证/样本不足/验证失败"三档分布（这是台账#1 刚过的回测层，顺带验收）。

## 4. 注册计划任务（每天交易日自动跑）

我已写好任务脚本 `win/bottomturn_job.bat`（纯 ASCII）。你执行：

```cmd
schtasks /Create /TN "V88-BottomTurn" /TR "C:\Users\admin\Desktop\StockAI\win\bottomturn_job.bat" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 17:07 /F
schtasks /Run /TN "V88-BottomTurn"
```

- 时间口径：云端原档北京 17:00，Win 取 17:07（A股15:00/港股16:00 收盘后有增量）。
- `/Run` 后立即 `type C:\Users\admin\Desktop\ai-daily-report-v2\data\bottom_turn_pool.json | findstr generated_at` 看时间戳是否刷新（注意 bat 里 git push 失败要有日志——查 `win\logs\bottomturn.log`）。
- schtasks 输出原文记进报告。

## 5. 收尾

写 `win/KIMI_WIN_REPORT5.md`：环境清单、网络实测结论、首跑统计（universe/fetched/listed/三档回测分布）、计划任务注册输出、遗留问题。commit + push，然后对用户说"报告5已提交"。

**明示边界（铁律23）**：本任务只做环境与部署；发现引擎代码要改才能跑通时，停下写进报告，不许自改 src/。
