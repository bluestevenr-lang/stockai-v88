# Win-Kimi 任务书 #3A（补充包）：Kimi 额度查询与用量统计（2026-08-20 14:55）

执行者：Win 上的 Kimi Code（yolo 会话）。在任务书#3 之后执行；幂等，可重跑。
纪律同前：密钥永不入 git；openclaw CLI 用绝对路径。

用户需求（原话）：蓝一要能查 Kimi 额度使用情况（包括上次充值的 50 元还剩多少），并且在 K2.7 模型下完成统计分析——**查余额本身零 token 消耗**（直连 REST 接口），只有整理措辞用 K2.7。

---

## 1. 验证额度脚本

脚本已入库：`win/k3_quota.py`（余额查询 + 本地调用账本统计，文件无密钥）。

```bash
cd /c/Users/admin/Desktop/StockAI && git pull
py -3 win/k3_quota.py
```

- 正常输出 = "== 账户余额 == 可用余额 … 元现金/代金券 …" + "== K3 直达调用账本 =="
- 若 HTTP 401/404/400：检查 `~/.openclaw/openclaw.json` 里 moonshot 的 baseUrl 是不是 `https://api.moonshot.cn/v1`（脚本会自动读，但若网关用了自定义 baseUrl，把真实值记进报告）。
- 把完整输出原文记进报告。

## 2. 验证 K3 调用记账

仓库最新版 `win/k3_ask.py` 已内置记账（每次 K3 调用把 token 用量追加到 `~/.openclaw/k3_usage.csv`）。实测一次：

```bash
py -3 win/k3_ask.py "ping，回复一个字即可"
type "%USERPROFILE%\.openclaw\k3_usage.csv" 2>nul || cat ~/.openclaw/k3_usage.csv
```

账本里应有一行带时间戳和 token 数的记录，记进报告。

## 3. 给蓝一追加额度查询规则

在 v88-mobile 的 AGENTS.md（任务书#3 你追加 K3 直达规则的同一个文件）再追加，原文照抄：

```markdown
## 额度查询规则（2026-08-20 起）
- 用户消息含"额度/余额/用量/充值/统计"任一关键词时：用 exec 运行
  py -3 <k3_quota.py 最终可用路径>
  然后把输出整理成人话回复：余额报准数字（区分现金/代金券），账本报调用次数和 token 总量。
- 严禁编造余额数字；脚本报错就原文转述并建议检查密钥。
- 此功能不调 K3，零额外消耗；你自身的整理措辞用 K2.7 即可。
```

（路径处理同任务书#3 的 k3_ask.py：若 agent 工作目录受限，拷进工作区，报告记录最终路径。）

## 4. 探测内建用量统计（可选，5 分钟上限）

```bash
OC="/c/Users/admin/AppData/Roaming/npm/openclaw.cmd"
"$OC" status 2>&1 | head -50
"$OC" sessions list 2>&1 | head -20
```

看输出里有没有 token/cost 统计字段，有就把用法记进报告（网关侧 K2.7/K3 调用的官方统计口径）；没有就注明"内建统计不可用，以本地账本为准"。

## 5. 验收（让用户在飞书发）

1. `查一下额度` —— 应回复当前余额（现金/代金券分开）+ 调用账本概览
2. 数字必须和 platform.moonshot.cn 控制台显示的一致（让用户肉眼核对一次）

写 `win/KIMI_WIN_REPORT3A.md`（每步原始输出 + 验收结果），然后：

```bash
cd /c/Users/admin/Desktop/StockAI
git add win/KIMI_WIN_REPORT3A.md
git commit -m "win-kimi: 任务书3A报告——额度查询与用量统计"
git push
```

push 成功后对用户说"报告3A已提交"。
