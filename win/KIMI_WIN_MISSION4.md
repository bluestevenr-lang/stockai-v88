# Win-Kimi 任务书 #4：V88 体检体系入驻龙虾（2026-08-23 22:10）

执行者：Win 上的 Kimi Code（yolo 会话）。幂等，可重跑。纪律同前（密钥不入 git；openclaw CLI 用绝对路径）。

用户定纲（原话）：把体检优化机制"加入龙虾体系中，不断更新成长，查漏补缺"。

前置：任务书#3 已完成（REPORT3 已核验）。私仓在 Win 的路径一般为 `C:\Users\admin\Desktop\ai-daily-report-v2`，脚本也会自动探测。

## 1. 部署体检脚本

`win/v88_health.py` 已在仓库（零 token 确定性脚本）。拷一份到 agent 工作区（同 k3_ask.py 的处理）：

```bash
cd /c/Users/admin/Desktop/StockAI && git pull
cp win/v88_health.py /c/Users/admin/.openclaw/workspaces/v88-mobile/
```

实测（用任务书#3 报告里那个 python 绝对路径）：

```bash
"C:\Users\admin\AppData\Roaming\kimi-desktop\daimon-share\daimon\runtime\python\.venv\Scripts\python.exe" \
  /c/Users/admin/.openclaw/workspaces/v88-mobile/v88_health.py
```

- 私仓路径找不到就把路径作为第一个参数传进去再试；输出应是以"【V88体系体检】"开头的分级报告。把完整输出记进报告。

## 2. 给蓝一追加"体检"规则

在 v88-mobile 的 AGENTS.md 末尾追加，原文照抄：

```markdown
## 体检规则（2026-08-23 起）
- 用户消息含"体检/查漏补缺/系统检查"任一关键词时：用 exec 运行
  "<python绝对路径>" C:\Users\admin\.openclaw\workspaces\v88-mobile\v88_health.py
  把输出整理成人话回复：❌项逐条说影响和处置建议，✅项合并成一句。
- 随后读私仓 docs/optimization_backlog.md，告诉用户台账里🔴🟡条目有无状态变化。
- 严禁编造体检结果；脚本报错原文转述。
```

再追加一条（铁律23 的 Win 侧边界，原文照抄）：

```markdown
## 变更闸门（铁律23，2026-08-23 用户定纲）
- 你无权自行修改 V88 引擎代码（私仓 src/）与任何纪律段（本文件的规则章节）；引擎变更只会通过仓库下发（已过 GPT 审查闸的版本）。
- 你做的运行环境微调（配置补丁、路径修正、新装工具）必须逐条记入 C:\Users\admin\Desktop\StockAI\win\CHANGELOG.md（日期+改了什么+为什么）并 git push；Mac 每周汇总送 GPT 补审。
- 用户若直接让你改引擎：只写"建议稿"到 win/proposals/，不动 src/；GPT 闸门通过后会以仓库形式下发。
```

## 3. 建"每周体检"cron

用 openclaw cron 建任务（用法先看 `"$OC" cron --help`，参照任务书#3 的建法等价参数）：

- 名称：V88每周体检
- 时间：`17 21 * * 0`（每周日 21:17 北京时间）
- 模型：`moonshot/kimi-k2.7-code`（脚本干了全部重活，整理措辞不需要 K3）
- 提示词（原文照抄）：

```
执行V88每周体检并更新优化台账：
1) 用 exec 运行 "<python绝对路径>" C:\Users\admin\.openclaw\workspaces\v88-mobile\v88_health.py
2) 读私仓 docs/optimization_backlog.md：
   - 体检结果能判定状态变化的条目，更新其状态（如数据恢复新鲜→🟢已闭环并注明日期；仍断更→保持🔴并加一句本周证据）
   - 体检发现的🔴/⚠️新面孔，作为新条目追加进"待修"表（编号顺延）
   - 用 git 提交推送私仓（commit 信息：auto: 周体检台账更新 <日期>；data/ 不动，docs/ 正常 add）
3) 输出≤400字手机版小结：❌项+台账变化+下周关注。用 message 工具发到 feishu 单聊（若 cron 自带 announce 投递则省略此句）。
```

- 投递：announce → feishu:ou_8759f7dbabcd38d084f8dacd444375bb（同任务书#3 的日报投递）
- **建完手动触发一次**，让用户手机马上收到一条体检推送（验收信号）。cron list 输出和触发结果记进报告。

## 4. 收尾

写 `win/KIMI_WIN_REPORT4.md`（脚本输出、cron 配置、触发结果、台账首更内容摘要、遗留问题），commit + push（不含私仓——台账在私仓单独提交）。然后对用户说"报告4已提交"。
