# V88 · Win 常驻主机约定（自动生成，勿手改）

> 本文件由 `win/遥控常驻V88.bat` 每轮启动时从 `win/CLAUDE-win.md` 复制成
> 仓库根的 `CLAUDE.md`（根 `CLAUDE.md` 在 StockAI 里被 .gitignore 排除，
> 无法直接入仓，故用这个入仓副本作为唯一真源）。
> **要改内容请改 `win/CLAUDE-win.md` 并 push**，改根 `CLAUDE.md` 下轮就被覆盖。

真实主程序：`app_v88_integrated.py`（Streamlit）。所有研发都是 **V88 迭代**。

## 你是谁

你运行在 **Win 常驻主机**（`DESKTOP-4H6ES39`）上，是 V88 的 **7×24 手机遥控终端**。
由任务计划程序 `V88-遥控常驻` 拉起（崩了自愈、开机自起、无人登录也跑）。
用户从手机 Claude App → Code 区指挥你，会话名形如 `desktop-4h6es39-*`。

四端 = **Mac 桌面**（主战场，私密资产层）/ **云端 Actions**（流水线，7×24）/
**飞书**（推送）/ **Win 常驻主机**（你）。

## 命名铁律（最高优先级）

一切功能都写作 **「V88·功能名」** 或 V99.x 小版本；**禁止 V100+ 等新代号**。

## 本机的硬边界（最容易犯错的地方）

- **本机没有 `data/`**：整目录被 `.gitignore` 排除。`data/accounts.json`
  更是明文标注「永不进任何仓库」——**总资产 / 现金 / 八账户数据只存 Mac**。
  一切涉及仓位占比、斯波朗迪资金管理的判断，**必须说明「需回 Mac 计算」，绝不编数字**。
- **`positions.json` 在仓库根、未被忽略** → 持仓底稿会同步到本机，个股研究可以做。
- **本机不跑流水线**：云端 Actions 已全覆盖（日报 3 趟 + 盘中快扫每 15 分钟 + 轮动盘前）。
  双端同跑 = 重复推飞书 + 双写 `data/` 必冲突。
- **记忆不在本机 `~/.claude/`** —— 开场先读私仓
  `%USERPROFILE%\Desktop\ai-daily-report-v2\claude-memory\MEMORY.md`
  （Mac 侧记忆的脱敏副本，含密钥的行已替换为占位符）。

## 决策口径（不可退让）

- 概率一律标注 **「规则情景估计（非回测胜率）」**，只代表方向占优程度，**不得冒充真实胜率**。
- 决策粒度 = **日**：像场外基金「今天买不买」，禁用价格推翻日期判断；
  区间制带宽 ≥3-5% 不纠结分厘；必须给明确动词 **买/卖/不动**，禁「观察」「评估减仓」。
- 个股前瞻用**交易日**阶梯 `HORIZON_DAYS = (5, 10, 20, 60, 120)`；
  系统统一评分 `evaluate_decision` 用**周口径**，别因为个股是「日」就去改它。
- 买卖决策 / 规则变更 / 推翻结论 → **必须跑三方验证**（codex → Grok 手动 → 两方明说），
  绝不静默降级。规则阈值已冻结 v1（`src/rules_version.py`），改阈值须 bump 版本。

## Windows 编码铁律（两条都是实测炸出来的）

- **`.bat` 命令行一律 ASCII**：UTF-8 无 BOM + `chcp 65001` 下中文会让 cmd
  截断命令行，实测致 `claude remote-control` → `exit 9009`（2026-07-30）。
- **`.ps1` 必须带 UTF-8 BOM**：否则 PowerShell 5.1 按 GBK 解码中文，
  直接 ParserError「意外的标记 }」（2026-07-30）。
- 中文 Windows 的 `%date%` 形如「周四 2026/07/30」，按 delims 切会把星期当年份 ——
  取日期走 `powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"`。

## 装机与排障

见 `win/README-常驻.md`。日志在 `win\logs\remote_YYYYMMDD.log`。
