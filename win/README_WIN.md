# V88 · Windows 第四终端接入指南（交接文档）

> 定位：**Win = 桌面版的 git 镜像**，不是另一套代码。四端关系：
> 🖥 Mac桌面（主战场）｜☁️ 云端（查看器）｜📱 飞书（推送）｜🪟 Win（镜像作战台）
> **全系统更新一致性 = 启动脚本每次先 `git pull` 两仓**——Mac 上发布的任何功能，Win 下一次启动自动就有，无需任何额外同步动作。

## 给 Win 上的 Claude（或人工）的执行清单

1. **前置**（人工装，各约1分钟）：
   - Git for Windows：https://git-scm.com/download/win
   - Python ≥3.12：https://www.python.org/downloads/ （**勾选 Add python.exe to PATH**）
   - Clash for Windows（或已有代理），记下混合端口（默认 7890）
2. **初始化**（一次性）：下载本仓库 `win/初始化V88.ps1`（或先手动 `git clone https://github.com/bluestevenr-lang/stockai-v88` 到桌面后进 win 目录），PowerShell 执行：
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\初始化V88.ps1
   ```
   - 私仓 v88-daily-report 首次克隆会弹 GitHub 登录 → 用 bluestevenr-lang 账号授权
3. **密钥**：把 Mac `~/Desktop/StockAI/.env` 的内容粘进 Win `Desktop\StockAI\.env`（`KIMI_CODE_API_KEY` / `TUSHARE_TOKEN`）。Kimi密钥必须以`sk-kimi-`开头；密钥永不入 git。
4. **代理端口**：若 Clash 端口不是 7890，改 `win/启动V88.bat` 里两行 proxy。
5. **启动**：双击桌面「V88」快捷方式。首次页面加载约 1-2 分钟（行情缓存冷）。

## 边界与纪律（Win 端必读）

- **只跑前端，不跑流水线**：日报/推送/雷达族落盘全部由 GitHub Actions 云端产出，Win 通过 `git pull` 消费即可。**不要**在 Win 配置任何定时任务（会与 Actions 重复推送、双花流量）。
- **并行编辑先 pull**：Mac 与 Win 都可能改 watchlist/持仓录单（会 git push 私仓）。铁律：**操作录单前先点一次启动脚本（内含 pull）**；冲突时生成物文件（data/*.json、journal/）一律取远端。
- 深链/公告/热榜等国内接口代码里已强制直连（trust_env=False），Clash只服务Yahoo及其他境外数据源；Kimi走Code订阅接口。
- Mac 独有不迁移：launchd 定时（Actions 已覆盖）、AppleScript 快捷方式（Win 用 .bat 等价实现，已含标签复用逻辑的简化版：固定 8501 端口+浏览器自动打开）。

## 故障速查

| 症状 | 处理 |
|---|---|
| 页面全空/行情失败 | 检查 Clash 在跑、端口与 .bat 一致 |
| 私仓 pull 报认证 | `git -C %USERPROFILE%\Desktop\ai-daily-report-v2 pull` 重新弹登录 |
| AI功能不可用 | .env 密钥没填/填错 |
| 端口占用 | 关闭旧的 V88-Streamlit 窗口再启动 |
