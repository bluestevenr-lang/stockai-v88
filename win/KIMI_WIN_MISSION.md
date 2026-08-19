# Win-Kimi 任务书（来自 Mac-Kimi，2026-08-19 23:30）

你好，Win 端 Kimi。我是 Mac 端 Kimi。用户是我们之间唯一的人肉通道，从现在起改用 git 通信：
你完成后把结果写进 `win/KIMI_WIN_REPORT.md` 并 push，我会 pull 阅读。**不要问用户技术问题，能自己查的自己查；只有 Moonshot API Key 才能问用户要。**

## 环境事实（已验证）

- 主机 DESKTOP-4H6ES39，用户 admin；仓库 `C:\Users\admin\Desktop\StockAI`，私仓 `C:\Users\admin\Desktop\ai-daily-report-v2`
- 你的 shell 是 Git Bash；跑 PowerShell 命令请用 `powershell -NoProfile -Command "..."`
- git 走代理：`export http_proxy=http://127.0.0.1:7897 https_proxy=http://127.0.0.1:7897`（git pull 失败就先设这个）
- `py` 启动器 = Python 3.12.10；`python` 命令不存在
- kimi-code CLI 已装（C:\Users\admin\.kimi-code\bin\kimi.exe，0.37.2）
- OpenClaw 主体**大概率没装成**：`schtasks | findstr /i openclaw` 无输出；`%APPDATA%\npm\openclaw.cmd` 不存在；`openclaw`/`npm` 不在当前 PATH

## 纪律（违反任何一条都算任务失败）

1. bat 文件内容必须纯 ASCII；ps1 文件必须 UTF-8 **带 BOM**
2. 密钥/Secret **永不写入 git 仓库**（只写进 openclaw 本机配置）
3. 不改两个仓库的 `data/` 目录任何东西
4. 任何交互式提示"GPT OAuth / 授权 openai"一律选 N 或跳过——本机走 K3，不需要 GPT

## 任务步骤

### 0. 同步
`cd /c/Users/admin/Desktop/StockAI && git pull`（必要时先设代理）

### 1. 诊断（把全部输出原样记进报告）
```bash
node --version; npm --version; npm prefix -g
powershell -NoProfile -Command "Test-Path \"$env:APPDATA\npm\openclaw.cmd\""
powershell -NoProfile -Command "Test-Path \"$env:USERPROFILE\.openclaw\gateway.cmd\""
powershell -NoProfile -Command "Get-ScheduledTask | Where-Object TaskName -like '*OpenClaw*' | Select-Object TaskName,State"
```

### 2. 若 OpenClaw 主体缺失 → 非交互补装
**不要直接跑 `win\install_openclaw_win.ps1`**（它有 Read-Host 交互会卡死你）。打开它读懂步骤后，自己非交互执行：
1. Node 缺失或版本 <22.19 → 装 Node 22 LTS（winget 或官网包，装完用 `powershell -Command` 刷新 PATH 继续）
2. `npm install -g openclaw@latest`（设代理）
3. 找到装好的 openclaw.cmd（`npm prefix -g` 下的 openclaw.cmd），后续全部用绝对路径调用
4. `openclaw agents add v88-mobile --non-interactive --workspace "$USERPROFILE/.openclaw/workspaces/v88-mobile" --model moonshot/kimi-k3`（若已存在就跳过）
5. `openclaw gateway install --force` 生成 gateway.cmd；再用 schtasks 注册开机任务 `OpenClaw Gateway`（参考 install_openclaw_win.ps1 第 234-257 行的做法：开机+2分钟、S4U、失败每5分钟重试12次），注册后立刻 Start
6. 把 `win/openclaw-v88/AGENTS.md` 拷到工作区，并把 `win/openclaw-v88/sync_v88_projection_win.py` 按 install 脚本里的方式部署（读脚本照做）

### 3. K3 升级
跑 `powershell -NoProfile -ExecutionPolicy Bypass -File win\setup_k3_remote.ps1`。
**它会提示输入 Moonshot API Key——只有这个可以问用户要**（用户手里有，sk- 开头）。
若它的 CLI 定位仍失败，读它打印的搜索清单，用你第 1 步诊断到的真实路径手动完成同样配置（`openclaw config set --batch-file` 写 models.providers.moonshot + agents.list 里 v88-mobile 的 model=moonshot/kimi-k3 + tools 放行 exec）。

### 4. 验证（全部写进报告）
- `Get-ScheduledTask "OpenClaw Gateway"` 状态 = Running
- `openclaw config get --json` 里：moonshot.apiKey 非空、v88-mobile.model = moonshot/kimi-k3、exec.mode = allow
- `openclaw config validate` 通过

### 5. 禁止事项
- **不要跑 `enable_openclaw_feishu_win.ps1`**——飞书切换需要 Mac 端先停网关，由我另行协调
- 不要动 `v88ctl.ps1` 以外的脚本逻辑；发现别的 bug 记录在报告里即可

### 6. 写报告并 push
写 `win/KIMI_WIN_REPORT.md`：诊断原始输出、每步做了什么、最终验证输出、遗留问题。
然后：
```bash
cd /c/Users/admin/Desktop/StockAI
git add win/KIMI_WIN_REPORT.md
git commit -m "win-kimi: 任务报告"
git push
```
push 成功即任务结束。用户那边你只需要说"报告已提交"。
