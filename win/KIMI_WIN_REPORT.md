# Win-Kimi 任务报告

> 执行时间：2026-08-19（北京时间）  
> 执行环境：DESKTOP-4H6ES39，管理员账户 admin，Git Bash，PowerShell

---

## 0. 同步

```bash
cd /c/Users/admin/Desktop/StockAI && git pull
```

结果：`Already up to date.`

---

## 1. 环境诊断（原始输出）

```bash
node --version; npm --version; npm prefix -g
```

```
v24.19.0
---
11.17.0
---
C:\Users\admin\AppData\Roaming\npm
```

```powershell
Test-Path "$env:APPDATA\npm\openclaw.cmd"
Test-Path "$env:USERPROFILE\.openclaw\gateway.cmd"
Get-ScheduledTask | Where-Object TaskName -like '*OpenClaw*' | Select-Object TaskName,State
```

```
False
False
（无输出：OpenClaw 相关任务不存在）
```

结论：Node 已满足版本要求（v24.19.0 ≥ 22.19），但 OpenClaw 主体、gateway.cmd、计划任务均缺失，需要补装。

---

## 2. 补装 OpenClaw 主体

### 2.1 安装 OpenClaw

```bash
npm install -g openclaw@latest
```

结果：安装成功。

```
OpenClaw 2026.7.1-2 (0790d9f)
```

`openclaw.cmd` 位于 `C:\Users\admin\AppData\Roaming\npm\openclaw.cmd`。

### 2.2 生成 v88-mobile 工作区

```bash
mkdir -p /c/Users/admin/.openclaw/workspaces/v88-mobile
cp /c/Users/admin/Desktop/StockAI/win/openclaw-v88/AGENTS.md /c/Users/admin/.openclaw/workspaces/v88-mobile/AGENTS.md
py -3 /c/Users/admin/Desktop/StockAI/win/openclaw-v88/sync_v88_projection_win.py \
  --source /c/Users/admin/Desktop/ai-daily-report-v2/data \
  --dest /c/Users/admin/.openclaw/workspaces/v88-mobile/context
```

结果：`projection ok: 2464 stocks`

### 2.3 创建 v88-mobile 代理

```bash
openclaw agents add v88-mobile --non-interactive \
  --workspace "C:\Users\admin\.openclaw\workspaces\v88-mobile" \
  --model moonshot/kimi-k3
```

结果：代理创建成功，model 直接指定为 `moonshot/kimi-k3`。

### 2.4 安装 Gateway 并注册计划任务

```bash
openclaw gateway install --force
```

结果：

```
Installed Scheduled Task: OpenClaw Gateway
Task script: C:\Users\admin\.openclaw\gateway.cmd
```

> **说明**：随后尝试按 `install_openclaw_win.ps1` 第 234-257 行重新注册计划任务（开机+2分钟、S4U、失败每5分钟重试12次），但当前 shell 令牌未提升，UAC 无法在非交互环境中触发，注册被拒绝（HRESULT 0x80070005）。因此保留 `openclaw gateway install` 自动创建的任务，并手动启动 Gateway 进程（见第 4 节）。遗留问题详见文末。

---

## 3. K3 升级与 API Key 注入

### 3.1 setup_k3_remote.ps1 执行情况

直接运行 `win\setup_k3_remote.ps1` 失败，原因：

```
[K3升级] openclaw: C:\Users\admin\AppData\Roaming\npm\openclaw.ps1
node.exe : Missing required argument "path".
```

根因：当前 OpenClaw 2026.7.1-2 中 `openclaw config get --json` 已要求传入 `<path>` 参数，而脚本仍按旧版无参方式调用。因任务书要求“不要动 `v88ctl.ps1` 以外的脚本逻辑”，故未修改 `setup_k3_remote.ps1`，改为**手动完成同等配置**。

### 3.2 手动完成的等价配置

1. **写入 Moonshot API Key**（仅写入本机 `~\.openclaw\openclaw.json`，未入 git）：

   ```bash
   openclaw config set models.providers.moonshot.apiKey <key>
   ```

2. **写入 Moonshot 提供商配置 + K3 模型定义 + v88-mobile 工具策略**：
   通过 `openclaw config set --batch-file` 写入与 `setup_k3_remote.ps1` 一致的配置项：
   - `models.providers.moonshot.baseUrl = https://api.moonshot.cn/v1`
   - `models.providers.moonshot.api = openai-completions`
   - `models.providers.moonshot.models = [kimi-k3]`
   - `agents.list[1].model = moonshot/kimi-k3`
   - `agents.list[1].tools.allow = [read, exec, process]`
   - `agents.list[1].tools.exec.mode = full`
   - `agents.defaults.models["moonshot/kimi-k3"].agentRuntime.id = openclaw`

   > 注：脚本原文使用 `exec.mode = allow`，但当前 OpenClaw 校验仅接受 `"deny"|"allowlist"|"ask"|"auto"|"full"`，`allow` 会被拒绝。因此使用语义最接近的 `full`，并在报告中记录。

3. **cloudflared 已就绪**：`C:\Users\admin\.openclaw\tools\cloudflared.exe` 已存在。

4. **配置校验通过**：

   ```
   Config valid: ~\.openclaw\openclaw.json
   ```

5. **重启 Gateway**：

   ```bash
   openclaw gateway restart
   ```

   结果：`Restarted Scheduled Task: OpenClaw Gateway`

---

## 4. 最终验证

### 4.1 核心验证输出

```
=== Node / npm ===
v24.19.0
11.17.0
C:\Users\admin\AppData\Roaming\npm
=== openclaw version ===
OpenClaw 2026.7.1-2 (0790d9f)
=== Scheduled task ===
TaskName         State
--------           -----
OpenClaw Gateway Ready
=== Gateway process ===
（后台 `Bash` 任务运行超时后，已改用 `Start-Process -WindowStyle Hidden` 以脱离当前 shell 的方式重新拉起）
cmd       17496  C:\WINDOWS\system32\cmd.exe  （gateway.cmd 包装进程）
node      15568  C:\Program Files\nodejs\node.exe  （OpenClaw Gateway 主进程）
日志确认：`agent model: moonshot/kimi-k3`、`gateway ready`、Moonshot API 请求 `status=200`。
=== Config validate ===
Config valid: ~\.openclaw\openclaw.json
=== Agents ===
[
  ...
  {
    "id": "v88-mobile",
    "model": "moonshot/kimi-k3",
    ...
  }
]
=== Config get model ===
moonshot/kimi-k3
=== Config get exec.mode ===
full
=== Moonshot provider key set ===
__OPENCLAW_REDACTED__
```

### 4.2 验证结论

| 检查项 | 结果 | 备注 |
|--------|------|------|
| `node` 版本 | ✅ v24.19.0 | 满足 OpenClaw 要求 |
| `openclaw` 已安装 | ✅ 2026.7.1-2 | 路径见上文 |
| `v88-mobile` 代理 | ✅ 存在 | model = `moonshot/kimi-k3` |
| Moonshot `apiKey` | ✅ 已设置 | 由 OpenClaw 自动脱敏显示 |
| `exec.mode` | ✅ `full` | 因 `allow` 不是有效枚举值，改用 `full` |
| `openclaw config validate` | ✅ 通过 | - |
| Gateway 进程 | ✅ 运行中 | cmd PID 17496 + node PID 15568；日志显示 `gateway ready`、Moonshot `status=200` |
| 计划任务状态 | ⚠️ `Ready` | 未提升权限导致无法重注册为 S4U/开机延迟；任务未显示 Running，但 Gateway 进程已通过 `Start-Process -WindowStyle Hidden` 脱离当前 shell 运行 |

---

## 5. 遗留问题

1. **计划任务未按 `install_openclaw_win.ps1` 第 234-257 行精确注册**
   - 当前任务由 `openclaw gateway install` 创建，属性为 `Interactive` 登录、`RunLevel = Limited`、`RestartCount = 0`、无 `PT2M` 延迟。
   - 重注册需要管理员提升令牌；当前 Kimi Code shell 未提升，且 `Start-Process -Verb RunAs` 在非交互窗口站下失败（“被用户取消”类错误）。
   - **建议**：用户在本地以管理员身份运行一次 PowerShell，执行 `win\install_openclaw_win.ps1`（或其中第 234-257 行的任务注册段落），即可按原设计获得 S4U+开机延迟+失败重试的稳健任务。

2. **`setup_k3_remote.ps1` 与当前 OpenClaw CLI 不兼容**
   - `openclaw config get --json` 现已要求 `<path>` 参数，脚本直接调用会失败。
   - 已手动完成脚本意图的等效配置，未修改脚本本身。

3. **`exec.mode = allow` 不是有效枚举值**
   - OpenClaw 2026.7.1-2 仅接受 `"deny"|"allowlist"|"ask"|"auto"|"full"`。
   - 已改为 `full` 以使配置校验通过，并在工具策略中保留 `deny` 列表限制写/编辑/浏览器/网络等高风险工具。

---

## 6. 变更提交

```bash
cd /c/Users/admin/Desktop/StockAI
git add win/KIMI_WIN_REPORT.md
git commit -m "win-kimi: 任务报告"
git push
```

---

*报告由 Win-Kimi 自动生成并提交。*
