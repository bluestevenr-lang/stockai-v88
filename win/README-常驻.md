# V88 · Windows 主机化（第四终端 → 常驻主机）

**2026-07-30 定则变更**：Win 从「只跑前端的第四终端」升级为 **常驻遥控主机**。
Mac 变成可开可关。流水线仍然**不在任何个人电脑上跑**——那是云端 GitHub Actions 的活。

## 为什么是 Win 而不是 Mac

| | Windows 任务计划程序 | macOS LaunchAgent |
|---|---|---|
| 无人登录时能跑吗 | **能**（S4U 登录类型） | **不能**，必须有登录会话 |
| 崩溃自愈 | 失败重试 N 次，内置 | KeepAlive，但需会话在 |
| 断电自动开机 | BIOS `Restore on AC Power Loss` | `pmset autorestart` |
| 实测教训 | — | 2026-07-30 Mac 重启后 v88lite 的 LaunchAgent 压根没加载，8501/8600 全死 |

## 装法（一次性）

1. 双击 `同步V88.bat` —— 先把这批新文件拉下来
2. **管理员** PowerShell：
   ```
   powershell -ExecutionPolicy Bypass -File "%USERPROFILE%\Desktop\StockAI\win\常驻V88.ps1"
   ```
3. **⚠️ 手动过两个交互闸（脚本代答不了，跳过则后台永远起不来）**——普通 PowerShell：
   ```
   cd "$env:USERPROFILE\Desktop\StockAI"
   claude                     # 选 1. Yes, I trust this folder  然后 /exit
   claude remote-control      # 对 Enable Remote Control? (y/n) 答 y  然后 Ctrl+C
   ```
   实测教训（2026-07-30）：漏掉这两步时日志一直刷
   `Error: Workspace not trusted...` 和 `Enable Remote Control? (y/n)`，
   任务显示 Running 但其实卡在等输入。
4. 进 BIOS 打开 `Restore on AC Power Loss`（脚本改不了，断电后自动开机靠它）
5. 手机验收：Claude App → Code 区 → 能看到这台 Win（电脑图标 + 绿点）

> 若未装 Claude Code：`irm https://claude.ai/install.ps1 | iex`
> 装完**必须先手动跑一次 `claude` 登录**（与手机同账号），
> 否则无人登录模式下拿不到凭据，任务会起不来。

## 文件职责

| 文件 | 谁调用 | 干什么 |
|---|---|---|
| `初始化V88.ps1` | 人，一次性 | 克隆两仓 + 装依赖 |
| `常驻V88.ps1` | 人，一次性（管理员） | 关睡眠 + 注册两个计划任务 |
| `遥控常驻V88.bat` | **任务计划程序** | 拉两仓 → 起 `claude remote-control` → 挂了自己重起 |
| `夜间重启遥控.bat` | **任务计划程序 03:30** | 踢一次遥控，强制拿最新代码 |
| `手机遥控V88.bat` | 人，手动调试用 | 交互版，有 pause，关窗即下线 |
| `启动V88.bat` | 人 | 看板，绑 `127.0.0.1`，仅本机 |
| `启动V88-手机可见.bat` | 人 | 看板，绑 `0.0.0.0`，内网手机可看 |
| `同步V88.bat` | 人 | 把 Win 的改动 commit+push 回 GitHub |

## 注册了哪两个任务

- **`V88-遥控常驻`** — 开机 +1 分钟起；不管用户是否登录都运行；失败每 5 分钟重试，最多 3 次；运行时长不限
- **`V88-夜间重启遥控`** — 每天 **03:30**（撞 V88 维护时间窗 03:00-04:30）；可唤醒计算机

## 三层分工（最终形态）

| 角色 | 谁干 | 需要开机吗 |
|---|---|---|
| 流水线 / 飞书推送 | **云端 GitHub Actions** | 都不用 |
| 手机遥控主机 | **Win，7×24** | Win |
| 看板 | **Win :8501 内网** | Win |
| 私密资产层（`data/accounts.json`） | **Mac，按需** | Mac |

## 已知边界（别踩）

- **`data/` 整目录被 `.gitignore` 排除**，`data/accounts.json` 明文标注「永不进任何仓库」
  → **总资产/现金/八账户数据只存 Mac**，Win 上算不出仓位占比、跑不了斯波朗迪资金管理那套。
  需要资产层判断时开 Mac。
- `positions.json` 在仓库根、**未被忽略** → 持仓底稿会同步到 Win，Win 能做个股研究。
- **Win 仍然不接流水线**：双端同跑会重复推飞书，且两边都写 `data/` 必然 rebase 冲突。
- `启动V88-手机可见.bat` 绑 0.0.0.0 且面板**无登录保护**
  → 只在信任的家庭 WiFi 用，**绝对不要端口映射到公网**，里面有持仓。
- 笔记本的**电池模式没动**，拔电源仍会睡。要 7×24 请保持插电。

## 排障

```
Get-ScheduledTask V88-*                                   # 看状态
Get-ScheduledTaskInfo V88-遥控常驻                         # 看上次运行结果
Get-Content "$env:USERPROFILE\Desktop\StockAI\win\logs\remote_*.log" -Tail 40
schtasks /end /tn "V88-遥控常驻"; schtasks /run /tn "V88-遥控常驻"   # 手动重启
```

卸载：

```
Unregister-ScheduledTask -TaskName V88-遥控常驻,V88-夜间重启遥控 -Confirm:$false
powercfg /change standby-timeout-ac 30
```
