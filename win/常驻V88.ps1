# ══════════════════════════════════════════════════════════════
#  V88 Windows 主机化 一键安装（第四终端 → 升级为常驻主机）— 2026-07-30
#
#  由来：用户 2026-07-30 决定「把 Win 作为主机，Mac 可开可关，对手机帮助最大化」。
#  Windows 相对 macOS 的结构性优势：任务计划程序可「不管用户是否登录都运行」，
#  而 macOS LaunchAgent 必须有登录会话（Mac 重启未自动登录就全哑）。
#
#  用法：管理员 PowerShell 里执行
#    powershell -ExecutionPolicy Bypass -File .\常驻V88.ps1
#  幂等：重复执行只覆盖任务定义，不会产生重复任务。
#
#  本脚本做三件事：
#    ① 电源：交流电下永不睡眠 / 关休眠 / 屏幕 15 分钟黑
#    ② 注册「V88-遥控常驻」：开机自起 + 崩溃自愈 + 无人登录也跑
#    ③ 注册「V88-夜间重启遥控」：每天 03:30 踢一次，强制拿最新代码
#
#  卸载：见文件末尾注释。
# ══════════════════════════════════════════════════════════════
$ErrorActionPreference = "Stop"

function Ok($m)   { Write-Host "  [OK]   $m" -ForegroundColor Green }
function Warn($m) { Write-Host "  [警告] $m" -ForegroundColor Yellow }
function Die($m)  { Write-Host "  [失败] $m" -ForegroundColor Red; exit 1 }

Write-Host ""
Write-Host "════ V88 Windows 主机化安装 ════" -ForegroundColor Cyan

# ── 0. 前置检查 ────────────────────────────────────────────────
$isAdmin = ([Security.Principal.WindowsPrincipal] `
            [Security.Principal.WindowsIdentity]::GetCurrent() `
           ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
  Die "请用【管理员】PowerShell 运行。开始菜单搜 PowerShell → 右键 → 以管理员身份运行。"
}
Ok "管理员权限"

# 桌面路径：必须用 GetFolderPath，不能拼 $env:USERPROFILE\Desktop
# —— OneDrive 会把桌面重定向到 %USERPROFILE%\OneDrive\Desktop，硬拼路径会找不到仓库
$Desk = [Environment]::GetFolderPath('Desktop')
if (-not $Desk) { $Desk = "$env:USERPROFILE\Desktop" }

$StockAI = "$Desk\StockAI"
$Report  = "$Desk\ai-daily-report-v2"

# 兜底：真实桌面没有就回退到硬拼路径（老装法的位置）
if (-not (Test-Path $StockAI) -and (Test-Path "$env:USERPROFILE\Desktop\StockAI")) {
  $StockAI = "$env:USERPROFILE\Desktop\StockAI"
  $Report  = "$env:USERPROFILE\Desktop\ai-daily-report-v2"
}
Write-Host "  [路径] 桌面 = $Desk"
Write-Host "  [路径] StockAI = $StockAI"
$SvcBat  = "$StockAI\win\遥控常驻V88.bat"
$NightBat= "$StockAI\win\夜间重启遥控.bat"

if (-not (Test-Path $StockAI))  { Die "找不到 $StockAI —— 先跑 初始化V88.ps1" }
if (-not (Test-Path $Report))   { Warn "找不到私仓 $Report —— 遥控能起，但拿不到持仓/日报数据，建议先跑 初始化V88.ps1" }
if (-not (Test-Path $SvcBat))   { Die "找不到 $SvcBat —— 先双击 同步V88.bat 拉取最新代码" }
if (-not (Test-Path $NightBat)) { Die "找不到 $NightBat —— 先双击 同步V88.bat 拉取最新代码" }
Ok "脚本齐全"

if (-not (Get-Command git -ErrorAction SilentlyContinue))    { Die "缺少 git" }

# Claude Code 的原生安装装到 %USERPROFILE%\.local\bin，但装完不会自动进 PATH。
# 这里主动找它并把目录永久写进用户 PATH，省掉手点「系统属性→环境变量」。
$ClaudeBin = "$env:USERPROFILE\.local\bin"
$claudeCmd = Get-Command claude -ErrorAction SilentlyContinue
if (-not $claudeCmd -and (Test-Path "$ClaudeBin\claude.exe")) {
  $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
  if ($userPath -notlike "*$ClaudeBin*") {
    [Environment]::SetEnvironmentVariable("Path", "$userPath;$ClaudeBin", "User")
    Ok "已把 $ClaudeBin 写进用户 PATH（新开的终端才生效）"
  }
  $env:Path = "$env:Path;$ClaudeBin"      # 本次会话立即可用
  $claudeCmd = Get-Command claude -ErrorAction SilentlyContinue
}

if ($claudeCmd) {
  Ok "Claude Code 已安装: $($claudeCmd.Source)"
  # 登录凭据在 %USERPROFILE%\.claude 下；没登录过的话无人登录模式拿不到凭据
  if (-not (Test-Path "$env:USERPROFILE\.claude")) {
    Warn "还没登录过 Claude Code。装完任务后请手动跑一次 claude 登录（与手机同账号），否则遥控起不来。"
  }
} else {
  Warn "未装 Claude Code。任务照样注册，但遥控起不来。装法："
  Write-Host "         irm https://claude.ai/install.ps1 | iex" -ForegroundColor Gray
  Write-Host "       装完必须先手动跑一次 claude 登录（用与手机相同的账号），" -ForegroundColor Gray
  Write-Host "       否则无人登录模式下拿不到凭据。" -ForegroundColor Gray
}

# ── 1. 电源：让这台机器不睡 ─────────────────────────────────────
Write-Host ""
Write-Host "── ① 电源设置 ──" -ForegroundColor Cyan
powercfg /change standby-timeout-ac   0   | Out-Null   # 交流电永不睡眠
powercfg /change hibernate-timeout-ac 0   | Out-Null   # 交流电永不休眠
powercfg /change disk-timeout-ac      0   | Out-Null   # 硬盘不断电
powercfg /change monitor-timeout-ac   15  | Out-Null   # 屏幕 15 分钟黑（不影响后台）
powercfg /hibernate off                   | Out-Null   # 彻底关休眠，省一份 sleepimage
Ok "交流电下：永不睡眠 / 永不休眠 / 屏幕 15 分钟黑"

$batt = Get-CimInstance Win32_Battery -ErrorAction SilentlyContinue
if ($batt) {
  Warn "检测到电池（笔记本）。电池模式【没动】——拔电源仍会睡。要 7x24 请保持插电。"
} else {
  Ok "无电池（台式机），插电即常驻"
}
Warn "断电自动开机需进 BIOS 打开 [Restore on AC Power Loss]，脚本改不了。"

# ── 2. 注册遥控常驻任务 ────────────────────────────────────────
Write-Host ""
Write-Host "── ② 注册 V88-遥控常驻 ──" -ForegroundColor Cyan

$TaskMain  = "V88-遥控常驻"
$TaskNight = "V88-夜间重启遥控"
$me = "$env:USERDOMAIN\$env:USERNAME"

foreach ($t in @($TaskMain, $TaskNight)) {
  if (Get-ScheduledTask -TaskName $t -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $t -Confirm:$false
    Ok "清掉旧任务 $t（幂等）"
  }
}

$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$SvcBat`""

# 开机后延迟 1 分钟起，等网络/代理就绪
$trig = New-ScheduledTaskTrigger -AtStartup
$trig.Delay = "PT1M"

# S4U = 不管用户是否登录都运行，且不需要保存密码（这就是打赢 macOS LaunchAgent 的关键）
$principal = New-ScheduledTaskPrincipal -UserId $me -LogonType S4U -RunLevel Highest

$settings = New-ScheduledTaskSettingsSet `
  -MultipleInstances IgnoreNew `
  -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 5) `
  -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
  -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
  -StartWhenAvailable

Register-ScheduledTask -TaskName $TaskMain -Action $action -Trigger $trig `
  -Principal $principal -Settings $settings `
  -Description "V88 手机遥控常驻：开机自起、崩溃自愈、无人登录也跑。日志 win\logs\remote_*.log" | Out-Null
Ok "$TaskMain 已注册（开机+1min / 失败每5分钟重试3次 / 运行时长不限）"

# ── 3. 注册夜间重启任务（撞维护窗 03:00-04:30）──────────────────
Write-Host ""
Write-Host "── ③ 注册 V88-夜间重启遥控 ──" -ForegroundColor Cyan

$actionN = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$NightBat`""
$trigN   = New-ScheduledTaskTrigger -Daily -At "03:30"
$setN    = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew `
             -StartWhenAvailable -WakeToRun `
             -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

Register-ScheduledTask -TaskName $TaskNight -Action $actionN -Trigger $trigN `
  -Principal $principal -Settings $setN `
  -Description "每天 03:30（V88 维护窗）踢一次遥控，强制拉最新代码换新会话" | Out-Null
Ok "$TaskNight 已注册（每天 03:30，可唤醒计算机）"

# ── 4. 立刻起一次并验收 ────────────────────────────────────────
Write-Host ""
Write-Host "── ④ 启动并验收 ──" -ForegroundColor Cyan
Start-ScheduledTask -TaskName $TaskMain
Start-Sleep -Seconds 12

Get-ScheduledTask -TaskName $TaskMain, $TaskNight |
  Select-Object TaskName, State |
  Format-Table -AutoSize

$log = Get-ChildItem "$StockAI\win\logs\remote_*.log" -ErrorAction SilentlyContinue |
       Sort-Object LastWriteTime -Descending | Select-Object -First 1
if ($log) {
  Write-Host "最新日志 $($log.Name) 尾部：" -ForegroundColor Gray
  Get-Content $log.FullName -Tail 12 | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
} else {
  Warn "还没生成日志，等 1 分钟后看 win\logs\"
}

Write-Host ""
Write-Host "════ 完成 ════" -ForegroundColor Cyan
Write-Host "手机验收：Claude App → Code 区 → 应能看到这台 Win（电脑图标 + 绿点）。" -ForegroundColor White
Write-Host "开场对 Win 端 Claude 说：按 win/README_WIN.md 与私仓 claude-memory/ 接管 V88。" -ForegroundColor White
Write-Host ""
Write-Host "查看状态: Get-ScheduledTask V88-*" -ForegroundColor Gray
Write-Host "手动重启: schtasks /end /tn `"V88-遥控常驻`"; schtasks /run /tn `"V88-遥控常驻`"" -ForegroundColor Gray
Write-Host "卸载    : Unregister-ScheduledTask -TaskName V88-遥控常驻,V88-夜间重启遥控 -Confirm:`$false" -ForegroundColor Gray
Write-Host "          powercfg /change standby-timeout-ac 30   # 恢复默认睡眠" -ForegroundColor Gray
Write-Host ""
