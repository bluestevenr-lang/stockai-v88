# 常驻2-自动登录方案.ps1 — 【放弃 session-0，改自动登录】2026-08-06 用户批准
#
# ## 为什么推倒 07-30 的 S4U 方案
# 用户复盘:"折腾一天多,Win 最后都没实现『不关机+手机端正常远程』"。
# 根因=S4U「无人登录也跑」与整个栈的假设相反:
#   ① Clash 是**用户级 GUI 应用**,登录才启动 → 无登录=无代理=claude 连不上 Anthropic
#     (07-30 在 Clash 节点上折腾一下午全是弯路——真凶是这个)
#   ② claude remote-control 的交互闸活在用户会话里,session-0 卡住时任务显示
#     Running(267009) 但实际在等输入
#   ③ Git Credential Manager 依赖用户会话 DPAPI
# **新方案=让 Win 永远处于「已登录」状态**——把它变成一台永远醒着且登录着的 Mac,
# 那是唯一验证过可用的形态。夜间 03:30 重启后自动重新登录,链路自愈。
#
# ## 安全代价(用户须知,家用环境自行权衡)
# 自动登录=开机直接进桌面,任何能碰到主机的人都能进系统。
# 缓解:脚本会设置「登录后1分钟自动锁屏」——锁屏不杀会话,Clash/claude 照跑,
# 但屏幕前的人需要密码才能操作。
#
# 用法: 管理员 PowerShell 运行本脚本,然后按屏幕提示做两步人工操作。
$ErrorActionPreference = "Stop"
Write-Host "════ V88 常驻·自动登录方案 ════"

# ── 第1步:电源(永不睡眠;显示器可关) ──
powercfg /change standby-timeout-ac 0
powercfg /change hibernate-timeout-ac 0
powercfg /change monitor-timeout-ac 10
Write-Host "① 电源已设:永不睡眠,显示器10分钟关"

# ── 第2步:改任务触发器 S4U开机 → 用户登录时 ──
$user = "$env:USERDOMAIN\$env:USERNAME"
$act1 = New-ScheduledTaskAction -Execute "$env:USERPROFILE\Desktop\StockAI\win\遥控常驻V88.bat"
$trg1 = New-ScheduledTaskTrigger -AtLogOn -User $user
$trg1.Delay = "PT1M"
$set1 = New-ScheduledTaskSettingsSet -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 5) `
        -ExecutionTimeLimit (New-TimeSpan -Days 0) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName "V88-遥控常驻" -Action $act1 -Trigger $trg1 -Settings $set1 `
        -User $user -RunLevel Highest -Force | Out-Null
Write-Host "② V88-遥控常驻 已改为『登录时+1分钟』触发(跑在真实用户会话,Clash/凭据/交互闸全部可用)"

# 夜间重启保持不变(重启→自动登录→登录触发→链路自愈)
$act2 = New-ScheduledTaskAction -Execute "shutdown" -Argument "/r /t 30 /c V88夜间重启拿新代码"
$trg2 = New-ScheduledTaskTrigger -Daily -At "03:30"
$set2 = New-ScheduledTaskSettingsSet -WakeToRun -AllowStartIfOnBatteries
Register-ScheduledTask -TaskName "V88-夜间重启遥控" -Action $act2 -Trigger $trg2 -Settings $set2 `
        -User $user -RunLevel Highest -Force | Out-Null
Write-Host "③ V88-夜间重启遥控 03:30 已确认(重启后自动登录→任务自动重跑)"

# ── 第3步:登录后自动锁屏(安全缓解;锁屏不杀会话) ──
$act3 = New-ScheduledTaskAction -Execute "rundll32.exe" -Argument "user32.dll,LockWorkStation"
$trg3 = New-ScheduledTaskTrigger -AtLogOn -User $user
$trg3.Delay = "PT1M"
Register-ScheduledTask -TaskName "V88-登录后锁屏" -Action $act3 -Trigger $trg3 `
        -User $user -Force | Out-Null
Write-Host "④ 登录1分钟后自动锁屏(Clash/claude 在锁屏下照常运行)"

Write-Host ""
Write-Host "════ 剩两步必须人手做(我无法代做) ════"
Write-Host "⑤ 开自动登录: Win+R → netplwiz → 取消勾选『要使用本计算机,用户必须输入用户名和密码』"
Write-Host "   → 确定 → 输入两次当前密码。(若无此勾选项,先在注册表把"
Write-Host "   HKLM\SOFTWARE\Microsoft\Windows NT\CurrentVersion\PasswordLess\Device\DevicePasswordLessBuildVersion 设为 0)"
Write-Host "⑥ 确认 Clash 开机自启: Clash 设置里勾选『开机启动』(用户级,登录即起)"
Write-Host ""
Write-Host "验收: 重启一次 → 不碰键盘等3分钟 → 手机 Claude App Code 区应出现 desktop-* 会话。"
Write-Host "     连不上就跑 诊断遥控.ps1,它会指出断在哪一环。"
