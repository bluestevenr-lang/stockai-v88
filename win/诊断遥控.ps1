# 诊断遥控.ps1 — 【Win 遥控链路一键诊断】2026-08-06
# 用户复盘:"上周折腾一天多,Win 最后都没好好实现『不关机+手机端正常远程』"。
# 手机连不上时别瞎猜——跑这个脚本,它逐环检查并指出**断在哪一环**。
# 链路: 登录会话 → Clash代理 → Anthropic可达 → 任务/进程 → 令牌 → git凭据
# 用法: 右键"使用 PowerShell 运行",或 Win 端 Claude 直接跑它。
$ErrorActionPreference = "SilentlyContinue"
$fail = @()
Write-Host "════════ V88 遥控链路诊断 $(Get-Date -Format 'yyyy-MM-dd HH:mm') ════════"

# ① 登录会话 —— 一切的前提(Clash/凭据/交互闸都活在用户会话里)
$sess = (query user 2>$null | Select-String "Active")
if ($sess) { Write-Host "① 登录会话      ✅ 有活动会话" }
else { Write-Host "① 登录会话      ❌ 无人登录 —— **这就是根因**:Clash是用户级应用,无登录=无代理=claude连不上Anthropic。跑 常驻2-自动登录方案.ps1"; $fail += "无登录会话" }

# ② Clash 进程 + 代理端口
$clash = Get-Process | Where-Object { $_.ProcessName -match "clash|verge|mihomo" }
if ($clash) { Write-Host "② Clash进程     ✅ $($clash[0].ProcessName) (PID $($clash[0].Id))" }
else { Write-Host "② Clash进程     ❌ 未运行 → 无代理,claude 必死。检查 Clash 是否设了开机自启(用户级)"; $fail += "Clash未运行" }
$port = $null
foreach ($p in 7890, 7897) {
    if ((Test-NetConnection 127.0.0.1 -Port $p -InformationLevel Quiet -WarningAction SilentlyContinue)) { $port = $p; break }
}
if ($port) { Write-Host "   代理端口      ✅ 127.0.0.1:$port 可连" }
else { Write-Host "   代理端口      ❌ 7890/7897 均不通"; $fail += "代理端口不通" }

# ③ Anthropic 可达性(经代理)
if ($port) {
    try {
        $r = Invoke-WebRequest -Uri "https://api.anthropic.com" -Proxy "http://127.0.0.1:$port" -TimeoutSec 10 -UseBasicParsing
        Write-Host "③ Anthropic     ✅ 经代理可达 (HTTP $($r.StatusCode))"
    } catch {
        if ($_.Exception.Response) { Write-Host "③ Anthropic     ✅ 经代理可达 (HTTP $($_.Exception.Response.StatusCode.value__) — 4xx也算通)" }
        else { Write-Host "③ Anthropic     ❌ 经代理不可达: $($_.Exception.Message)"; $fail += "Anthropic不可达" }
    }
} else { Write-Host "③ Anthropic     ⏭ 跳过(无代理端口)" }

# ④ 计划任务状态 —— 267009=Running 但可能卡在交互闸,别被骗
foreach ($t in "V88-遥控常驻", "V88-夜间重启遥控") {
    $info = schtasks /query /tn $t /fo LIST /v 2>$null | Out-String
    if ($info -match "Last Result:\s+(\S+)") {
        $rc = $Matches[1]
        $tag = if ($rc -eq "0") { "✅ 上次成功" } elseif ($rc -eq "267009") { "⚠️ 267009=正在运行(可能卡在交互闸,看⑤是否真有claude进程)" } else { "❌ 上次结果 $rc" }
        Write-Host "④ 任务 $t  $tag"
    } else { Write-Host "④ 任务 $t  ❌ 不存在"; $fail += "$t 不存在" }
}

# ⑤ claude 进程真伪
$cl = Get-Process | Where-Object { $_.ProcessName -match "^claude$|^node$" }
if ($cl) { Write-Host "⑤ claude进程    ✅ $($cl.Count) 个相关进程" }
else { Write-Host "⑤ claude进程    ❌ 无 —— 任务若显示Running即为假象(卡交互闸)"; $fail += "claude进程不存在" }

# ⑥ 令牌新鲜度(worker-403 前科:messages通但worker 403 → /logout+/login 换牌,别追网络)
$cred = Get-ChildItem "$env:USERPROFILE\.claude" -Filter "*credential*" -Recurse 2>$null | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if ($cred) {
    $age = [int]((Get-Date) - $cred.LastWriteTime).TotalDays
    Write-Host "⑥ 令牌文件      $(if ($age -le 7) {'✅'} else {'⚠️'}) 最后更新 $age 天前 $(if ($age -gt 7) {'(偏旧;若手机卡Allocating sandbox → /logout+/login 换牌,五分钟,别折腾节点)'})"
} else { Write-Host "⑥ 令牌文件      ⚠️ 未找到(可能未登录过 claude)" }

# ⑦ git 凭据(私仓 pull)
Set-Location "$env:USERPROFILE\Desktop\ai-daily-report-v2" 2>$null
if ($?) {
    git fetch --dry-run 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { Write-Host "⑦ 私仓git凭据   ✅ fetch 可用" }
    else { Write-Host "⑦ 私仓git凭据   ❌ fetch 失败(S4U下DPAPI取不到凭据是已知病;自动登录方案可解)"; $fail += "git凭据失败" }
} else { Write-Host "⑦ 私仓git凭据   ❌ 仓库目录不存在" }

Write-Host "════════════════════════════════════════"
if ($fail.Count -eq 0) { Write-Host "🎉 全链路通。若手机仍连不上 → 大概率是 worker-403 令牌问题:/logout + /login" }
else { Write-Host "🔴 断点: $($fail -join ' → ')　按①→⑦顺序修,上游断了下游全是陪葬" }
