$ErrorActionPreference = 'Stop'

Write-Host ''
Write-Host 'V88 OpenClaw 飞书主机切换' -ForegroundColor Cyan
Write-Host '只有在 Mac OpenClaw Gateway 已停止后才能继续，否则两台会抢同一条消息。' -ForegroundColor Yellow
$confirm = Read-Host '确认 Mac 网关已停止，请输入 CUTOVER'
if ($confirm -ne 'CUTOVER') {
    Write-Host '已取消，没有修改飞书配置。'
    Read-Host '按回车关闭'
    exit 0
}

$Openclaw = (Get-Command openclaw -ErrorAction SilentlyContinue).Source
if (-not $Openclaw) {
    $cand = @(
        (Join-Path $env:APPDATA 'npm\openclaw.cmd'),
        (Join-Path $env:ProgramFiles 'nodejs\openclaw.cmd'),
        (Join-Path ${env:ProgramFiles(x86)} 'nodejs\openclaw.cmd')
    )
    foreach ($c in $cand) { if (Test-Path $c) { $Openclaw = $c; break } }
}
if (-not $Openclaw) { throw '找不到 openclaw。请先运行安装OpenClaw-双击我.bat。' }

$appId = (Read-Host '请输入飞书应用 App ID').Trim()
if (-not $appId) { throw 'App ID 不能为空。' }
$secureSecret = Read-Host '请输入飞书应用 App Secret（输入内容不会显示）' -AsSecureString
$bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureSecret)
$plainSecret = $null
try {
    $plainSecret = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    if (-not $plainSecret) { throw 'App Secret 不能为空。' }
    $patch = @{
        channels = @{
            feishu = @{
                enabled = $true
                connectionMode = 'websocket'
                domain = 'feishu'
                dmPolicy = 'pairing'
                groupPolicy = 'disabled'
                requireMention = $true
                accounts = @{
                    default = @{
                        appId = $appId
                        appSecret = $plainSecret
                        enabled = $true
                    }
                }
            }
        }
    }
    $json = $patch | ConvertTo-Json -Depth 12 -Compress
    $json | & $Openclaw config patch --stdin
    if ($LASTEXITCODE -ne 0) { throw '写入飞书本机配置失败。' }
} finally {
    $plainSecret = $null
    if ($bstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}

& $Openclaw plugins enable feishu
if ($LASTEXITCODE -ne 0) { throw '启用飞书插件失败。' }
& $Openclaw agents bind --agent v88-mobile --bind feishu:default
if ($LASTEXITCODE -ne 0) { throw '绑定 v88-mobile 到飞书失败。' }
& $Openclaw config validate
if ($LASTEXITCODE -ne 0) { throw 'OpenClaw 配置校验失败。' }
& $Openclaw gateway restart
if ($LASTEXITCODE -ne 0) { throw 'Gateway 重启失败。' }
& $Openclaw channels status --probe

Write-Host ''
Write-Host 'Win 飞书网关已启用。现在从手机给机器人发一句“状态”。' -ForegroundColor Green
Write-Host '若机器人返回配对码，请在 Win 运行：openclaw pairing approve feishu <配对码>' -ForegroundColor Yellow
Read-Host '按回车关闭'
