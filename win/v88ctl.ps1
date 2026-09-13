# V88 遥控 wrapper —— OpenClaw 代理唯一允许执行的脚本
# 子命令: start(启动V88) / url(手机链接) / sync(git同步) / review(订阅双审) / status(状态)
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('start','url','sync','review','status')]
    [string]$Command
)
$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot
$ReportRoot = Join-Path $env:USERPROFILE 'Desktop\ai-daily-report-v2'
$ToolsDir = Join-Path $env:USERPROFILE '.openclaw\tools'
$CfExe    = Join-Path $ToolsDir 'cloudflared.exe'
$CfLog    = Join-Path $ToolsDir 'cloudflared.log'
$PyLauncher = 'C:\Users\admin\AppData\Local\Programs\Python\Launcher\py.exe'
$V88Python = 'C:\Users\admin\v88env\Scripts\python.exe'
$Projection = Join-Path $RepoRoot 'win\openclaw-v88\sync_v88_projection_win.py'
$MobileContext = Join-Path $env:USERPROFILE '.openclaw\workspaces\v88-mobile\context'
$GptWorkspace = Join-Path $env:USERPROFILE '.openclaw\workspaces\v88-gpt'
$GptContext = Join-Path $GptWorkspace 'context'
$GptKnowledge = Join-Path $GptWorkspace 'knowledge\v88-claude-memory'

function Test-V88Up {
    try {
        $null = Invoke-WebRequest -Uri 'http://127.0.0.1:8501' -UseBasicParsing -TimeoutSec 4
        return $true
    } catch { return $false }
}

function Start-V88 {
    if (Test-V88Up) { Write-Output 'V88 已在运行 (http://127.0.0.1:8501)'; return }
    $app = Join-Path $RepoRoot 'app_v88_integrated.py'
    if (-not (Test-Path $app)) { throw "找不到 $app" }
    if (-not (Test-Path -LiteralPath $PyLauncher)) { throw "找不到Python启动器 $PyLauncher" }
    Start-Process $PyLauncher -WindowStyle Hidden -WorkingDirectory $RepoRoot -ArgumentList @(
        '-3','-m','streamlit','run','app_v88_integrated.py',
        '--server.address','127.0.0.1','--server.headless','true','--server.port','8501'
    )
    Write-Output 'V88 启动中（首次加载约1-2分钟）...'
    for ($i=0; $i -lt 40; $i++) {
        Start-Sleep -Seconds 3
        if (Test-V88Up) { Write-Output 'V88 已就绪 (http://127.0.0.1:8501)'; return }
    }
    throw 'V88 启动超时（120秒未响应），请人工查看。'
}

function Get-TunnelUrl {
    if (-not (Get-Process cloudflared -ErrorAction SilentlyContinue)) { return $null }
    if (Test-Path $CfLog) {
        $m = Select-String -Path $CfLog -Pattern 'https://[a-z0-9-]+\.trycloudflare\.com' -AllMatches |
             Select-Object -Last 1
        if ($m) { return $m.Matches[0].Value }
    }
    return $null
}

function New-TunnelUrl {
    if (-not (Test-Path $CfExe)) { throw "缺少 $CfExe（请先运行 install_openclaw_win.ps1）" }
    $running = Get-Process cloudflared -ErrorAction SilentlyContinue
    if ($running) {
        $u = Get-TunnelUrl
        if ($u) { Write-Output $u; return }
        $running | Stop-Process -Force; Start-Sleep -Seconds 2
    }
    Remove-Item $CfLog -Force -ErrorAction SilentlyContinue
    Start-Process $CfExe -WindowStyle Hidden -ArgumentList @(
        'tunnel','--url','http://127.0.0.1:8501','--logfile',$CfLog,'--loglevel','info'
    )
    for ($i=0; $i -lt 20; $i++) {
        Start-Sleep -Seconds 2
        $u = Get-TunnelUrl
        if ($u) { Write-Output $u; return }
    }
    throw '隧道 URL 生成超时，请人工运行 cloudflared 排查。'
}

function Sync-V88Data {
    if (-not (Test-Path -LiteralPath (Join-Path $ReportRoot '.git'))) {
        throw "找不到 V88 私仓 $ReportRoot"
    }
    if (-not (Test-Path -LiteralPath $Projection)) { throw "找不到投影脚本 $Projection" }
    if (-not (Test-Path -LiteralPath $PyLauncher)) { throw "找不到Python启动器 $PyLauncher" }

    # Windows PowerShell 5 turns benign native stderr (for example Git's
    # "From ...") into an ErrorRecord when the script-wide preference is Stop.
    # Capture native output under Continue, then judge the real process code.
    $previousNativePreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $publicPullRaw = & git -C $RepoRoot pull --ff-only origin main 2>&1
        $publicPullExit = $LASTEXITCODE
        $publicPullOutput = @($publicPullRaw | ForEach-Object { $_.ToString() })
    } finally {
        $ErrorActionPreference = $previousNativePreference
    }
    if ($publicPullExit -ne 0) {
        throw "V88 公仓同步失败：$($publicPullOutput | Select-Object -Last 8 | Out-String)"
    }

    $pythonForGit = 'C:/Users/admin/v88env/Scripts/python.exe'
    if (-not (Test-Path -LiteralPath $pythonForGit)) {
        throw "找不到 V88 Python $pythonForGit"
    }
    & git -C $ReportRoot config merge.v88json.name 'V88 JSON newest-timestamp-wins'
    & git -C $ReportRoot config merge.v88json.driver "`"$pythonForGit`" scripts/v88_json_merge.py %O %A %B %P"
    & git -C $ReportRoot config merge.v88jsonl.name 'V88 JSONL lossless-union'
    & git -C $ReportRoot config merge.v88jsonl.driver "`"$pythonForGit`" scripts/v88_jsonl_merge_driver.py %O %A %B %P"

    try {
        $ErrorActionPreference = 'Continue'
        $pullRaw = & git -C $ReportRoot pull --rebase --autostash origin main 2>&1
        $pullExit = $LASTEXITCODE
        $pullOutput = @($pullRaw | ForEach-Object { $_.ToString() })
    } finally {
        $ErrorActionPreference = $previousNativePreference
    }
    $previousPythonUtf8 = $env:PYTHONUTF8
    $env:PYTHONUTF8 = '1'
    Push-Location $ReportRoot
    try {
        try {
            $ErrorActionPreference = 'Continue'
            $postPullRaw = & $PyLauncher -3 (Join-Path $ReportRoot 'scripts\v88_json_merge.py') --postpull 2>&1
            $postPullExit = $LASTEXITCODE
            $postPullOutput = @($postPullRaw | ForEach-Object { $_.ToString() })
        } finally {
            $ErrorActionPreference = $previousNativePreference
        }
    } finally {
        Pop-Location
        $env:PYTHONUTF8 = $previousPythonUtf8
    }
    $unresolved = @(& git -C $ReportRoot diff --name-only --diff-filter=U)
    if ($pullExit -ne 0 -or $postPullExit -ne 0 -or $unresolved.Count -gt 0) {
        throw "V88 私仓安全同步失败：pull=$pullExit postpull=$postPullExit unresolved=$($unresolved.Count)`n$($pullOutput | Select-Object -Last 8 | Out-String)$($postPullOutput | Select-Object -Last 8 | Out-String)"
    }

    foreach ($destination in @($MobileContext, $GptContext)) {
        try {
            $ErrorActionPreference = 'Continue'
            $projectionRaw = & $PyLauncher -3 $Projection --source (Join-Path $ReportRoot 'data') --dest $destination 2>&1
            $projectionExit = $LASTEXITCODE
            $projectionOutput = @($projectionRaw | ForEach-Object { $_.ToString() })
        } finally {
            $ErrorActionPreference = $previousNativePreference
        }
        if ($projectionExit -ne 0) {
            throw "V88 持仓投影失败（$destination）：$($projectionOutput | Out-String)"
        }
    }

    # 旧版曾把 claude-memory 整目录复制给 OpenClaw，其中可能含账户结构、成本和
    # 资产目标。远端会审只需要下面的 AGENTS 纪律与脱敏数据投影；先清掉历史镜像，
    # 此后不再复制任何私有记忆正文。
    if (Test-Path -LiteralPath $GptKnowledge) {
        Remove-Item -LiteralPath $GptKnowledge -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $GptKnowledge | Out-Null
    $gptInstructions = Join-Path $RepoRoot 'win\openclaw-v88\AGENTS-GPT.md'
    if (Test-Path -LiteralPath $gptInstructions) {
        New-Item -ItemType Directory -Force -Path $GptWorkspace | Out-Null
        Copy-Item -LiteralPath $gptInstructions -Destination `
            (Join-Path $GptWorkspace 'AGENTS.md') -Force
    }
    $mobileInstructions = Join-Path $RepoRoot 'win\openclaw-v88\AGENTS.md'
    if (Test-Path -LiteralPath $mobileInstructions) {
        $mobileWorkspace = Split-Path -Parent $MobileContext
        New-Item -ItemType Directory -Force -Path $mobileWorkspace | Out-Null
        Copy-Item -LiteralPath $mobileInstructions -Destination `
            (Join-Path $mobileWorkspace 'AGENTS.md') -Force
    }

    $portfolioPath = Join-Path $GptContext 'modules\portfolio_pub.json'
    # Windows PowerShell 5 defaults BOM-less UTF-8 to the legacy ANSI codepage.
    # The projection writes canonical UTF-8 JSON, so decode it explicitly.
    $portfolio = Get-Content -LiteralPath $portfolioPath -Raw -Encoding UTF8 | ConvertFrom-Json
    Write-Output (($pullOutput | Select-Object -Last 3 | Out-String).Trim())
    Write-Output "V88 GPT-6/经典巨著 快照已更新：$(@($portfolio.items).Count) 只持仓，持仓源时间 $($portfolio.updated_at)"
    Write-Output 'GPT-6/经典巨著纪律与只读脱敏数据投影均已同步；私有记忆正文未复制。'
}

function Invoke-V88SubscriptionReview {
    # 先拉取最新代码和事实。模型子进程本身还会清除按量API覆盖；这里再做一层
    # Windows 入口硬隔离，确保只使用现有 Codex OAuth 订阅。
    $null = @(Sync-V88Data)
    if (-not (Test-Path -LiteralPath $V88Python)) {
        throw "找不到 V88 Python $V88Python"
    }
    # 不只清理已知供应商名称：任何 API key、访问令牌或自定义端点都可能把
    # 本机订阅 CLI 误导到按量接口。Codex 的 OAuth 凭据在各自配置目录中，
    # 不依赖这些环境变量。
    Get-ChildItem Env: | Where-Object {
        $_.Name -match '(?i)(API_KEY|ACCESS_TOKEN|AUTH_TOKEN|BASE_URL|API_BASE|ENDPOINT)'
    } | ForEach-Object {
        Remove-Item -Path ("Env:" + $_.Name) -ErrorAction SilentlyContinue
    }
    foreach ($name in @('V88_DISABLE_LLM','GITHUB_ACTIONS','ANALYSIS_PROVIDER')) {
        Remove-Item -Path "Env:$name" -ErrorAction SilentlyContinue
    }
    $env:V88_GPT_MODEL = 'gpt-6-astra'
    $reviewStartedAt = Get-Date
    $previousPythonUtf8 = $env:PYTHONUTF8
    $previousNativePreference = $ErrorActionPreference
    $env:PYTHONUTF8 = '1'
    Push-Location $ReportRoot
    try {
        try {
            $ErrorActionPreference = 'Continue'
            $reviewRaw = & $V88Python (Join-Path $ReportRoot 'src\review_pipeline.py') `
                review --trigger scheduled --limit-batches 5 2>&1
            $reviewExit = $LASTEXITCODE
            $reviewOutput = @($reviewRaw | ForEach-Object { $_.ToString() })
        } finally {
            $ErrorActionPreference = $previousNativePreference
        }
    } finally {
        Pop-Location
        $env:PYTHONUTF8 = $previousPythonUtf8
    }
    if ($reviewExit -ne 0) {
        throw "V88 GPT-6/经典巨著订阅双审失败：$($reviewOutput | Select-Object -Last 12 | Out-String)"
    }

    # review_pipeline 的状态必须证明当前模型审核成功；只有本轮状态文件明确证明双方
    # 同包晋升，才能宣称“双审完成”。
    $statusPath = Join-Path $ReportRoot 'data\dual_cli_review.json'
    if (-not (Test-Path -LiteralPath $statusPath)) {
        throw 'V88双审未生成状态文件，保持PENDING。'
    }
    $statusFile = Get-Item -LiteralPath $statusPath
    $status = Get-Content -LiteralPath $statusPath -Raw | ConvertFrom-Json
    if ($statusFile.LastWriteTimeUtc -lt $reviewStartedAt.ToUniversalTime().AddSeconds(-2) `
            -or $status.status -ne 'complete' -or $status.model -ne 'gpt-6-astra' `
            -or -not $status.sell_promotion_ok -or [int]$status.unreviewed_count -gt 0) {
        throw "V88双审失败关闭：状态=$($status.status)，未形成可用同包裁决，保持PENDING。"
    }

    foreach ($destination in @($MobileContext, $GptContext)) {
        $projectionOutput = & $PyLauncher -3 $Projection `
            --source (Join-Path $ReportRoot 'data') --dest $destination 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "双审完成但投影刷新失败（$destination）：$($projectionOutput | Out-String)"
        }
    }
    $selectionPath = Join-Path $ReportRoot 'data\triad_selection.json'
    $selection = Get-Content -LiteralPath $selectionPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ([string]::IsNullOrWhiteSpace([string]$status.factpack_id) `
            -or [string]$selection.factpack_id -ne [string]$status.factpack_id) {
        throw 'V88双审与中央裁决事实包不一致，保持PENDING。'
    }
    $nowCount = @($selection.recommendations).Count
    $prepCount = @($selection.preparations).Count
    $blockedCount = @($selection.blocked_3a).Count
    $conditionalCount = @($selection.conditional).Count
    $researchCount = @($selection.observations).Count
    $pendingCount = @($selection.pending).Count
    Write-Output ("V88订阅双审完成：3A现买{0}、3A准备{1}、3A冻结{2}、2A条件{3}、研究/分歧{4}、待审{5}。" -f `
        $nowCount,$prepCount,$blockedCount,$conditionalCount,$researchCount,$pendingCount)
    Write-Output '路由：Codex OAuth + 独立经典规则；按量API与fallback均已禁用。'
}

switch ($Command) {
    'start'  { Start-V88 }
    'url'    {
        Start-V88 | Out-Null
        $u = New-TunnelUrl
        Write-Output "手机访问链接: $u"
        Write-Output '⚠️ 链接即钥匙，请勿转发；重启隧道后旧链接失效。'
    }
    'sync'   { Sync-V88Data }
    'review' { Invoke-V88SubscriptionReview }
    'status' {
        $v88 = if (Test-V88Up) { '运行中' } else { '未运行' }
        $u = Get-TunnelUrl
        $gw = (Get-ScheduledTask -TaskName 'OpenClaw Gateway' -ErrorAction SilentlyContinue).State
        Write-Output "V88页面: $v88"
        Write-Output "隧道链接: $(if ($u) { $u } else { '无' })"
        Write-Output "OpenClaw网关: $gw"
    }
}
