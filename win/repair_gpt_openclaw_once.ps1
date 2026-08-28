# One-shot recovery for the GPT OpenClaw subscription route.
# It never prints credentials and never enables API-key/PAYG fallback.
$ErrorActionPreference = 'Continue'

# Disabled on 2026-08-28.  The old one-shot repair guessed model routes, could
# report success after a failed route probe, and published raw diagnostics to a
# public repository.  A clean rebuild must follow the reviewed handoff instead.
Write-Error 'This legacy repair is disabled. Read win/V88_WIN_REBUILD_HANDOFF_20260828.md and perform a clean, local-only recovery.'
exit 12

$Repo = Split-Path -Parent $PSScriptRoot
$State = Join-Path $env:USERPROFILE '.openclaw'
$Config = Join-Path $State 'openclaw.json'
$Done = Join-Path $State 'gpt_openclaw_recovery_20260827.done'
$Report = Join-Path $PSScriptRoot 'CODEX_WIN_OPENCLAW_AUTO_REPORT_20260827.md'

if (Test-Path $Done) {
    Write-Output '[OpenClaw remote recovery] already completed.'
    exit 0
}

$OpenClaw = $null
foreach ($candidate in @(
    "$env:APPDATA\npm\openclaw.cmd",
    "$env:LOCALAPPDATA\npm\openclaw.cmd",
    "$env:ProgramFiles\nodejs\openclaw.cmd"
)) {
    if ($candidate -and (Test-Path $candidate)) {
        $OpenClaw = $candidate
        break
    }
}

$Lines = [System.Collections.Generic.List[string]]::new()
function Add-Line([string]$Value) {
    if ($null -eq $Value) { return }
    $safe = $Value
    $safe = $safe -replace '(?i)sk-[A-Za-z0-9_-]{8,}', '[REDACTED]'
    $safe = $safe -replace '(?i)(appSecret|apiKey|accessToken|refreshToken|token)(\s*[=:]\s*)\S+', '$1$2[REDACTED]'
    $Lines.Add($safe)
}

function Run-OpenClaw([string]$Label, [string[]]$Arguments) {
    Add-Line ''
    Add-Line ("## {0}" -f $Label)
    if (-not $OpenClaw) {
        Add-Line 'OPENCLAW_CLI_NOT_FOUND'
        return @('OPENCLAW_CLI_NOT_FOUND')
    }
    $output = @(& $OpenClaw @Arguments 2>&1 | ForEach-Object { "$_" })
    if ($output.Count -eq 0) { $output = @('(no output)') }
    foreach ($line in $output) { Add-Line $line }
    Add-Line ("exit_code={0}" -f $LASTEXITCODE)
    return $output
}

function Set-SubscriptionModel([string]$Model) {
    if (-not (Test-Path $Config)) {
        Add-Line 'MODEL_REPAIR_CONFIG_MISSING'
        return $false
    }
    try {
        $doc = Get-Content -Raw -Encoding UTF8 $Config | ConvertFrom-Json
        $agents = @($doc.agents.list)
        $agentIndex = -1
        for ($i = 0; $i -lt $agents.Count; $i++) {
            if ($agents[$i].id -eq 'v88-gpt') { $agentIndex = $i; break }
        }
        if ($agentIndex -lt 0) {
            Add-Line 'MODEL_REPAIR_AGENT_NOT_FOUND'
            return $false
        }
        $batch = @(
            @{ path = "agents.list[$agentIndex].model"; value = $Model },
            @{ path = "agents.defaults.models[`"$Model`"].agentRuntime"; value = @{ id = 'codex' } },
            @{ path = 'plugins.entries.codex.enabled'; value = $true }
        )
        $batchPath = Join-Path $env:TEMP 'v88_gpt_subscription_repair.json'
        $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
        [System.IO.File]::WriteAllText(
            $batchPath,
            ($batch | ConvertTo-Json -Depth 12),
            $utf8NoBom
        )
        try {
            $output = @(& $OpenClaw config set --batch-file $batchPath 2>&1 | ForEach-Object { "$_" })
            $exitCode = $LASTEXITCODE
            foreach ($line in $output) { Add-Line $line }
            Add-Line ("set_model={0}; runtime=codex; exit_code={1}" -f $Model, $exitCode)
            return ($exitCode -eq 0)
        } finally {
            Remove-Item -Force $batchPath -ErrorAction SilentlyContinue
        }
    } catch {
        Add-Line ("MODEL_REPAIR_ERROR={0}" -f $_.Exception.Message)
        return $false
    }
}

function Test-SubscriptionModel([string]$Model) {
    Add-Line ''
    Add-Line ("## Subscription route probe: {0}" -f $Model)
    $output = @(& $OpenClaw agent --agent v88-gpt -m 'Reply with exactly V88_ROUTE_READY.' --json 2>&1 |
        ForEach-Object { "$_" })
    $exitCode = $LASTEXITCODE
    foreach ($line in $output) { Add-Line $line }
    $joined = $output -join "`n"
    $ok = ($exitCode -eq 0 -and $joined -match 'V88_ROUTE_READY' -and
        $joined -notmatch '(?i)(model is unavailable|configured model is unavailable|api[_ -]?key|billing)')
    Add-Line ("probe_model={0}; ok={1}; exit_code={2}" -f $Model, $ok, $exitCode)
    return $ok
}

Add-Line '# Win GPT OpenClaw subscription-route recovery report'
Add-Line ''
Add-Line ("- generated_at: {0}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz'))
Add-Line ("- host: {0}" -f $env:COMPUTERNAME)
Add-Line ("- cli_found: {0}" -f [bool]$OpenClaw)
Add-Line ("- config_exists: {0}" -f (Test-Path $Config))

if (-not $OpenClaw) {
    Add-Line '- root_cause: OpenClaw CLI is not installed in the expected Windows locations.'
} else {
    $env:OPENCLAW_CONFIG_PATH = $Config
    $env:OPENCLAW_STATE_DIR = $State
    $env:OPENCLAW_HOME = $State
    $env:CLAWDBOT_STATE_DIR = $State

    # Zero-PAYG hard gate: keep OAuth files on disk but remove every common
    # API-key/custom-endpoint variable from this process and its children.
    Get-ChildItem Env: | Where-Object {
        $_.Name -match '(?i)(API_KEY|ACCESS_TOKEN|AUTH_TOKEN|BASE_URL|API_BASE|ENDPOINT)'
    } | ForEach-Object {
        Remove-Item ("Env:{0}" -f $_.Name) -ErrorAction SilentlyContinue
    }

    Run-OpenClaw 'Doctor repair' @('doctor', '--fix') | Out-Null

    $routeOk = $false
    if (Set-SubscriptionModel 'openai/gpt-5.6-sol') {
        Run-OpenClaw 'Gateway restart for GPT-5.6 Sol' @('gateway', 'restart') | Out-Null
        Start-Sleep -Seconds 8
        $routeOk = Test-SubscriptionModel 'openai/gpt-5.6-sol'
    }
    if (-not $routeOk) {
        Add-Line 'GPT-5.6 Sol is not exposed to this OAuth workspace; trying the official subscription recovery model.'
        if (Set-SubscriptionModel 'openai/gpt-5.5') {
            Run-OpenClaw 'Gateway restart for GPT-5.5' @('gateway', 'restart') | Out-Null
            Start-Sleep -Seconds 8
            $routeOk = Test-SubscriptionModel 'openai/gpt-5.5'
        }
    }
    Add-Line ("subscription_route_ready={0}" -f $routeOk)

    Add-Line ''
    Add-Line '## Safe configuration summary'
    if (Test-Path $Config) {
        try {
            $doc = Get-Content -Raw -Encoding UTF8 $Config | ConvertFrom-Json
            $feishu = $doc.channels.feishu
            Add-Line ("feishu_enabled={0}" -f $feishu.enabled)
            if ($feishu.accounts) {
                foreach ($p in $feishu.accounts.PSObject.Properties) {
                    $v = $p.Value
                    Add-Line ("feishu_account={0}; enabled={1}; has_app_id={2}; has_secret={3}" -f `
                        $p.Name, $v.enabled, [bool]$v.appId, [bool]$v.appSecret)
                }
            } else {
                Add-Line 'feishu_accounts=none'
            }
            foreach ($agent in @($doc.agents.list)) {
                Add-Line ("agent={0}; model={1}" -f $agent.id, ($agent.model | ConvertTo-Json -Compress))
            }
        } catch {
            Add-Line ("config_summary_error={0}" -f $_.Exception.Message)
        }
    }

    $task = Get-ScheduledTask -TaskName 'OpenClaw Gateway' -ErrorAction SilentlyContinue
    $taskInfo = Get-ScheduledTaskInfo -TaskName 'OpenClaw Gateway' -ErrorAction SilentlyContinue
    Add-Line ''
    Add-Line '## Scheduled task before recovery'
    if ($task) {
        Add-Line ("state={0}; last_result={1}; last_run={2}; next_run={3}" -f `
            $task.State, $taskInfo.LastTaskResult, $taskInfo.LastRunTime, $taskInfo.NextRunTime)
        if ($task.State -ne 'Running') {
            Start-ScheduledTask -TaskName 'OpenClaw Gateway' -ErrorAction SilentlyContinue
            Start-Sleep -Seconds 5
            $task = Get-ScheduledTask -TaskName 'OpenClaw Gateway' -ErrorAction SilentlyContinue
            Add-Line ("state_after_start={0}" -f $task.State)
        }
    } else {
        Add-Line 'task=missing'
    }

    Run-OpenClaw 'Version' @('--version') | Out-Null
    Run-OpenClaw 'Config validation' @('config', 'validate') | Out-Null
    if (-not $routeOk) {
        Add-Line 'FAIL_CLOSED: no subscription model route is usable; no API-key fallback was attempted.'
    }
    Run-OpenClaw 'Deep status after restart' @('status', '--deep') | Out-Null
    Run-OpenClaw 'Feishu channel probe' @('channels', 'status', '--probe') | Out-Null
    Run-OpenClaw 'Agent bindings' @('agents', 'list', '--json') | Out-Null
    Run-OpenClaw 'Available models' @('models', 'list') | Out-Null
}

$utf8Bom = New-Object System.Text.UTF8Encoding($true)
[System.IO.File]::WriteAllLines($Report, $Lines, $utf8Bom)
Write-Output ("[OpenClaw remote recovery] report written: {0}" -f $Report)

& git -C $Repo add -- 'win/CODEX_WIN_OPENCLAW_AUTO_REPORT_20260827.md' 2>&1 | Out-Null
& git -C $Repo commit --only -m 'win: report GPT OpenClaw remote recovery' -- `
    'win/CODEX_WIN_OPENCLAW_AUTO_REPORT_20260827.md' 2>&1 | Out-Null
& git -C $Repo push origin HEAD:main 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Set-Content -Path $Done -Value (Get-Date -Format o) -Encoding ASCII
    Write-Output '[OpenClaw remote recovery] report pushed; one-shot completed.'
    exit 0
}

Write-Output '[OpenClaw remote recovery] report could not be pushed; will retry next mirror round.'
exit 0
