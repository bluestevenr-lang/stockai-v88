# One-shot remote diagnostics and safe restart for the GPT OpenClaw incident.
# This script never prints credentials, changes models, or changes V88 logic.
$ErrorActionPreference = 'Continue'

$Repo = Split-Path -Parent $PSScriptRoot
$State = Join-Path $env:USERPROFILE '.openclaw'
$Config = Join-Path $State 'openclaw.json'
$Done = Join-Path $State 'gpt_openclaw_recovery_20260824.done'
$Report = Join-Path $PSScriptRoot 'CODEX_WIN_OPENCLAW_AUTO_REPORT_20260824.md'

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

Add-Line '# Win GPT OpenClaw automatic recovery report'
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
    Run-OpenClaw 'Gateway restart' @('gateway', 'restart') | Out-Null
    Start-Sleep -Seconds 8
    Run-OpenClaw 'Deep status after restart' @('status', '--deep') | Out-Null
    Run-OpenClaw 'Feishu channel probe' @('channels', 'status', '--probe') | Out-Null
    Run-OpenClaw 'Agent bindings' @('agents', 'list', '--json') | Out-Null
    Run-OpenClaw 'Available models' @('models', 'list') | Out-Null
}

$utf8Bom = New-Object System.Text.UTF8Encoding($true)
[System.IO.File]::WriteAllLines($Report, $Lines, $utf8Bom)
Write-Output ("[OpenClaw remote recovery] report written: {0}" -f $Report)

& git -C $Repo add -- 'win/CODEX_WIN_OPENCLAW_AUTO_REPORT_20260824.md' 2>&1 | Out-Null
& git -C $Repo commit --only -m 'win: report GPT OpenClaw remote recovery' -- `
    'win/CODEX_WIN_OPENCLAW_AUTO_REPORT_20260824.md' 2>&1 | Out-Null
& git -C $Repo push origin HEAD:main 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Set-Content -Path $Done -Value (Get-Date -Format o) -Encoding ASCII
    Write-Output '[OpenClaw remote recovery] report pushed; one-shot completed.'
    exit 0
}

Write-Output '[OpenClaw remote recovery] report could not be pushed; will retry next mirror round.'
exit 0
