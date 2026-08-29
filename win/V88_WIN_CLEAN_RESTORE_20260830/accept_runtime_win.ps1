#requires -Version 5.1
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('Exit','Reboot')]
    [string]$Mode
)

$ErrorActionPreference = 'Stop'
$PackageRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PackageRoot)
$StatePath = Join-Path $env:LOCALAPPDATA 'V88CleanRestore\state.json'
$ManifestPath = Join-Path $PackageRoot 'MANIFEST.sha256'
$Agent = 'v88-gpt'
$Account = 'v88-gpt'
$Model = 'openai/gpt-5.6-sol'
$OpenClawVersion = '2026.7.1-2'
$GatewayTaskName = 'OpenClaw Gateway'

function Fail([string]$Message) { Write-Error $Message; exit 1 }

function Get-OwnerFingerprint {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $profile = [IO.Path]::GetFullPath($env:USERPROFILE).TrimEnd('\').ToLowerInvariant()
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [Text.Encoding]::UTF8.GetBytes($identity.User.Value + '|' + $profile)
        return ([BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant()
    } finally { $sha.Dispose() }
}

function Resolve-PrincipalSid([string]$UserId) {
    if ([string]::IsNullOrWhiteSpace($UserId)) { return $null }
    try { return ([Security.Principal.SecurityIdentifier]::new($UserId)).Value }
    catch {
        try { return ([Security.Principal.NTAccount]::new($UserId)).Translate([Security.Principal.SecurityIdentifier]).Value }
        catch { return $null }
    }
}

function Assert-PackageIntegrity {
    if (-not (Test-Path -LiteralPath $ManifestPath -PathType Leaf)) { Fail 'MANIFEST.sha256 is missing.' }
    $repoPrefix = [IO.Path]::GetFullPath($RepoRoot).TrimEnd('\') + '\'
    foreach ($line in Get-Content -LiteralPath $ManifestPath -Encoding UTF8) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        if ($line -notmatch '^([0-9a-fA-F]{64})  (.+)$') { Fail 'MANIFEST.sha256 has an invalid line.' }
        $target = [IO.Path]::GetFullPath((Join-Path $PackageRoot $Matches[2].Replace('/', '\')))
        if (-not $target.StartsWith($repoPrefix, [StringComparison]::OrdinalIgnoreCase) -or
            -not (Test-Path -LiteralPath $target -PathType Leaf) -or
            (Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash -ne $Matches[1].ToUpperInvariant()) {
            Fail ('Package integrity failed: ' + $Matches[2])
        }
    }
}

function Assert-GatewayTask([string]$CurrentSid) {
    Import-Module ScheduledTasks -ErrorAction Stop
    $tasks = @(Get-ScheduledTask -TaskName $GatewayTaskName -TaskPath '\' -ErrorAction SilentlyContinue)
    if ($tasks.Count -ne 1) { Fail 'Expected exactly one OpenClaw Gateway Scheduled Task.' }
    if ((Resolve-PrincipalSid ([string]$tasks[0].Principal.UserId)) -ne $CurrentSid) { Fail 'Gateway task belongs to a different Windows user.' }
    [xml]$xml = Export-ScheduledTask -TaskName $GatewayTaskName -TaskPath '\'
    if (-not $xml.Task.Triggers.LogonTrigger -or $xml.Task.Principals.Principal.LogonType -ne 'InteractiveToken' -or
        $xml.Task.Settings.MultipleInstancesPolicy -ne 'IgnoreNew' -or
        $xml.Task.Settings.RestartOnFailure.Count -ne '3' -or $xml.Task.Settings.RestartOnFailure.Interval -ne 'PT1M') {
        Fail 'Gateway task trigger, identity or restart policy changed.'
    }
    $startup = [Environment]::GetFolderPath('Startup')
    if ($startup -and (Test-Path -LiteralPath $startup -PathType Container)) {
        $fallback = @(Get-ChildItem -LiteralPath $startup -File -ErrorAction SilentlyContinue |
            Where-Object { $_.BaseName -eq $GatewayTaskName -and $_.Extension -in @('.cmd','.vbs') })
        if ($fallback.Count -gt 0) { Fail 'A duplicate Startup-folder Gateway fallback exists.' }
    }
}

function Test-GptSubscription($OpenClaw) {
    $status = (& $OpenClaw models status --agent $Agent --json --probe --probe-provider openai --probe-max-tokens 8 --probe-timeout 30000) | ConvertFrom-Json
    $provider = @($status.auth.providers | Where-Object { $_.provider -eq 'openai' })
    $probe = @($status.auth.probes.results | Where-Object { $_.provider -eq 'openai' -and $_.mode -eq 'oauth' -and $_.status -eq 'ok' })
    if ($status.resolvedDefault -ne $Model -or @($status.fallbacks).Count -ne 0 -or $provider.Count -ne 1 -or
        $provider[0].profiles.oauth -ne 1 -or $provider[0].profiles.apiKey -ne 0 -or $provider[0].profiles.token -ne 0 -or $probe.Count -lt 1) {
        Fail 'GPT subscription route is not clean OAuth/no-fallback.'
    }
    $sentinel = if ($Mode -eq 'Exit') { 'WIN_V88_POST_EXIT_OK' } else { 'WIN_V88_POST_REBOOT_OK' }
    $raw = & $OpenClaw agent --agent $Agent --session-id ("win-accept-{0}" -f $Mode.ToLowerInvariant()) `
        --message ("Reply only: {0}" -f $sentinel) --thinking low --timeout 180 --json
    if ($LASTEXITCODE -ne 0) { Fail 'GPT subscription sentinel failed.' }
    $reply = $raw | ConvertFrom-Json
    if ($reply.result.payloads[0].text -notmatch [regex]::Escape($sentinel) -or
        $reply.result.meta.agentMeta.provider -ne 'openai' -or $reply.result.meta.executionTrace.fallbackUsed) {
        Fail 'GPT sentinel used the wrong provider or fallback.'
    }
}

Assert-PackageIntegrity
if (-not (Test-Path -LiteralPath $StatePath -PathType Leaf)) { Fail 'Recovery state is missing.' }
$state = Get-Content -LiteralPath $StatePath -Raw -Encoding UTF8 | ConvertFrom-Json
if (-not $state.service_accepted -or $state.owner_fingerprint -ne (Get-OwnerFingerprint) -or
    [string]$state.owner_sid -ne [Security.Principal.WindowsIdentity]::GetCurrent().User.Value) {
    Fail 'Service acceptance or recovery owner is not valid.'
}

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
if ($Mode -eq 'Exit') {
    if (@(Get-Process -ErrorAction SilentlyContinue | Where-Object { $_.ProcessName -match '(?i)^(Codex|ChatGPT)' }).Count -gt 0) {
        Fail 'Codex/ChatGPT Desktop is still running; close it before PostExit acceptance.'
    }
    $freshAfter = [DateTimeOffset]::UtcNow
} else {
    if (-not $state.post_exit -or -not $state.post_exit_boot_utc) { Fail 'PostExit acceptance or boot marker is missing.' }
    $currentBootTime = (Get-CimInstance Win32_OperatingSystem).LastBootUpTime
    $currentBoot = [DateTimeOffset]$currentBootTime
    $oldBoot = [DateTimeOffset]::Parse([string]$state.post_exit_boot_utc)
    if ($currentBoot -le $oldBoot) { Fail 'Windows has not rebooted since the service acceptance marker.' }
    $freshAfter = $currentBoot
}

$blockedEnvironment = @('OPENAI_API_KEY','ANTHROPIC_API_KEY','GOOGLE_API_KEY','GEMINI_API_KEY','MOONSHOT_API_KEY',
    'KIMI_API_KEY','KIMI_CODE_API_KEY','DEEPSEEK_API_KEY','OPENROUTER_API_KEY','CODEX_API_KEY',
    'OPENAI_BASE_URL','ANTHROPIC_BASE_URL','MOONSHOT_BASE_URL','KIMI_BASE_URL','DEEPSEEK_BASE_URL',
    'OPENROUTER_BASE_URL','OPENAI_EXTRA_USAGE','CODEX_EXTRA_USAGE','OPENCLAW_HOME','OPENCLAW_STATE_DIR',
    'OPENCLAW_CONFIG_PATH','OPENCLAW_WINDOWS_TASK_NAME')
if (@($blockedEnvironment | Where-Object {
    $name = $_
    @('Process','User','Machine') | Where-Object { [Environment]::GetEnvironmentVariable($name, $_) } | Select-Object -First 1
}).Count -gt 0) { Fail 'A paid route or OpenClaw state override is present.' }

$openclawCommand = Get-Command openclaw.cmd, openclaw.exe, openclaw -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $openclawCommand -or [IO.Path]::GetFullPath($openclawCommand.Source) -ne [IO.Path]::GetFullPath([string]$state.openclaw_path)) {
    Fail 'OpenClaw executable path changed after Prepare.'
}
$OpenClaw = $openclawCommand.Source
$nodeCommand = Get-Command node.exe, node -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $nodeCommand -or [IO.Path]::GetFullPath($nodeCommand.Source) -ne [IO.Path]::GetFullPath([string]$state.node_path) -or
    ((& $nodeCommand.Source --version 2>$null | Out-String).Trim().TrimStart('v')) -ne [string]$state.node_version) {
    Fail 'Node executable path or version changed after Prepare.'
}
Assert-GatewayTask $identity.User.Value

$gateway = (& $OpenClaw gateway status --json) | ConvertFrom-Json
$pids = @($gateway.port.listeners | ForEach-Object { $_.pid } | Sort-Object -Unique)
$serviceEnvironment = @($gateway.service.command.environment.PSObject.Properties.Name)
if (-not $gateway.rpc.ok -or -not $gateway.service.loaded -or $gateway.gateway.bindMode -ne 'loopback' -or
    $gateway.gateway.bindHost -ne '127.0.0.1' -or $gateway.gateway.version -ne $OpenClawVersion -or
    $pids.Count -ne 1 -or @($gateway.extraServices).Count -ne 0 -or
    @($serviceEnvironment | Where-Object { $_ -match '(API_KEY|BASE_URL|ENDPOINT|EXTRA_USAGE)' }).Count -gt 0) {
    Fail 'Gateway runtime is not one clean loopback-only no-paid-env service.'
}

& (Join-Path $PackageRoot 'test_feishu_roundtrip.ps1') -OpenClawPath $OpenClaw -After $freshAfter `
    -Agent $Agent -Account $Account -Model 'gpt-5.6-sol' -RequireHumanConfirmation -TimeoutSeconds 300
if ($LASTEXITCODE -ne 0) { Fail 'Fresh formal Feishu nonce roundtrip failed.' }

Test-GptSubscription $OpenClaw
foreach ($taskName in @('V88 OpenClaw Health','V88 OpenClaw Projection')) {
    $task = Get-ScheduledTask -TaskName $taskName -TaskPath '\' -ErrorAction SilentlyContinue
    if (-not $task -or (Resolve-PrincipalSid ([string]$task.Principal.UserId)) -ne $identity.User.Value) {
        Fail ('Required current-user task is missing or owned by another user: ' + $taskName)
    }
    $freshTask = $false
    for ($i = 0; $i -lt 72; $i++) {
        $info = Get-ScheduledTaskInfo -TaskName $taskName -TaskPath '\'
        $taskState = (Get-ScheduledTask -TaskName $taskName -TaskPath '\').State
        if ($info.LastRunTime -and [DateTimeOffset]$info.LastRunTime -gt $freshAfter -and
            $info.LastTaskResult -eq 0 -and $taskState -ne 'Running') {
            $freshTask = $true
            break
        }
        Start-Sleep -Seconds 5
    }
    if (-not $freshTask) { Fail ('Required task did not complete cleanly after this acceptance began: ' + $taskName) }
}

Write-Host ("{0}_ACCEPTED: permanent Gateway, phone roundtrip, GPT OAuth/no-fallback and scheduled tasks passed." -f $Mode.ToUpperInvariant())
exit 0
