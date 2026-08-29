#requires -Version 5.1
[CmdletBinding()]
param(
    [switch]$MacReceiverDisabled,
    [switch]$PhoneAnswerConfirmed,
    [switch]$RemoteMacReadConfirmed,
    [string]$V88DataPath = (Join-Path $env:USERPROFILE 'Desktop\ai-daily-report-v2\data')
)

$ErrorActionPreference = 'Stop'
$PackageRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$WinRoot = Split-Path -Parent $PackageRoot
$RepoRoot = Split-Path -Parent $WinRoot
$Agent = 'v88-gpt'
$Account = 'v88-gpt'
$Model = 'openai/gpt-5.6-sol'
$OpenClawVersion = '2026.7.1-2'
$Workspace = Join-Path $env:USERPROFILE '.openclaw\workspaces\v88-gpt'
$StateDir = Join-Path $env:LOCALAPPDATA 'V88CleanRestore'
$StatePath = Join-Path $StateDir 'state.json'
$ManifestPath = Join-Path $PackageRoot 'MANIFEST.sha256'
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

function Update-VerificationState([hashtable]$Patch) {
    $state = @{}
    $old = Get-Content -LiteralPath $StatePath -Raw -Encoding UTF8 | ConvertFrom-Json
    foreach ($property in $old.PSObject.Properties) { $state[$property.Name] = $property.Value }
    foreach ($key in $Patch.Keys) { $state[$key] = $Patch[$key] }
    $state['updated_at'] = [DateTimeOffset]::UtcNow.ToString('o')
    [IO.File]::WriteAllText($StatePath, ($state | ConvertTo-Json -Depth 10), (New-Object Text.UTF8Encoding($false)))
}

function Resolve-Python {
    foreach ($name in @('py.exe','python.exe','python3.exe')) {
        $command = Get-Command $name -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $command -or $command.Source -match '\\WindowsApps\\') { continue }
        $prefix = @(); if ($name -eq 'py.exe') { $prefix = @('-3') }
        & $command.Source @prefix -c 'import sys; raise SystemExit(0 if sys.version_info >= (3,10) else 2)'
        if ($LASTEXITCODE -eq 0) { return @{ File = $command.Source; Prefix = $prefix } }
    }
    Fail 'Python 3.10+ is required.'
}

function Invoke-Projection {
    $source = [IO.Path]::GetFullPath($V88DataPath)
    if ($source.Contains('"')) { Fail 'V88 data path contains an unsupported quote character.' }
    if (-not (Test-Path -LiteralPath $source -PathType Container)) { Fail 'V88 data directory is missing.' }
    $python = Resolve-Python; $pythonArgs = @($python.Prefix); $pythonExe = [string]$python.File
    $tests = Join-Path $WinRoot 'openclaw-v88\projection_tests.py'
    $script = Join-Path $WinRoot 'openclaw-v88\sync_v88_projection_win.py'
    & $pythonExe @pythonArgs $tests
    if ($LASTEXITCODE -ne 0) { Fail 'Projection tests failed.' }
    $dest = Join-Path $Workspace 'context'
    & $pythonExe @pythonArgs $script --source $source --dest $dest
    if ($LASTEXITCODE -ne 0) { Fail 'Actual privacy-minimized projection failed.' }
    if (-not (Test-Path -LiteralPath (Join-Path $dest 'overview.json') -PathType Leaf) -or
        @(Get-ChildItem -LiteralPath (Join-Path $dest 'stocks') -Filter '*.json' -File).Count -lt 1) { Fail 'Actual projection is incomplete.' }
    $blockedPattern = '"(account|account_id|assets|balance|cash|cost|cost_basis|holdings|market_value|positions|qty|quantity|shares|total_assets)"\s*:'
    if (Get-ChildItem -LiteralPath $dest -Filter '*.json' -File -Recurse | Select-String -Pattern $blockedPattern -Quiet) {
        Fail 'Privacy scan found a blocked field.'
    }
}

function Stop-TemporaryGateway {
    if (-not (Test-Path -LiteralPath $StatePath -PathType Leaf)) { Fail 'Recovery state is missing.' }
    $state = Get-Content -LiteralPath $StatePath -Raw -Encoding UTF8 | ConvertFrom-Json
    if (-not $state.temp_gateway_pid -or -not $state.temp_gateway_start_utc) { Fail 'Temporary Gateway identity is incomplete; refusing to stop an unknown process.' }
    $before = (& $OpenClaw gateway status --json 2>$null) | ConvertFrom-Json
    $listeners = @($before.port.listeners | ForEach-Object { $_.pid } | Sort-Object -Unique)
    if (-not $before.rpc.ok -or $before.gateway.bindMode -ne 'loopback' -or $listeners.Count -ne 1 -or
        [int]$listeners[0] -ne [int]$state.temp_gateway_pid) {
        Fail 'The current listener no longer matches the recorded temporary Gateway; nothing was stopped.'
    }
    $process = Get-Process -Id ([int]$state.temp_gateway_pid) -ErrorAction SilentlyContinue
    if (-not $process -or $process.StartTime.ToUniversalTime().ToString('o') -ne [string]$state.temp_gateway_start_utc) {
        Fail 'The recorded temporary Gateway PID was reused or changed; nothing was stopped.'
    }
    $cim = Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $process.Id) -ErrorAction SilentlyContinue
    $owner = if ($cim) { Invoke-CimMethod -InputObject $cim -MethodName GetOwnerSid -ErrorAction SilentlyContinue } else { $null }
    if (-not $cim -or $cim.CommandLine -notmatch '(?i)openclaw' -or $cim.CommandLine -notmatch '(?i)gateway' -or
        $cim.CommandLine -notmatch '(?i)(--port\s+18789|--port=18789)' -or -not $owner -or $owner.ReturnValue -ne 0 -or
        $owner.Sid -ne [Security.Principal.WindowsIdentity]::GetCurrent().User.Value -or
        $state.temp_gateway_owner_fingerprint -ne (Get-OwnerFingerprint)) {
        Fail 'The recorded process command is not the expected temporary OpenClaw Gateway; nothing was stopped.'
    }
    Stop-Process -Id $process.Id -Force
    Start-Sleep -Seconds 3
    $after = & $OpenClaw gateway status --json 2>$null
    if ($LASTEXITCODE -eq 0 -and ($after | ConvertFrom-Json).rpc.ok) { Fail 'An unmanaged Gateway remained after stopping the recorded temporary process.' }
}

function Set-And-Test-GatewayTask {
    Import-Module ScheduledTasks -ErrorAction Stop
    $startup = [Environment]::GetFolderPath('Startup')
    $fallbacks = @()
    if ($startup -and (Test-Path -LiteralPath $startup -PathType Container)) {
        $fallbacks = @(Get-ChildItem -LiteralPath $startup -File -ErrorAction SilentlyContinue |
            Where-Object { $_.BaseName -eq $GatewayTaskName -and $_.Extension -in @('.cmd','.vbs') })
    }
    if ($fallbacks.Count -gt 0) { Fail 'OpenClaw used the Startup-folder fallback; this host requires the formal Scheduled Task.' }
    $tasks = @(Get-ScheduledTask -TaskName $GatewayTaskName -TaskPath '\' -ErrorAction SilentlyContinue)
    if ($tasks.Count -ne 1) { Fail 'Expected exactly one formal OpenClaw Gateway Scheduled Task.' }
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    if ((Resolve-PrincipalSid ([string]$tasks[0].Principal.UserId)) -ne $identity.User.Value) {
        Fail 'OpenClaw Gateway task belongs to a different Windows user.'
    }
    [xml]$taskXml = Export-ScheduledTask -TaskName $GatewayTaskName -TaskPath '\'
    if (-not $taskXml.Task.Triggers.LogonTrigger -or $taskXml.Task.Principals.Principal.LogonType -ne 'InteractiveToken' -or
        $taskXml.Task.Settings.MultipleInstancesPolicy -ne 'IgnoreNew') {
        Fail 'OpenClaw Gateway task is not the expected current-user LogonTrigger/InteractiveToken task.'
    }
    $actionPath = [IO.Path]::GetFullPath([string]$taskXml.Task.Actions.Exec.Command)
    if (-not (Test-Path -LiteralPath $actionPath -PathType Leaf) -or [IO.Path]::GetExtension($actionPath) -ne '.cmd') {
        Fail 'OpenClaw Gateway task action is not a real gateway.cmd file.'
    }
    $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
        -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
        -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
    Set-ScheduledTask -TaskName $GatewayTaskName -TaskPath '\' -Settings $settings | Out-Null
    [xml]$updated = Export-ScheduledTask -TaskName $GatewayTaskName -TaskPath '\'
    if ($updated.Task.Settings.RestartOnFailure.Count -ne '3' -or $updated.Task.Settings.RestartOnFailure.Interval -ne 'PT1M') {
        Fail 'Gateway crash restart policy was not persisted.'
    }
}

function Wait-FreshPermanentPhoneRoundtrip([DateTimeOffset]$After) {
    & (Join-Path $PackageRoot 'test_feishu_roundtrip.ps1') -OpenClawPath $OpenClaw -After $After `
        -Agent $Agent -Account $Account -Model 'gpt-5.6-sol' -RequireHumanConfirmation -TimeoutSeconds 300
    if ($LASTEXITCODE -ne 0) { Fail 'Permanent formal Feishu nonce roundtrip failed.' }
}

function Test-PermanentGptSentinel {
    $modelStatus = (& $OpenClaw models status --agent $Agent --json --probe --probe-provider openai --probe-max-tokens 8 --probe-timeout 30000) | ConvertFrom-Json
    $provider = @($modelStatus.auth.providers | Where-Object { $_.provider -eq 'openai' })
    $probe = @($modelStatus.auth.probes.results | Where-Object { $_.provider -eq 'openai' -and $_.mode -eq 'oauth' -and $_.status -eq 'ok' })
    if ($modelStatus.resolvedDefault -ne $Model -or @($modelStatus.fallbacks).Count -ne 0 -or $provider.Count -ne 1 -or
        $provider[0].profiles.oauth -ne 1 -or $provider[0].profiles.apiKey -ne 0 -or $provider[0].profiles.token -ne 0 -or $probe.Count -lt 1) {
        Fail 'The permanent Gateway GPT route is not clean OAuth/no-fallback.'
    }
    $raw = & $OpenClaw agent --agent $Agent --session-id 'win-permanent-verify-gpt' --message 'Reply only: WIN_V88_PERMANENT_GPT_OK' --thinking low --timeout 180 --json
    if ($LASTEXITCODE -ne 0) { Fail 'Permanent Gateway GPT sentinel failed.' }
    $gpt = $raw | ConvertFrom-Json
    if ($gpt.result.payloads[0].text -notmatch 'WIN_V88_PERMANENT_GPT_OK' -or
        $gpt.result.meta.agentMeta.provider -ne 'openai' -or $gpt.result.meta.executionTrace.fallbackUsed) {
        Fail 'Permanent Gateway GPT sentinel used the wrong provider or a fallback.'
    }
}

function Install-ProjectionTask {
    Import-Module ScheduledTasks -ErrorAction Stop
    $taskName = 'V88 OpenClaw Projection'
    $marker = 'V88 privacy projection v1; current-user logon; no network credentials.'
    $existing = Get-ScheduledTask -TaskName $taskName -TaskPath '\' -ErrorAction SilentlyContinue
    if ($existing -and $existing.Description -ne $marker) { Fail 'A different task already uses the V88 projection task name.' }
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $runner = Join-Path $PackageRoot 'projection_runner.ps1'
    $source = [IO.Path]::GetFullPath($V88DataPath)
    $dest = Join-Path $Workspace 'context'
    $args = '-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "{0}" -Source "{1}" -Dest "{2}"' -f $runner,$source,$dest
    $action = New-ScheduledTaskAction -Execute (Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe') -Argument $args -WorkingDirectory $PackageRoot
    $periodic = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 5)
    $logon = New-ScheduledTaskTrigger -AtLogOn -User $identity.User.Value
    $principal = New-ScheduledTaskPrincipal -UserId $identity.User.Value -LogonType Interactive -RunLevel Limited
    $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 4) -StartWhenAvailable
    Register-ScheduledTask -TaskName $taskName -TaskPath '\' -Action $action -Trigger @($periodic,$logon) -Principal $principal -Settings $settings -Description $marker -Force | Out-Null
    Start-ScheduledTask -TaskName $taskName -TaskPath '\'
    for ($i = 0; $i -lt 12; $i++) {
        Start-Sleep -Seconds 5
        if ((Get-ScheduledTask -TaskName $taskName -TaskPath '\').State -ne 'Running') { break }
    }
    if ((Get-ScheduledTask -TaskName $taskName -TaskPath '\').State -eq 'Running') { Fail 'Projection task did not finish within one minute.' }
    if ((Get-ScheduledTaskInfo -TaskName $taskName -TaskPath '\').LastTaskResult -ne 0) { Fail 'Projection task did not complete successfully.' }
}

Assert-PackageIntegrity
if (-not (Test-Path -LiteralPath $StatePath -PathType Leaf)) { Fail 'Recovery state is missing.' }
$restoreState = Get-Content -LiteralPath $StatePath -Raw -Encoding UTF8 | ConvertFrom-Json
if (-not $restoreState.owner_fingerprint -or $restoreState.owner_fingerprint -ne (Get-OwnerFingerprint) -or
    [string]$restoreState.owner_sid -ne [Security.Principal.WindowsIdentity]::GetCurrent().User.Value) {
    Fail 'Verify is running under a different Windows user/profile than Prepare.'
}
& (Join-Path $PackageRoot 'preflight_new_ssd_win.ps1') -Phase Runtime
if ($LASTEXITCODE -ne 0) { Fail 'Windows runtime preflight did not pass cleanly.' }
$openclawCommand = Get-Command openclaw.cmd, openclaw.exe, openclaw -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $openclawCommand) { Fail 'OpenClaw is not installed.' }
$OpenClaw = $openclawCommand.Source
if ([IO.Path]::GetFullPath($OpenClaw) -ne [IO.Path]::GetFullPath([string]$restoreState.openclaw_path)) {
    Fail 'OpenClaw executable path changed after Prepare.'
}
$nodeCommand = Get-Command node.exe, node -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $nodeCommand -or [IO.Path]::GetFullPath($nodeCommand.Source) -ne [IO.Path]::GetFullPath([string]$restoreState.node_path)) {
    Fail 'Node executable path changed after Prepare.'
}
$actualNodeVersion = ((& $nodeCommand.Source --version 2>$null | Out-String).Trim().TrimStart('v'))
if (-not $restoreState.node_version -or $actualNodeVersion -ne [string]$restoreState.node_version) {
    Fail 'Node version changed after Prepare.'
}

$blocked = @('OPENAI_API_KEY','ANTHROPIC_API_KEY','GOOGLE_API_KEY','GEMINI_API_KEY','MOONSHOT_API_KEY',
    'KIMI_API_KEY','KIMI_CODE_API_KEY','DEEPSEEK_API_KEY','OPENROUTER_API_KEY','CODEX_API_KEY',
    'OPENAI_BASE_URL','ANTHROPIC_BASE_URL','MOONSHOT_BASE_URL','KIMI_BASE_URL','DEEPSEEK_BASE_URL',
    'OPENROUTER_BASE_URL','OPENAI_EXTRA_USAGE','CODEX_EXTRA_USAGE')
if (@($blocked | Where-Object {
    $name = $_
    @('Process','User','Machine') | Where-Object { [Environment]::GetEnvironmentVariable($name, $_) } | Select-Object -First 1
}).Count -gt 0) { Fail 'A paid API environment variable is present.' }
if (-not $MacReceiverDisabled) { Fail 'Mac formal V88-GPT receiver has not been confirmed disabled.' }
if (-not $PhoneAnswerConfirmed) { Fail 'A real phone natural-language answer has not been confirmed.' }
if (-not $RemoteMacReadConfirmed) { Fail 'Mac has not actually read the newly paired Windows Remote task.' }

$auth = (& $OpenClaw models auth list --agent $Agent --json) | ConvertFrom-Json
$badProfiles = @($auth.profiles | Where-Object { $_.provider -ne 'openai' -or $_.type -ne 'oauth' })
$goodProfiles = @($auth.profiles | Where-Object { $_.provider -eq 'openai' -and $_.type -eq 'oauth' })
if ($badProfiles.Count -gt 0 -or $goodProfiles.Count -ne 1) { Fail 'Expected exactly one OpenAI OAuth profile and no API-key profiles.' }

$agents = @((& $OpenClaw agents list --json) | ConvertFrom-Json)
$target = @($agents | Where-Object { $_.id -eq $Agent })
if ($target.Count -ne 1 -or $target[0].bindings -ne 1) { Fail 'Expected one v88-gpt agent with exactly one binding.' }
$bindings = @((& $OpenClaw agents bindings --json) | ConvertFrom-Json)
$feishuBindings = @($bindings | Where-Object { $_.match.channel -eq 'feishu' })
if ($feishuBindings.Count -ne 1 -or $feishuBindings[0].agentId -ne $Agent -or $feishuBindings[0].match.accountId -ne $Account) {
    Fail 'Expected exactly one formal Feishu route to v88-gpt.'
}

$configPath = (& $OpenClaw config file).Trim()
$config = Get-Content -LiteralPath $configPath -Raw -Encoding UTF8 | ConvertFrom-Json
$entry = @($config.agents.list | Where-Object { $_.id -eq $Agent })
if ($entry.Count -ne 1) { Fail 'v88-gpt configuration is missing or duplicated.' }
if ($entry[0].model.primary -ne $Model -or @($entry[0].model.fallbacks).Count -ne 0) { Fail 'Model or fallback policy is wrong.' }
$allowed = @($entry[0].tools.allow)
if ($entry[0].tools.exec.mode -ne 'deny' -or -not $entry[0].tools.fs.workspaceOnly -or $allowed.Count -ne 1 -or $allowed[0] -ne 'read') { Fail 'Read-only tool policy is not proven.' }
if ($config.channels.feishu.defaultAccount -ne $Account) { Fail 'Formal Feishu account is not the default.' }
$accountNames = @($config.channels.feishu.accounts.PSObject.Properties.Name)
if ($accountNames.Count -ne 1 -or $accountNames[0] -ne $Account -or $config.channels.feishu.appId -or $config.channels.feishu.appSecret) {
    Fail 'Legacy or duplicate Feishu accounts remain.'
}

$channel = (& $OpenClaw channels status --json) | ConvertFrom-Json
$formal = @($channel.channelAccounts.feishu | Where-Object { $_.accountId -eq $Account })
if ($formal.Count -ne 1 -or -not $formal[0].enabled -or -not $formal[0].running -or -not $formal[0].connected) { Fail 'Formal Feishu account is not connected.' }
if (-not $formal[0].lastInboundAt -or -not $formal[0].lastOutboundAt) { Fail 'No real Feishu inbound/outbound evidence was observed.' }

$modelStatus = (& $OpenClaw models status --agent $Agent --json --probe --probe-provider openai --probe-max-tokens 8 --probe-timeout 30000) | ConvertFrom-Json
$provider = @($modelStatus.auth.providers | Where-Object { $_.provider -eq 'openai' })
$probe = @($modelStatus.auth.probes.results | Where-Object { $_.provider -eq 'openai' -and $_.mode -eq 'oauth' -and $_.status -eq 'ok' })
if ($modelStatus.resolvedDefault -ne $Model -or @($modelStatus.fallbacks).Count -ne 0 -or $provider.Count -ne 1 -or
    $provider[0].profiles.oauth -ne 1 -or $provider[0].profiles.apiKey -ne 0 -or $provider[0].profiles.token -ne 0 -or $probe.Count -lt 1) {
    Fail 'The live GPT subscription route is not clean OAuth/no-fallback.'
}

$gptRaw = & $OpenClaw agent --agent $Agent --session-id 'win-clean-verify-gpt' --message 'Reply only: WIN_V88_GPT_VERIFY_OK' --thinking low --timeout 180 --json
$gpt = $gptRaw | ConvertFrom-Json
if ($gpt.result.payloads[0].text -notmatch 'WIN_V88_GPT_VERIFY_OK' -or $gpt.result.meta.agentMeta.provider -ne 'openai' -or $gpt.result.meta.executionTrace.fallbackUsed) { Fail 'GPT no-fallback sentinel failed.' }

Invoke-Projection

$serviceStartedAt = [DateTimeOffset]::UtcNow
Import-Module ScheduledTasks -ErrorAction Stop
$existingGatewayTasks = @(Get-ScheduledTask -TaskName $GatewayTaskName -TaskPath '\' -ErrorAction SilentlyContinue)
if ($existingGatewayTasks.Count -gt 1) { Fail 'Multiple OpenClaw Gateway tasks exist.' }
if ($existingGatewayTasks.Count -eq 0) {
    Update-VerificationState @{ gateway_conversion_started = $true; gateway_autostart = $false }
    $currentStatus = $null
    try { $currentStatus = (& $OpenClaw gateway status --json 2>$null) | ConvertFrom-Json } catch { }
    if ($currentStatus -and $currentStatus.rpc.ok) { Stop-TemporaryGateway }
    & $OpenClaw gateway install --force --port 18789
    if ($LASTEXITCODE -ne 0) { Fail 'Gateway service installation failed.' }
} elseif (-not $restoreState.gateway_conversion_started) {
    Fail 'An untracked permanent Gateway task exists; clean verification will not take it over.'
}
Set-And-Test-GatewayTask
Update-VerificationState @{ gateway_conversion_started = $true; gateway_autostart = $true }
Start-ScheduledTask -TaskName $GatewayTaskName -TaskPath '\'
$gateway = $null
for ($i = 0; $i -lt 12; $i++) {
    Start-Sleep -Seconds 5
    try {
        $candidate = (& $OpenClaw gateway status --json) | ConvertFrom-Json
        if ($candidate.rpc.ok) { $gateway = $candidate; break }
    } catch { }
}
if (-not $gateway) { Fail 'Installed Gateway Scheduled Task did not become RPC-ready within one minute.' }
$listenerPids = @($gateway.port.listeners | ForEach-Object { $_.pid } | Sort-Object -Unique)
$serviceEnvNames = @($gateway.service.command.environment.PSObject.Properties.Name)
if (-not $gateway.rpc.ok -or -not $gateway.service.loaded -or $gateway.gateway.bindMode -ne 'loopback' -or
    $gateway.gateway.bindHost -ne '127.0.0.1' -or $gateway.gateway.version -ne $OpenClawVersion -or
    $listenerPids.Count -ne 1 -or @($gateway.extraServices).Count -ne 0 -or
    @($serviceEnvNames | Where-Object { $_ -match '(API_KEY|BASE_URL|ENDPOINT|EXTRA_USAGE)' }).Count -gt 0) {
    Fail 'Gateway is not one clean loopback-only, no-paid-env service.'
}
Wait-FreshPermanentPhoneRoundtrip $serviceStartedAt
Test-PermanentGptSentinel

$watchdog = Join-Path $WinRoot 'install_openclaw_watchdog.ps1'
if (-not (Test-Path -LiteralPath $watchdog -PathType Leaf)) { Fail 'Approved health monitor installer is missing.' }
& powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File $watchdog -Account $Account -Agent $Agent -DryRun
if ($LASTEXITCODE -ne 0) { Fail 'Health monitor dry-run failed.' }
& powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File $watchdog -Account $Account -Agent $Agent -Notify
if ($LASTEXITCODE -ne 0) { Fail 'Health monitor installation failed.' }
$python = Resolve-Python; $pythonArgs = @($python.Prefix); $pythonExe = [string]$python.File
$opsCore = Join-Path $env:USERPROFILE '.openclaw\ops\bin\openclaw_ops.py'
$opsState = Join-Path $env:USERPROFILE '.openclaw\ops'
& $pythonExe @pythonArgs $opsCore check --host-role windows-primary --account $Account --agent $Agent --state-dir $opsState --openclaw-home (Join-Path $env:USERPROFILE '.openclaw')
if ($LASTEXITCODE -ne 0) { Fail 'The installed health monitor did not pass one real read-only check.' }
Install-ProjectionTask

Write-Host 'VERIFY_OK: permanent Gateway, fresh phone roundtrip, GPT subscription/no-fallback, formal Feishu, projection, Remote and monitors passed.'
Write-Host 'Kimi and current same-factpack V88 dual review remain a separate business-certification gate.'
Write-Host 'No reboot was performed. Next: exit Codex and retest phone; then reboot manually and retest once.'
