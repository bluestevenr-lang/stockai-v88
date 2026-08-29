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

function Fail([string]$Message) { Write-Error $Message; exit 1 }

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
    if (-not $state.temp_gateway_pid) { Fail 'Temporary Gateway PID is missing; refusing to stop an unknown process.' }
    $process = Get-Process -Id ([int]$state.temp_gateway_pid) -ErrorAction SilentlyContinue
    if ($process) { Stop-Process -Id $process.Id -Force; Start-Sleep -Seconds 3 }
    $after = & $OpenClaw gateway status --json 2>$null
    if ($LASTEXITCODE -eq 0 -and ($after | ConvertFrom-Json).rpc.ok) { Fail 'An unmanaged Gateway remained after stopping the recorded temporary process.' }
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
$openclawCommand = Get-Command openclaw.cmd, openclaw.exe, openclaw -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $openclawCommand) { Fail 'OpenClaw is not installed.' }
$OpenClaw = $openclawCommand.Source

$blocked = @('OPENAI_API_KEY','ANTHROPIC_API_KEY','GOOGLE_API_KEY','GEMINI_API_KEY','MOONSHOT_API_KEY',
    'KIMI_API_KEY','KIMI_CODE_API_KEY','DEEPSEEK_API_KEY','OPENROUTER_API_KEY','CODEX_API_KEY',
    'OPENAI_BASE_URL','ANTHROPIC_BASE_URL','MOONSHOT_BASE_URL','KIMI_BASE_URL','DEEPSEEK_BASE_URL',
    'OPENROUTER_BASE_URL','OPENAI_EXTRA_USAGE','CODEX_EXTRA_USAGE')
if (@($blocked | Where-Object { [Environment]::GetEnvironmentVariable($_) }).Count -gt 0) { Fail 'A paid API environment variable is present.' }
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

Stop-TemporaryGateway
& $OpenClaw gateway install --force --port 18789
if ($LASTEXITCODE -ne 0) { Fail 'Gateway service installation failed.' }
& $OpenClaw gateway restart
if ($LASTEXITCODE -ne 0) { Fail 'Gateway restart failed.' }
Start-Sleep -Seconds 5
$gateway = (& $OpenClaw gateway status --json) | ConvertFrom-Json
$listenerPids = @($gateway.port.listeners | ForEach-Object { $_.pid } | Sort-Object -Unique)
$serviceEnvNames = @($gateway.service.command.environment.PSObject.Properties.Name)
if (-not $gateway.rpc.ok -or -not $gateway.service.loaded -or $gateway.gateway.bindMode -ne 'loopback' -or
    $gateway.gateway.bindHost -ne '127.0.0.1' -or $gateway.gateway.version -ne $OpenClawVersion -or
    $listenerPids.Count -ne 1 -or @($gateway.extraServices).Count -ne 0 -or
    @($serviceEnvNames | Where-Object { $_ -match '(API_KEY|BASE_URL|ENDPOINT|EXTRA_USAGE)' }).Count -gt 0) {
    Fail 'Gateway is not one clean loopback-only, no-paid-env service.'
}

$watchdog = Join-Path $WinRoot 'install_openclaw_watchdog.ps1'
if (-not (Test-Path -LiteralPath $watchdog -PathType Leaf)) { Fail 'Approved health monitor installer is missing.' }
& powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File $watchdog -Account $Account -Agent $Agent -DryRun
if ($LASTEXITCODE -ne 0) { Fail 'Health monitor dry-run failed.' }
& powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File $watchdog -Account $Account -Agent $Agent
if ($LASTEXITCODE -ne 0) { Fail 'Health monitor installation failed.' }
$python = Resolve-Python; $pythonArgs = @($python.Prefix); $pythonExe = [string]$python.File
$opsCore = Join-Path $env:USERPROFILE '.openclaw\ops\bin\openclaw_ops.py'
$opsState = Join-Path $env:USERPROFILE '.openclaw\ops'
& $pythonExe @pythonArgs $opsCore check --host-role windows-primary --account $Account --agent $Agent --state-dir $opsState --openclaw-home (Join-Path $env:USERPROFILE '.openclaw')
if ($LASTEXITCODE -ne 0) { Fail 'The installed health monitor did not pass one real read-only check.' }
Install-ProjectionTask

Write-Host 'VERIFY_OK: GPT subscription, no-fallback policy, formal Feishu, actual private projection, Remote confirmation, Gateway and monitors passed.'
Write-Host 'Kimi and current same-factpack V88 dual review remain a separate business-certification gate.'
Write-Host 'No reboot was performed. Next: exit Codex and retest phone; then reboot manually and retest once.'
