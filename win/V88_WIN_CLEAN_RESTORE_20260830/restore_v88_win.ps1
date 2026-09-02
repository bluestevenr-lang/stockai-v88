#requires -Version 5.1
[CmdletBinding()]
param(
    [ValidateSet('Prepare','OAuth','Kimi','Feishu','Verify','PostExit','PostReboot')]
    [string]$Stage = 'Prepare',
    [switch]$MacReceiverDisabled,
    [switch]$PhoneAnswerConfirmed,
    [switch]$RemoteMacReadConfirmed,
    [switch]$CodexClosedConfirmed,
    [switch]$HostPreflightReviewed,
    [switch]$SsdHealthExternallyVerified,
    [switch]$UnscopedEventsReviewed,
    [switch]$PowerSettingsExternallyVerified,
    [switch]$TimeSyncExternallyVerified,
    [switch]$PendingRenameReviewed,
    [switch]$RemoteMacReadAfterRebootConfirmed,
    [string]$V88DataPath = (Join-Path $env:USERPROFILE 'Desktop\ai-daily-report-v2\data')
)

$ErrorActionPreference = 'Stop'
$OpenClawVersion = '2026.7.1-2'
$CodexPluginVersion = '2026.7.1-1'
$FeishuPluginVersion = '2026.7.1'
$Agent = 'v88-gpt'
$Account = 'v88-gpt'
$Model = 'openai/gpt-5.6-sol'
$PackageRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$WinRoot = Split-Path -Parent $PackageRoot
$RepoRoot = Split-Path -Parent $WinRoot
$StateDir = Join-Path $env:LOCALAPPDATA 'V88CleanRestore'
$StatePath = Join-Path $StateDir 'state.json'
$Workspace = Join-Path $env:USERPROFILE '.openclaw\workspaces\v88-gpt'
$ManifestPath = Join-Path $PackageRoot 'MANIFEST.sha256'

function Stop-Restore([string]$Message) { throw $Message }

function Get-OwnerFingerprint {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $profile = [IO.Path]::GetFullPath($env:USERPROFILE).TrimEnd('\').ToLowerInvariant()
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [Text.Encoding]::UTF8.GetBytes($identity.User.Value + '|' + $profile)
        return ([BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant()
    } finally { $sha.Dispose() }
}

function Assert-SameRecoveryOwner {
    if (-not (Test-Path -LiteralPath $StatePath -PathType Leaf)) { Stop-Restore 'Recovery state is missing. Run Prepare as the dedicated Windows user.' }
    try { $state = Get-Content -LiteralPath $StatePath -Raw -Encoding UTF8 | ConvertFrom-Json }
    catch { Stop-Restore 'Recovery state is unreadable.' }
    if (-not $state.owner_fingerprint -or $state.owner_fingerprint -ne (Get-OwnerFingerprint) -or
        [string]$state.owner_sid -ne [Security.Principal.WindowsIdentity]::GetCurrent().User.Value) {
        Stop-Restore 'This stage is running under a different Windows user/profile than Prepare.'
    }
}

function Protect-StateDirectory {
    New-Item -ItemType Directory -Path $StateDir -Force | Out-Null
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $acl = New-Object Security.AccessControl.DirectorySecurity
    $acl.SetOwner($identity.User)
    $acl.SetAccessRuleProtection($true, $false)
    foreach ($sidValue in @($identity.User.Value, 'S-1-5-18')) {
        $sid = [Security.Principal.SecurityIdentifier]::new($sidValue)
        $rule = [Security.AccessControl.FileSystemAccessRule]::new($sid, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
        $acl.AddAccessRule($rule)
    }
    Set-Acl -LiteralPath $StateDir -AclObject $acl
}

function Assert-PackageIntegrity {
    if (-not (Test-Path -LiteralPath $ManifestPath -PathType Leaf)) { Stop-Restore 'MANIFEST.sha256 is missing.' }
    $repoPrefix = [IO.Path]::GetFullPath($RepoRoot).TrimEnd('\') + '\'
    foreach ($line in Get-Content -LiteralPath $ManifestPath -Encoding UTF8) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        if ($line -notmatch '^([0-9a-fA-F]{64})  (.+)$') { Stop-Restore 'MANIFEST.sha256 has an invalid line.' }
        $target = [IO.Path]::GetFullPath((Join-Path $PackageRoot $Matches[2].Replace('/', '\')))
        if (-not $target.StartsWith($repoPrefix, [StringComparison]::OrdinalIgnoreCase)) { Stop-Restore 'Manifest path escapes the repository.' }
        if (-not (Test-Path -LiteralPath $target -PathType Leaf)) { Stop-Restore ('Manifest file is missing: ' + $Matches[2]) }
        if ((Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash -ne $Matches[1].ToUpperInvariant()) {
            Stop-Restore ('Manifest hash mismatch: ' + $Matches[2])
        }
    }
}

function Save-State([hashtable]$Patch) {
    New-Item -ItemType Directory -Path $StateDir -Force | Out-Null
    $state = @{}
    if (Test-Path -LiteralPath $StatePath -PathType Leaf) {
        try {
            $old = Get-Content -LiteralPath $StatePath -Raw -Encoding UTF8 | ConvertFrom-Json
            foreach ($p in $old.PSObject.Properties) { $state[$p.Name] = $p.Value }
        } catch { Stop-Restore 'Recovery state is unreadable. Do not reuse old state.' }
    }
    if (-not $state.ContainsKey('recovery_id')) { $state['recovery_id'] = [guid]::NewGuid().ToString() }
    foreach ($key in $Patch.Keys) { $state[$key] = $Patch[$key] }
    $state['updated_at'] = (Get-Date).ToUniversalTime().ToString('o')
    [IO.File]::WriteAllText($StatePath, ($state | ConvertTo-Json -Depth 8), (New-Object Text.UTF8Encoding($false)))
}

function Read-State {
    if (-not (Test-Path -LiteralPath $StatePath -PathType Leaf)) { return $null }
    try { return Get-Content -LiteralPath $StatePath -Raw -Encoding UTF8 | ConvertFrom-Json }
    catch { Stop-Restore 'Recovery state is unreadable. Do not reuse old state.' }
}

function Assert-StateFlags([string[]]$Required) {
    $state = Read-State
    if (-not $state) { Stop-Restore 'Recovery state is missing. Start with Prepare.' }
    foreach ($name in $Required) {
        if (-not $state.$name) { Stop-Restore ('Required earlier stage is not valid: ' + $name) }
    }
}

function Assert-UpstreamMutationAllowed {
    $state = Read-State
    if ($state -and ($state.service_accepted -or $state.gateway_conversion_started)) {
        Stop-Restore 'The permanent Gateway conversion has started. Do not rerun Prepare/OAuth/Kimi/Feishu; resume Verify or use a separately reviewed maintenance/reset procedure.'
    }
}

function Invalidate-Downstream([string]$FromStage) {
    $fieldsByStage = @{
        Prepare = @('prepared','projection_ready','gpt_oauth','gpt_sentinel','kimi_managed_oauth','kimi_sentinel',
            'feishu_configured','phone_roundtrip','remote_mac_read','service_accepted','post_exit','post_reboot','verified')
        OAuth = @('gpt_oauth','gpt_sentinel','kimi_managed_oauth','kimi_sentinel','feishu_configured','phone_roundtrip',
            'remote_mac_read','service_accepted','post_exit','post_reboot','verified')
        Kimi = @('kimi_managed_oauth','kimi_sentinel','feishu_configured','phone_roundtrip','remote_mac_read',
            'service_accepted','post_exit','post_reboot','verified')
        Feishu = @('feishu_configured','phone_roundtrip','remote_mac_read','service_accepted','post_exit','post_reboot','verified')
        Verify = @('service_accepted','post_exit','post_reboot','verified')
        PostExit = @('post_exit','post_reboot','verified')
        PostReboot = @('post_reboot','verified')
    }
    if (-not $fieldsByStage.ContainsKey($FromStage)) { Stop-Restore ('Unknown recovery stage: ' + $FromStage) }
    $patch = @{ gateway_autostart = $false }
    foreach ($field in @($fieldsByStage[$FromStage])) { $patch[$field] = $false }
    Save-State $patch
}

function Assert-ZeroPayEnvironment {
    $blocked = @('OPENAI_API_KEY','ANTHROPIC_API_KEY','GOOGLE_API_KEY','GEMINI_API_KEY',
        'MOONSHOT_API_KEY','KIMI_API_KEY','KIMI_CODE_API_KEY','DEEPSEEK_API_KEY','OPENROUTER_API_KEY',
        'CODEX_API_KEY','OPENAI_BASE_URL','ANTHROPIC_BASE_URL','MOONSHOT_BASE_URL','KIMI_BASE_URL',
        'DEEPSEEK_BASE_URL','OPENROUTER_BASE_URL','OPENAI_EXTRA_USAGE','CODEX_EXTRA_USAGE')
    $present = @($blocked | Where-Object {
        $name = $_
        @('Process','User','Machine') | Where-Object { [Environment]::GetEnvironmentVariable($name, $_) } | Select-Object -First 1
    })
    if ($present.Count -gt 0) {
        Stop-Restore ('Paid API environment variables are present: ' + ($present -join ', ') + '. Remove them before continuing.')
    }
}

function Assert-CleanFirstRun {
    if (Test-Path -LiteralPath $StatePath -PathType Leaf) { return }
    $configPath = Join-Path $env:USERPROFILE '.openclaw\openclaw.json'
    if (Test-Path -LiteralPath $configPath -PathType Leaf) {
        try { $config = Get-Content -LiteralPath $configPath -Raw -Encoding UTF8 | ConvertFrom-Json }
        catch { Stop-Restore 'Existing OpenClaw configuration is unreadable. Use a clean Windows profile.' }
        $extraAgents = @($config.agents.list | Where-Object { $_.id -and $_.id -ne 'main' })
        $bindings = @($config.bindings)
        $accounts = @()
        if ($config.channels.feishu.accounts) { $accounts = @($config.channels.feishu.accounts.PSObject.Properties.Name) }
        if ($extraAgents.Count -gt 0 -or $bindings.Count -gt 0 -or $accounts.Count -gt 0 -or
            $config.channels.feishu.appId -or $config.channels.feishu.appSecret) {
            Stop-Restore 'Old OpenClaw agents, Feishu routes or credentials exist. Do not merge them into the clean restore.'
        }
    }
    foreach ($taskName in @('OpenClaw Gateway','V88 OpenClaw Health','V88 OpenClaw Projection')) {
        if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
            Stop-Restore ('Old scheduled task exists: ' + $taskName + '. Remove it before clean restore.')
        }
    }
}

function Resolve-OpenClaw {
    $command = Get-Command openclaw.cmd, openclaw.exe, openclaw -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $command) { Stop-Restore 'OpenClaw is not available on PATH.' }
    return $command.Source
}

function Invoke-OpenClaw([string[]]$Arguments) {
    $openclaw = Resolve-OpenClaw
    & $openclaw @Arguments
    if ($LASTEXITCODE -ne 0) { Stop-Restore ('OpenClaw failed: ' + ($Arguments -join ' ')) }
}

function Resolve-Python {
    foreach ($name in @('py.exe','python.exe','python3.exe')) {
        $command = Get-Command $name -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $command -or $command.Source -match '\\WindowsApps\\') { continue }
        $prefix = @(); if ($name -eq 'py.exe') { $prefix = @('-3') }
        & $command.Source @prefix -c 'import sys; raise SystemExit(0 if sys.version_info >= (3,10) else 2)'
        if ($LASTEXITCODE -eq 0) { return @{ File = $command.Source; Prefix = $prefix } }
    }
    Stop-Restore 'Python 3.10+ is required.'
}

function Get-SupportedNodeVersion {
    $node = Get-Command node.exe, node -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $node) { Stop-Restore 'Node.js is missing.' }
    try { $version = [version]((& $node.Source --version 2>$null | Out-String).Trim().TrimStart('v')) }
    catch { Stop-Restore 'Node.js version is unreadable.' }
    $supported = (($version.Major -eq 22 -and $version -ge [version]'22.22.3') -or
        ($version.Major -eq 24 -and $version -ge [version]'24.15.0') -or
        ($version.Major -eq 25 -and $version -ge [version]'25.9.0'))
    if (-not $supported) { Stop-Restore 'Pinned OpenClaw 2026.7.1-2 requires Node 22.22.3-22.x, 24.15-24.x, or 25.9-25.x.' }
    return $version.ToString()
}

function Install-PinnedRuntime {
    $npm = Get-Command npm.cmd, npm -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $npm) {
        $winget = Get-Command winget.exe -ErrorAction SilentlyContinue
        if (-not $winget) { Stop-Restore 'Node.js/npm is missing. Install the current Node.js LTS, then rerun Prepare.' }
        & $winget.Source install --id OpenJS.NodeJS.LTS --exact --accept-package-agreements --accept-source-agreements
        if ($LASTEXITCODE -ne 0) { Stop-Restore 'Node.js installation failed.' }
        $env:PATH = "$env:ProgramFiles\nodejs;$env:APPDATA\npm;$env:PATH"
        $npm = Get-Command npm.cmd, npm -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
    }
    if (-not $npm) { Stop-Restore 'npm is still unavailable after Node.js setup.' }
    [void](Get-SupportedNodeVersion)
    & $npm.Source install -g ("openclaw@{0}" -f $OpenClawVersion)
    if ($LASTEXITCODE -ne 0) { Stop-Restore 'Pinned OpenClaw installation failed.' }
    Invoke-OpenClaw -Arguments @('plugins','install',("@openclaw/codex@{0}" -f $CodexPluginVersion),'--pin','--force')
    Invoke-OpenClaw -Arguments @('plugins','install',("@openclaw/feishu@{0}" -f $FeishuPluginVersion),'--pin','--force')
    Invoke-OpenClaw -Arguments @('plugins','enable','openai')
    Invoke-OpenClaw -Arguments @('plugins','enable','codex')
    Invoke-OpenClaw -Arguments @('plugins','enable','feishu')
    $actual = (& (Resolve-OpenClaw) --version 2>&1 | Out-String)
    if ($actual -notmatch [regex]::Escape($OpenClawVersion)) { Stop-Restore 'Installed OpenClaw version is not the pinned version.' }
    $plugins = (& (Resolve-OpenClaw) plugins list --json) | ConvertFrom-Json
    foreach ($expected in @(@('openai',$FeishuPluginVersion),@('codex',$CodexPluginVersion),@('feishu',$FeishuPluginVersion))) {
        $match = @($plugins.plugins | Where-Object { $_.id -eq $expected[0] -and $_.status -eq 'loaded' -and $_.version -eq $expected[1] })
        if ($match.Count -ne 1) { Stop-Restore ('Pinned plugin is not loaded: ' + $expected[0]) }
    }
}

function Set-ReadOnlyAgent {
    New-Item -ItemType Directory -Path $Workspace -Force | Out-Null
    Copy-Item -LiteralPath (Join-Path $PackageRoot 'AGENTS-GPT.md') -Destination (Join-Path $Workspace 'AGENTS.md') -Force
    $agents = @((& (Resolve-OpenClaw) agents list --json) | ConvertFrom-Json)
    if ($Agent -notin @($agents.id)) {
        Invoke-OpenClaw -Arguments @('agents','add',$Agent,'--non-interactive','--workspace',$Workspace,'--model',$Model)
    }
    $configPath = (& (Resolve-OpenClaw) config file).Trim()
    $config = Get-Content -LiteralPath $configPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $index = -1
    for ($i = 0; $i -lt $config.agents.list.Count; $i++) { if ($config.agents.list[$i].id -eq $Agent) { $index = $i; break } }
    if ($index -lt 0) { Stop-Restore 'v88-gpt agent was not created.' }
    $toolPolicy = [ordered]@{
        profile = 'coding'; allow = @('read')
        deny = @('write','edit','apply_patch','exec','process','browser','web_search','web_fetch','message',
            'sessions_spawn','sessions_send','cron','gateway','nodes','computer')
        codeMode = $false; elevated = @{ enabled = $false }; exec = @{ mode = 'deny' }
        fs = @{ workspaceOnly = $true }
        message = @{ allowCrossContextSend = $false; crossContext = @{ allowWithinProvider = $false; allowAcrossProviders = $false }; broadcast = @{ enabled = $false } }
    }
    $ops = @(
        @{ path = 'agents.defaults.model'; value = @{ primary = $Model; fallbacks = @() } },
        @{ path = 'agents.defaults.models'; value = @{ $Model = @{ agentRuntime = @{ id = 'openclaw' } } } },
        @{ path = ("agents.list[{0}].model" -f $index); value = @{ primary = $Model; fallbacks = @() } },
        @{ path = ("agents.list[{0}].tools" -f $index); value = $toolPolicy },
        @{ path = ("agents.list[{0}].subagents" -f $index); value = @{ allowAgents = @(); requireAgentId = $true } },
        @{ path = 'gateway.mode'; value = 'local' }
    )
    $temp = Join-Path $StateDir 'agent-config.json'
    New-Item -ItemType Directory -Path $StateDir -Force | Out-Null
    [IO.File]::WriteAllText($temp, ($ops | ConvertTo-Json -Depth 20), (New-Object Text.UTF8Encoding($false)))
    try { Invoke-OpenClaw -Arguments @('config','set','--batch-file',$temp,'--replace') } finally { Remove-Item -LiteralPath $temp -Force -ErrorAction SilentlyContinue }
    Invoke-OpenClaw -Arguments @('agents','set-identity','--agent',$Agent,'--name','V88-GPT 龙虾','--emoji','🦞','--theme','证据优先、GPT主审、K3独立复核、只读脱敏')
    Invoke-OpenClaw -Arguments @('config','validate')
}

function Assert-OnlyOpenAiOAuth {
    $raw = & (Resolve-OpenClaw) models auth list --agent $Agent --json
    if ($LASTEXITCODE -ne 0) { Stop-Restore 'Cannot read model authentication state.' }
    $auth = $raw | ConvertFrom-Json
    $bad = @($auth.profiles | Where-Object { $_.provider -ne 'openai' -or $_.type -ne 'oauth' })
    $good = @($auth.profiles | Where-Object { $_.provider -eq 'openai' -and $_.type -eq 'oauth' })
    if ($bad.Count -gt 0 -or $good.Count -ne 1) { Stop-Restore 'Authentication must contain exactly one OpenAI OAuth profile and no API-key profiles.' }
}

function Test-OpenAiSubscription {
    $raw = & (Resolve-OpenClaw) models status --agent $Agent --json --probe --probe-provider openai --probe-max-tokens 8 --probe-timeout 30000
    if ($LASTEXITCODE -ne 0) { Stop-Restore 'OpenAI subscription probe failed.' }
    $status = $raw | ConvertFrom-Json
    $provider = @($status.auth.providers | Where-Object { $_.provider -eq 'openai' })
    $routes = @($status.auth.runtimeAuthRoutes | Where-Object { $_.provider -eq 'openai' -and $_.status -eq 'usable' })
    $probes = @($status.auth.probes.results | Where-Object { $_.provider -eq 'openai' -and $_.status -eq 'ok' -and $_.mode -eq 'oauth' })
    if ($status.resolvedDefault -ne $Model -or @($status.fallbacks).Count -ne 0 -or $provider.Count -ne 1 -or
        $provider[0].profiles.oauth -ne 1 -or $provider[0].profiles.apiKey -ne 0 -or $provider[0].profiles.token -ne 0 -or
        $routes.Count -lt 1 -or $probes.Count -lt 1) { Stop-Restore 'OpenAI route is not exactly one usable OAuth route with no fallback/API key.' }
}

function Test-KimiManaged {
    $kimi = Get-Command kimi.exe, kimi.cmd, kimi -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $kimi) { Stop-Restore 'Official Kimi Code CLI is missing.' }
    $providers = (& $kimi.Source provider list 2>&1 | Out-String)
    $providerLines = @($providers -split "`r?`n" | Where-Object { $_ -match '^\S+\s+type=' })
    if ($providerLines.Count -ne 1 -or $providerLines[0] -notmatch '^managed:kimi-code\s+type=kimi\b' -or
        $providers -notmatch 'Default model:\s*kimi-code/k3-256k') {
        Stop-Restore 'Kimi must use only managed:kimi-code with default kimi-code/k3-256k.'
    }
    $reply = & $kimi.Source --model 'kimi-code/k3-256k' --prompt 'Reply only: WIN_V88_K3_MANAGED_OK' --output-format text 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0 -or $reply -notmatch 'WIN_V88_K3_MANAGED_OK') { Stop-Restore 'Kimi Code managed OAuth sentinel failed.' }
}

function Invoke-Projection([string]$SourcePath) {
    $source = [IO.Path]::GetFullPath($SourcePath)
    if ($source.Contains('"')) { Stop-Restore 'V88 data path contains an unsupported quote character.' }
    if (-not (Test-Path -LiteralPath $source -PathType Container)) { Stop-Restore 'V88 data directory is missing. Clone/sync the private repository first.' }
    $python = Resolve-Python
    $pythonExe = [string]$python.File
    $tests = Join-Path $WinRoot 'openclaw-v88\projection_tests.py'
    $script = Join-Path $WinRoot 'openclaw-v88\sync_v88_projection_win.py'
    $pythonArgs = @($python.Prefix)
    & $pythonExe @pythonArgs $tests
    if ($LASTEXITCODE -ne 0) { Stop-Restore 'Projection tests failed.' }
    $dest = Join-Path $Workspace 'context'
    & $pythonExe @pythonArgs $script --source $source --dest $dest
    if ($LASTEXITCODE -ne 0) { Stop-Restore 'Privacy-minimized V88 projection failed.' }
    foreach ($required in @('overview.json','name_index.json')) {
        if (-not (Test-Path -LiteralPath (Join-Path $dest $required) -PathType Leaf)) { Stop-Restore ('Projection output missing: ' + $required) }
    }
    if (@(Get-ChildItem -LiteralPath (Join-Path $dest 'stocks') -Filter '*.json' -File).Count -lt 1) { Stop-Restore 'Projection contains no stock documents.' }
    $blockedPattern = '"(account|account_id|assets|balance|cash|cost|cost_basis|holdings|market_value|positions|qty|quantity|shares|total_assets)"\s*:'
    if (Get-ChildItem -LiteralPath $dest -Filter '*.json' -File -Recurse | Select-String -Pattern $blockedPattern -Quiet) {
        Stop-Restore 'Privacy scan found a blocked field in the generated projection.'
    }
    Save-State @{ v88_data_path = $source; projection_ready = $true; projection_destination = $dest }
}

function Start-TemporaryGateway {
    $json = & (Resolve-OpenClaw) gateway status --json 2>$null
    if ($LASTEXITCODE -eq 0) {
        $existing = $json | ConvertFrom-Json
        if ($existing.rpc.ok) {
            $saved = $null
            if (Test-Path -LiteralPath $StatePath -PathType Leaf) { $saved = Get-Content -LiteralPath $StatePath -Raw -Encoding UTF8 | ConvertFrom-Json }
            $pids = @($existing.port.listeners | ForEach-Object { $_.pid } | Sort-Object -Unique)
            if (-not $saved.temp_gateway_pid -or $pids.Count -ne 1 -or [int]$saved.temp_gateway_pid -ne [int]$pids[0]) {
                Stop-Restore 'An unmanaged Gateway is already running; clean restore will not take it over.'
            }
            return
        }
    }
    Start-Process -FilePath (Resolve-OpenClaw) -ArgumentList @('gateway','--port','18789') -WindowStyle Hidden | Out-Null
    Start-Sleep -Seconds 5
    $status = (& (Resolve-OpenClaw) gateway status --json) | ConvertFrom-Json
    $pids = @($status.port.listeners | ForEach-Object { $_.pid } | Sort-Object -Unique)
    if (-not $status.rpc.ok -or $status.gateway.bindMode -ne 'loopback' -or $pids.Count -ne 1) { Stop-Restore 'Temporary Gateway did not become healthy and loopback-only.' }
    $process = Get-Process -Id ([int]$pids[0]) -ErrorAction Stop
    Save-State @{
        temp_gateway_pid = [int]$pids[0]
        temp_gateway_start_utc = $process.StartTime.ToUniversalTime().ToString('o')
        temp_gateway_owner_fingerprint = Get-OwnerFingerprint
        gateway_autostart = $false
    }
}

function Test-GptSentinel {
    Start-TemporaryGateway
    Test-OpenAiSubscription
    $raw = & (Resolve-OpenClaw) agent --agent $Agent --session-id 'win-clean-restore-gpt' --message 'Reply only: WIN_V88_GPT_OAUTH_OK' --thinking low --timeout 180 --json
    if ($LASTEXITCODE -ne 0) { Stop-Restore 'GPT subscription sentinel failed.' }
    $doc = $raw | ConvertFrom-Json
    $text = [string]$doc.result.payloads[0].text
    $provider = [string]$doc.result.meta.agentMeta.provider
    $fallback = [bool]$doc.result.meta.executionTrace.fallbackUsed
    if ($text -notmatch 'WIN_V88_GPT_OAUTH_OK' -or $provider -ne 'openai' -or $fallback) { Stop-Restore 'GPT sentinel did not prove the expected no-fallback subscription route.' }
}

function Configure-Feishu {
    if (-not $MacReceiverDisabled) { Stop-Restore 'First disable the Mac v88-gpt receiver, then rerun Feishu with -MacReceiverDisabled.' }
    $appId = (Read-Host 'Enter the formal V88-GPT Feishu App ID').Trim()
    if ($appId -notmatch '^cli_[A-Za-z0-9]+$') { Stop-Restore 'Invalid Feishu App ID format.' }
    $secure = Read-Host 'Enter the formal V88-GPT Feishu App Secret' -AsSecureString
    $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    $secret = $null
    $temp = Join-Path $StateDir 'feishu-config.json'
    try {
        $secret = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
        if ([string]::IsNullOrWhiteSpace($secret)) { Stop-Restore 'Feishu App Secret is empty.' }
        $ops = @(
            @{ path = 'channels.feishu.enabled'; value = $true },
            @{ path = 'channels.feishu.connectionMode'; value = 'websocket' },
            @{ path = 'channels.feishu.defaultAccount'; value = $Account },
            @{ path = 'channels.feishu.dmPolicy'; value = 'pairing' },
            @{ path = 'channels.feishu.groupPolicy'; value = 'disabled' },
            @{ path = 'channels.feishu.accounts'; value = @{ $Account = @{ appId = $appId; appSecret = $secret; enabled = $true } } }
        )
        [IO.File]::WriteAllText($temp, ($ops | ConvertTo-Json -Depth 12), (New-Object Text.UTF8Encoding($false)))
        Invoke-OpenClaw -Arguments @('config','set','--batch-file',$temp,'--replace')
    } finally {
        if ($ptr -ne [IntPtr]::Zero) { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr) }
        $secret = $null; $secure = $null
        Remove-Item -LiteralPath $temp -Force -ErrorAction SilentlyContinue
    }
    foreach ($binding in @((& (Resolve-OpenClaw) agents bindings --json) | ConvertFrom-Json)) {
        if ($binding.match.channel -eq 'feishu') {
            Invoke-OpenClaw -Arguments @('agents','unbind','--agent',$binding.agentId,'--bind',("feishu:{0}" -f $binding.match.accountId))
        }
    }
    foreach ($legacy in @('channels.feishu.appId','channels.feishu.appSecret')) {
        & (Resolve-OpenClaw) config unset $legacy 2>$null | Out-Null
    }
    Invoke-OpenClaw -Arguments @('agents','bind','--agent',$Agent,'--bind',("feishu:{0}" -f $Account))
    Invoke-OpenClaw -Arguments @('config','validate')
    Start-TemporaryGateway
    $connected = $false
    for ($i = 0; $i -lt 12; $i++) {
        try {
            $channel = (& (Resolve-OpenClaw) channels status --json) | ConvertFrom-Json
            $formal = @($channel.channelAccounts.feishu | Where-Object { $_.accountId -eq $Account })
            if ($formal.Count -eq 1 -and $formal[0].configured -and $formal[0].enabled -and
                $formal[0].running -and $formal[0].connected -and -not $formal[0].lastError) {
                $connected = $true
                break
            }
        } catch { }
        Start-Sleep -Seconds 5
    }
    if (-not $connected) { Stop-Restore 'Formal Feishu account did not become connected within 60 seconds.' }
}

Assert-ZeroPayEnvironment
if ($env:OS -ne 'Windows_NT') { Stop-Restore 'This recovery package must run on Windows.' }
Assert-PackageIntegrity
Protect-StateDirectory

switch ($Stage) {
    'Prepare' {
        Assert-UpstreamMutationAllowed
        if (Test-Path -LiteralPath $StatePath -PathType Leaf) { Assert-SameRecoveryOwner }
        else { Assert-CleanFirstRun }
        & (Join-Path $PackageRoot 'preflight_new_ssd_win.ps1') -Phase Host
        if ($LASTEXITCODE -notin @(0,2,3)) { Stop-Restore 'Windows host preflight did not complete.' }
        if ($LASTEXITCODE -eq 2) { Stop-Restore 'Windows host preflight is blocked.' }
        if ($LASTEXITCODE -eq 3) {
            $preflightPath = Join-Path $StateDir 'preflight-host.json'
            if (-not (Test-Path -LiteralPath $preflightPath -PathType Leaf)) { Stop-Restore 'Host preflight report is missing.' }
            $preflight = Get-Content -LiteralPath $preflightPath -Raw -Encoding UTF8 | ConvertFrom-Json
            $manualCodes = @($preflight.results | Where-Object { $_.status -eq 'MANUAL' } | ForEach-Object { $_.code })
            $reviewRules = @{
                AUTOLOGON_PRESENT_REVIEW_SECURITY = [bool]$HostPreflightReviewed
                SMART_COUNTERS_UNAVAILABLE = [bool]$SsdHealthExternallyVerified
                PHYSICAL_DISK_MAPPING_UNKNOWN = [bool]$SsdHealthExternallyVerified
                SSD_REPORTED_CORRECTED_ERRORS = [bool]$SsdHealthExternallyVerified
                UNSCOPED_STORAGE_OR_PCIE_EVENT_REVIEW = [bool]$UnscopedEventsReviewed
                POWER_SETTINGS_UNKNOWN = [bool]$PowerSettingsExternallyVerified
                TIME_SYNC_NOT_PROVEN = [bool]$TimeSyncExternallyVerified
                TIME_SYNC_QUERY_FAILED = [bool]$TimeSyncExternallyVerified
                PENDING_FILE_RENAME_REVIEW = [bool]$PendingRenameReviewed
            }
            $unknownManual = @($manualCodes | Where-Object { -not $reviewRules.ContainsKey($_) })
            if ($unknownManual.Count -gt 0) {
                Stop-Restore ('Host preflight has an unsupported manual condition that cannot be overridden: ' + ($unknownManual -join ', '))
            }
            $missingReviews = @($manualCodes | Where-Object { -not $reviewRules[$_] })
            if ($missingReviews.Count -gt 0) {
                Stop-Restore ('Host preflight needs matching item-by-item confirmation switches for: ' + ($missingReviews -join ', '))
            }
        }
        Save-State @{
            owner_fingerprint = Get-OwnerFingerprint
            owner_sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
            owner_profile = [IO.Path]::GetFullPath($env:USERPROFILE).TrimEnd('\')
            host_preflight_hash = (Get-FileHash -LiteralPath (Join-Path $StateDir 'preflight-host.json') -Algorithm SHA256).Hash.ToLowerInvariant()
            host_preflight_at = (Get-Item -LiteralPath (Join-Path $StateDir 'preflight-host.json')).LastWriteTimeUtc.ToString('o')
            host_review_autologon = [bool]$HostPreflightReviewed
            host_review_ssd_external = [bool]$SsdHealthExternallyVerified
            host_review_unscoped_events = [bool]$UnscopedEventsReviewed
            host_review_power = [bool]$PowerSettingsExternallyVerified
            host_review_time = [bool]$TimeSyncExternallyVerified
            host_review_pending_rename = [bool]$PendingRenameReviewed
        }
        Invalidate-Downstream 'Prepare'
        Install-PinnedRuntime
        Set-ReadOnlyAgent
        Invoke-Projection $V88DataPath
        $node = Get-Command node.exe, node -CommandType Application -ErrorAction Stop | Select-Object -First 1
        $nodeVersion = Get-SupportedNodeVersion
        Save-State @{
            prepared = $true
            owner_fingerprint = Get-OwnerFingerprint
            owner_sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
            owner_profile = [IO.Path]::GetFullPath($env:USERPROFILE).TrimEnd('\')
            node_path = [IO.Path]::GetFullPath($node.Source)
            node_version = $nodeVersion
            openclaw_path = [IO.Path]::GetFullPath((Resolve-OpenClaw))
            openclaw_version = $OpenClawVersion
            gateway_autostart = $false
            secrets_migrated = $false
        }
        Write-Host 'PREPARE_OK. Next: RESTORE_V88.bat -Stage OAuth'
    }
    'OAuth' {
        Assert-SameRecoveryOwner
        Assert-UpstreamMutationAllowed
        Assert-StateFlags @('prepared','projection_ready')
        Invalidate-Downstream 'OAuth'
        Invoke-OpenClaw -Arguments @('models','auth','login','--provider','openai','--force')
        Assert-OnlyOpenAiOAuth
        Test-GptSentinel
        Save-State @{ gpt_oauth = $true; gpt_sentinel = $true; gateway_autostart = $false }
        Write-Host 'GPT_OAUTH_OK. Install/login to official Kimi Code, then run Kimi stage.'
    }
    'Kimi' {
        Assert-SameRecoveryOwner
        Assert-UpstreamMutationAllowed
        Assert-StateFlags @('prepared','projection_ready','gpt_oauth','gpt_sentinel')
        Invalidate-Downstream 'Kimi'
        Test-KimiManaged
        Save-State @{ kimi_managed_oauth = $true; kimi_sentinel = $true }
        Write-Host 'KIMI_MANAGED_OK. This proves the independent review seat, not current V88 business certification.'
    }
    'Feishu' {
        Assert-SameRecoveryOwner
        Assert-UpstreamMutationAllowed
        Assert-StateFlags @('prepared','projection_ready','gpt_oauth','gpt_sentinel','kimi_managed_oauth','kimi_sentinel')
        Invalidate-Downstream 'Feishu'
        Assert-OnlyOpenAiOAuth
        Configure-Feishu
        Save-State @{ feishu_configured = $true; mac_receiver_disabled = $true; phone_roundtrip = $false; gateway_autostart = $false }
        Write-Host 'FEISHU_CONNECTED_PENDING_TEST. Send a natural-language message from the phone, approve pairing if asked, then send it again.'
        Write-Host 'After a real answer and fresh Mac Remote read: RESTORE_V88.bat -Stage Verify -MacReceiverDisabled -PhoneAnswerConfirmed -RemoteMacReadConfirmed'
    }
    'Verify' {
        Assert-SameRecoveryOwner
        Assert-StateFlags @('prepared','projection_ready','gpt_oauth','gpt_sentinel','kimi_managed_oauth','kimi_sentinel','feishu_configured')
        Invalidate-Downstream 'Verify'
        if (-not $MacReceiverDisabled -or -not $PhoneAnswerConfirmed -or -not $RemoteMacReadConfirmed) {
            Stop-Restore 'Verify requires Mac receiver disabled, real phone answer, and a fresh Mac Remote read confirmation.'
        }
        & (Join-Path $PackageRoot 'verify_v88_win.ps1') -MacReceiverDisabled -PhoneAnswerConfirmed -RemoteMacReadConfirmed -V88DataPath $V88DataPath
        if ($LASTEXITCODE -ne 0) {
            Stop-Restore 'Verification failed. Inspect the actual Gateway task, listener and Feishu state before retry or rollback; do not assume autostart was not installed.'
        }
        Save-State @{
            service_accepted = $true
            post_exit = $false
            post_reboot = $false
            verified = $false
            phone_roundtrip = $true
            remote_mac_read = $true
            gateway_autostart = $true
        }
        Write-Host 'SERVICE_ACCEPTED. Next: close Codex/ChatGPT Desktop and run -Stage PostExit -CodexClosedConfirmed.'
    }
    'PostExit' {
        Assert-SameRecoveryOwner
        Assert-StateFlags @('service_accepted')
        Invalidate-Downstream 'PostExit'
        if (-not $CodexClosedConfirmed) { Stop-Restore 'PostExit requires -CodexClosedConfirmed after closing Codex/ChatGPT Desktop.' }
        & (Join-Path $PackageRoot 'accept_runtime_win.ps1') -Mode Exit
        if ($LASTEXITCODE -ne 0) { Stop-Restore 'Post-exit acceptance failed.' }
        $postExitBoot = (Get-CimInstance Win32_OperatingSystem).LastBootUpTime.ToUniversalTime().ToString('o')
        Save-State @{ post_exit = $true; post_exit_boot_utc = $postExitBoot; post_reboot = $false; verified = $false; gateway_autostart = $true }
        Write-Host 'POST_EXIT_OK. Reboot manually, allow the same Windows user to log in, then run -Stage PostReboot.'
    }
    'PostReboot' {
        Assert-SameRecoveryOwner
        Assert-StateFlags @('service_accepted','post_exit')
        $before = Read-State
        if (-not $before.post_exit_boot_utc) { Stop-Restore 'PostExit boot marker is missing.' }
        Invalidate-Downstream 'PostReboot'
        if (-not $RemoteMacReadAfterRebootConfirmed) {
            Stop-Restore 'PostReboot requires a fresh Mac read of the newly paired Windows Remote task.'
        }
        & (Join-Path $PackageRoot 'accept_runtime_win.ps1') -Mode Reboot
        if ($LASTEXITCODE -ne 0) { Stop-Restore 'Post-reboot acceptance failed.' }
        Save-State @{ post_reboot = $true; verified = $true; gateway_autostart = $true; remote_mac_read_after_reboot = $true }
        Write-Host 'V88 WINDOWS HOST VERIFIED: login-after-reboot 7x24 service is accepted.'
    }
}
