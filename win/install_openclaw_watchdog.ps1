#requires -Version 5.1
<#
Install only the V88 health monitor; never starts/reconfigures the Gateway.
DryRun validates the local plan without writing files or registering a task.
Undo removes only this monitor task; local status history is preserved.

Microsoft references (verified 2026-08-29):
https://learn.microsoft.com/powershell/module/scheduledtasks/new-scheduledtasktrigger
https://learn.microsoft.com/powershell/module/scheduledtasks/new-scheduledtaskprincipal
https://learn.microsoft.com/windows/win32/taskschd/repetitionpattern-duration
Interactive requires an existing user login. Omitted repetition duration means indefinite.
#>
[CmdletBinding(DefaultParameterSetName = 'Install')]
param(
    [Parameter(ParameterSetName = 'Install')][string]$Account,
    [Parameter(ParameterSetName = 'Install')][string]$Agent,
    [Parameter(ParameterSetName = 'Install')][switch]$Notify,
    [Parameter(ParameterSetName = 'Undo', Mandatory = $true)][switch]$Undo,
    [switch]$DryRun
)
$ErrorActionPreference = 'Stop'
$TaskName = 'V88 OpenClaw Health'
$TaskMarker = 'V88 OpenClaw Health monitor v1; read-only probes; current-user login required.'

function Get-Property($Object, [string]$Name) {
    if ($null -eq $Object) { return $null }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) { return $null }
    return $property.Value
}

function Assert-SafeAlias([string]$Value, [string]$Label) {
    if ($Value -notmatch '^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$') {
        throw "$Label must be an existing configuration alias (letters, numbers, dot, dash or underscore)."
    }
}

function Quote-Literal([string]$Value) { return "'" + $Value.Replace("'", "''") + "'" }

function Assert-NoReparse([string]$ItemPath) {
    $cursor = [IO.Path]::GetFullPath($ItemPath)
    while ($cursor) {
        if (Test-Path -LiteralPath $cursor) {
            if ((Get-Item -LiteralPath $cursor -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) {
                throw 'A monitor path uses a junction/symlink. Refusing an ambiguous destination.'
            }
        }
        $parent = [IO.Directory]::GetParent($cursor)
        if ($null -eq $parent) { break }
        $cursor = $parent.FullName
    }
}

function Resolve-Python {
    $candidates = @()
    $venv = Join-Path $env:USERPROFILE 'v88env\Scripts\python.exe'
    if (Test-Path -LiteralPath $venv -PathType Leaf) {
        $candidates += @{ File = $venv; Prefix = @() }
    }
    foreach ($name in @('python.exe', 'python3.exe', 'py.exe')) {
        $command = Get-Command $name -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($command -and $command.Source -notmatch '\\WindowsApps\\') {
            $prefix = @()
            if ($name -eq 'py.exe') { $prefix = @('-3') }
            $candidates += @{ File = $command.Source; Prefix = $prefix }
        }
    }
    foreach ($candidate in $candidates) {
        try {
            $probeArgs = @($candidate.Prefix) + @('-c', 'import sys; print(sys.executable) if sys.version_info >= (3, 10) else sys.exit(2)')
            $result = @(& $candidate.File @probeArgs 2>$null)
            if ($LASTEXITCODE -eq 0 -and $result.Count -eq 1 -and
                (Test-Path -LiteralPath ([string]$result[0]) -PathType Leaf)) {
                return [IO.Path]::GetFullPath([string]$result[0])
            }
        } catch { }
    }
    throw 'An existing Python 3.10+ interpreter is required. Nothing was installed.'
}

try {
    if ($env:OS -ne 'Windows_NT') { throw 'This installer must be run on Windows.' }
    Import-Module ScheduledTasks -ErrorAction Stop
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $currentSid = $identity.User.Value
    $existing = Get-ScheduledTask -TaskName $TaskName -TaskPath '\' -ErrorAction SilentlyContinue
    if ($existing -and ($existing.Description -ne $TaskMarker -or
        $existing.Principal.UserId -notin @($currentSid, $identity.Name))) {
        throw 'A different task already uses this name. It has not been modified.'
    }
    if ($Undo) {
        if ($DryRun) { Write-Host 'DRY_RUN: remove only V88 OpenClaw Health; preserve all files and history.'; exit 0 }
        if ($existing) {
            Stop-ScheduledTask -TaskName $TaskName -TaskPath '\' -ErrorAction SilentlyContinue
            Unregister-ScheduledTask -TaskName $TaskName -TaskPath '\' -Confirm:$false
        }
        Write-Host 'Monitor task removed (if installed). Gateway and status history were not changed.'
        exit 0
    }

    $OpenClawHome = Join-Path $env:USERPROFILE '.openclaw'
    $ConfigPath = Join-Path $OpenClawHome 'openclaw.json'
    $StateDir = Join-Path $OpenClawHome 'ops'
    $BinDir = Join-Path $StateDir 'bin'
    $InstalledCore = Join-Path $BinDir 'openclaw_ops.py'
    $RunnerPath = Join-Path $BinDir 'run_health.ps1'
    $SourceCore = Join-Path $PSScriptRoot 'openclaw_ops.py'
    foreach ($item in @($OpenClawHome, $StateDir, $BinDir, $InstalledCore, $RunnerPath)) { Assert-NoReparse $item }
    if (-not (Test-Path -LiteralPath $SourceCore -PathType Leaf)) { throw 'Missing adjacent openclaw_ops.py; update the installation package first.' }
    $openclawCommand = Get-Command openclaw.cmd, openclaw.exe, openclaw -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $openclawCommand) { throw 'An existing OpenClaw installation on PATH is required. Nothing was installed.' }
    $OpenClawBin = Split-Path -Parent $openclawCommand.Source
    $PythonExe = Resolve-Python
    if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) { throw 'No current-user OpenClaw configuration exists. No account was created.' }
    try { $config = Get-Content -LiteralPath $ConfigPath -Raw -Encoding UTF8 | ConvertFrom-Json }
    catch { throw 'Cannot parse current-user openclaw.json as JSON. No configuration was changed.' }
    $feishu = Get-Property (Get-Property $config 'channels') 'feishu'
    if ($null -eq $feishu) { throw 'Feishu is not configured. No bot/account was created.' }
    $accountTable = Get-Property $feishu 'accounts'
    $accountNames = @()
    if ($accountTable) { $accountNames = @($accountTable.PSObject.Properties.Name) }
    if ($accountNames.Count -eq 0 -and (Get-Property $feishu 'appId')) { $accountNames = @('default') }
    if ($accountNames.Count -eq 0) { throw 'No explicit Feishu account is configured.' }
    $agentList = @((Get-Property (Get-Property $config 'agents') 'list'))
    $agentNames = @($agentList | ForEach-Object { Get-Property $_ 'id' } | Where-Object { $_ })
    if (-not $Account) {
        if ($DryRun) { throw 'DryRun requires explicit -Account and -Agent aliases; no account will be guessed.' }
        Write-Host ('Configured account aliases: ' + ($accountNames -join ', '))
        $Account = Read-Host 'Enter the Feishu account alias to monitor (not App ID)'
    }
    if (-not $Agent) {
        if ($DryRun) { throw 'DryRun requires explicit -Account and -Agent aliases; no agent will be guessed.' }
        Write-Host ('Configured agent aliases: ' + ($agentNames -join ', '))
        $Agent = Read-Host 'Enter the existing agent alias'
    }
    Assert-SafeAlias $Account 'Account'
    Assert-SafeAlias $Agent 'Agent'
    if ($Account -cnotin $accountNames -or $Agent -cnotin $agentNames) { throw 'The exact account/agent alias was not found in the current-user configuration.' }
    $matchingBindings = @((Get-Property $config 'bindings') | Where-Object {
        $match = Get-Property $_ 'match'
        (Get-Property $match 'channel') -ceq 'feishu' -and
        (Get-Property $match 'accountId') -ceq $Account -and
        (Get-Property $_ 'agentId') -ceq $Agent
    })
    if ($matchingBindings.Count -eq 0) { throw 'No explicit Feishu account-to-agent binding matches. No binding was guessed or created.' }
    $config = $null; $feishu = $null; $accountTable = $null

    $plan = [ordered]@{
        task = $TaskName; role = 'windows-primary'; account = $Account; agent = $Agent
        schedule = 'Every 5 minutes plus current-user logon'; requires_user_login = $true
        notifications = [bool]$Notify; gateway_changed = $false; model_test = 'UNTESTED'
        install_directory = $StateDir; runtime_acceptance = 'NOT_VERIFIED'
    }
    if ($DryRun) { $plan | ConvertTo-Json; exit 0 }

    # Protect only our own directory; do not alter the user's OpenClaw configuration ACL.
    New-Item -ItemType Directory -Path $StateDir -Force | Out-Null
    $acl = New-Object Security.AccessControl.DirectorySecurity
    $acl.SetOwner($identity.User)
    $acl.SetAccessRuleProtection($true, $false)
    foreach ($sidValue in @($currentSid, 'S-1-5-18')) {
        $sid = [Security.Principal.SecurityIdentifier]::new($sidValue)
        $rule = [Security.AccessControl.FileSystemAccessRule]::new($sid, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
        $acl.AddAccessRule($rule)
    }
    Set-Acl -LiteralPath $StateDir -AclObject $acl
    New-Item -ItemType Directory -Path $BinDir -Force | Out-Null
    if ($existing) { Stop-ScheduledTask -TaskName $TaskName -TaskPath '\' -ErrorAction SilentlyContinue }
    Copy-Item -LiteralPath $SourceCore -Destination $InstalledCore -Force
    $fixedArgs = @($InstalledCore, 'check', '--host-role', 'windows-primary', '--account', $Account,
        '--agent', $Agent, '--state-dir', $StateDir, '--openclaw-home', $OpenClawHome)
    if ($Notify) { $fixedArgs += '--notify-on-change' }
    $runner = @'
$ErrorActionPreference = 'Stop'
$env:PYTHONUTF8 = '1'
$env:PATH = __OPENCLAW_BIN__ + [IO.Path]::PathSeparator + $env:PATH
$fixedArgs = @(__FIXED_ARGS__)
$resultCode = 2
try { & __PYTHON__ @fixedArgs *> $null; if ($null -ne $LASTEXITCODE) { $resultCode = $LASTEXITCODE } } catch { $resultCode = 2 }
$status = @{ checked_at = (Get-Date).ToUniversalTime().ToString('o'); exit_code = $resultCode; model_test = 'UNTESTED' }
[IO.File]::WriteAllText(__RUNNER_STATUS__, ($status | ConvertTo-Json -Compress), (New-Object Text.UTF8Encoding($false)))
exit $resultCode
'@
    $runner = $runner.Replace('__OPENCLAW_BIN__', (Quote-Literal $OpenClawBin)).Replace('__PYTHON__', (Quote-Literal $PythonExe))
    $runner = $runner.Replace('__FIXED_ARGS__', (($fixedArgs | ForEach-Object { Quote-Literal $_ }) -join ', '))
    $runner = $runner.Replace('__RUNNER_STATUS__', (Quote-Literal (Join-Path $StateDir 'runner-status.json')))
    [IO.File]::WriteAllText($RunnerPath, $runner, (New-Object Text.UTF8Encoding($true)))
    $action = New-ScheduledTaskAction -Execute (Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe') `
        -Argument ('-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "{0}"' -f $RunnerPath) -WorkingDirectory $StateDir
    $periodic = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 5)
    $logon = New-ScheduledTaskTrigger -AtLogOn -User $currentSid
    $principal = New-ScheduledTaskPrincipal -UserId $currentSid -LogonType Interactive -RunLevel Limited
    $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 4) `
        -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
    Register-ScheduledTask -TaskName $TaskName -TaskPath '\' -Action $action -Trigger @($periodic, $logon) `
        -Principal $principal -Settings $settings -Description $TaskMarker -Force | Out-Null
    $plan['core_sha256'] = (Get-FileHash -LiteralPath $InstalledCore -Algorithm SHA256).Hash
    [IO.File]::WriteAllText((Join-Path $StateDir 'install-plan.json'), ($plan | ConvertTo-Json), (New-Object Text.UTF8Encoding($false)))
    Write-Host 'INSTALLED, NOT_RUNTIME_VERIFIED: first read-only check is scheduled within one minute.'
    Write-Host 'Requires this user to stay logged in; locking the screen is allowed. Logged-out operation is NOT provided.'
    Write-Host 'Gateway, OAuth, models, investment rules and existing tasks were not changed.'
    Write-Host 'Notifications use only an already paired recipient. No pairing means pending, not delivery to somebody else.'
    Write-Host 'No test message was sent. GPT model calls remain UNTESTED.'
} catch {
    Write-Error ('Monitor installation stopped: ' + $_.Exception.Message)
    exit 1
}
