#requires -Version 5.1
[CmdletBinding()]
param(
    [ValidateSet('Host','Runtime')]
    [string]$Phase = 'Host',
    [string]$OutputPath
)

$ErrorActionPreference = 'Stop'
$results = New-Object System.Collections.Generic.List[object]
$blocked = 0
$manual = 0

function Add-Result([string]$Name, [string]$Status, [string]$Code, $Data = $null) {
    if ($Status -eq 'BLOCKED') { $script:blocked++ }
    if ($Status -eq 'MANUAL') { $script:manual++ }
    $row = [ordered]@{ name = $Name; status = $Status; code = $Code }
    if ($null -ne $Data) { $row['data'] = $Data }
    $script:results.Add([pscustomobject]$row)
}

function Get-PowerAcSeconds([string]$SettingAlias) {
    try {
        $raw = (& powercfg.exe /query SCHEME_CURRENT SUB_SLEEP $SettingAlias 2>$null | Out-String)
        $matches = @([regex]::Matches($raw, '0x[0-9A-Fa-f]{8}') | ForEach-Object { $_.Value })
        if ($matches.Count -lt 2) { return $null }
        return [Convert]::ToUInt32($matches[$matches.Count - 2].Substring(2), 16)
    } catch { return $null }
}

function Test-TlsEndpoint([string]$HostName) {
    $client = New-Object Net.Sockets.TcpClient
    $ssl = $null
    try {
        $connect = $client.ConnectAsync($HostName, 443)
        if (-not $connect.Wait(7000) -or -not $client.Connected) { return $false }
        $ssl = New-Object Net.Security.SslStream($client.GetStream(), $false)
        $ssl.ReadTimeout = 7000
        $ssl.WriteTimeout = 7000
        $auth = $ssl.AuthenticateAsClientAsync($HostName)
        if (-not $auth.Wait(7000)) { return $false }
        return $ssl.IsAuthenticated -and $ssl.IsEncrypted
    } catch { return $false }
    finally {
        if ($ssl) { $ssl.Dispose() }
        $client.Dispose()
    }
}

function Get-CommandPaths([string[]]$Names) {
    $paths = @()
    foreach ($name in $Names) {
        $paths += @(Get-Command $name -CommandType Application -All -ErrorAction SilentlyContinue |
            ForEach-Object { $_.Source } | Where-Object { $_ -and $_ -notmatch '\\WindowsApps\\' })
    }
    return @($paths | ForEach-Object { [IO.Path]::GetFullPath($_) } | Sort-Object -Unique)
}

if ($env:OS -ne 'Windows_NT') { throw 'This preflight must run on Windows.' }
if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path $env:LOCALAPPDATA ('V88CleanRestore\preflight-{0}.json' -f $Phase.ToLowerInvariant())
}

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
$profilePath = [IO.Path]::GetFullPath($env:USERPROFILE).TrimEnd('\')
$isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
$temporaryProfile = $profilePath -match '(?i)\\TEMP(?:\.|\\|$)'
if ($temporaryProfile) { Add-Result 'windows_profile' 'BLOCKED' 'TEMPORARY_PROFILE' }
else { Add-Result 'windows_profile' 'PASS' 'STABLE_PROFILE' @{ elevated = $isAdmin } }
if ($isAdmin) { Add-Result 'deployment_admin' 'PASS' 'ELEVATED_PREPARE' }
else { Add-Result 'deployment_admin' 'BLOCKED' 'RUN_PREPARE_AS_ADMINISTRATOR' }
try {
    $interactiveName = [string](Get-CimInstance Win32_ComputerSystem).UserName
    $interactiveSid = if ($interactiveName) {
        ([Security.Principal.NTAccount]::new($interactiveName)).Translate([Security.Principal.SecurityIdentifier]).Value
    } else { $null }
    if (-not $interactiveSid -or $interactiveSid -ne $identity.User.Value) {
        Add-Result 'deployment_identity' 'BLOCKED' 'ELEVATION_CHANGED_WINDOWS_USER'
    } else { Add-Result 'deployment_identity' 'PASS' 'SAME_INTERACTIVE_ADMIN_USER' }
} catch { Add-Result 'deployment_identity' 'BLOCKED' 'INTERACTIVE_USER_NOT_PROVEN' }

try {
    $os = Get-CimInstance Win32_OperatingSystem
    $build = [int]$os.BuildNumber
    if ($os.Caption -notmatch 'Windows 11' -or $build -lt 22000) {
        Add-Result 'windows_version' 'BLOCKED' 'WINDOWS_11_REQUIRED' @{ build = $build }
    } else { Add-Result 'windows_version' 'PASS' 'WINDOWS_11' @{ build = $build } }
} catch { Add-Result 'windows_version' 'MANUAL' 'OS_QUERY_FAILED' }

try {
    $driveLetter = $env:SystemDrive.TrimEnd(':')
    $partition = Get-Partition -DriveLetter $driveLetter
    $disk = Get-Disk -Number $partition.DiskNumber
    $physical = @(Get-PhysicalDisk | Where-Object { [string]$_.DeviceId -eq [string]$disk.Number })
    $systemVolume = Get-CimInstance Win32_Volume -Filter ("DriveLetter='{0}'" -f $env:SystemDrive)
    $diskData = [ordered]@{
        bus_type = [string]$disk.BusType
        partition_style = [string]$disk.PartitionStyle
        size_gb = [math]::Round($disk.Size / 1GB, 1)
        free_gb = [math]::Round((Get-PSDrive -Name $driveLetter).Free / 1GB, 1)
        volume_dirty = if ($null -eq $systemVolume.DirtyBitSet) { $null } else { [bool]$systemVolume.DirtyBitSet }
    }
    if ($disk.IsOffline -or $disk.IsReadOnly -or $disk.OperationalStatus -notcontains 'Online' -or $diskData.volume_dirty -eq $true) {
        Add-Result 'system_disk' 'BLOCKED' 'SYSTEM_DISK_NOT_CLEAN' $diskData
    } elseif ($diskData.free_gb -lt 50) {
        Add-Result 'system_disk' 'BLOCKED' 'SYSTEM_DISK_LOW_SPACE' $diskData
    } else { Add-Result 'system_disk' 'PASS' 'SYSTEM_DISK_BASELINE' $diskData }

    if ($physical.Count -ne 1) {
        Add-Result 'ssd_health' 'MANUAL' 'PHYSICAL_DISK_MAPPING_UNKNOWN'
    } else {
        $pd = $physical[0]
        $reliability = $null
        try { $reliability = Get-StorageReliabilityCounter -PhysicalDisk $pd -ErrorAction Stop }
        catch { }
        $health = [ordered]@{
            health = [string]$pd.HealthStatus
            operational = @($pd.OperationalStatus | ForEach-Object { [string]$_ })
            media = [string]$pd.MediaType
            temperature_c = if ($reliability) { $reliability.Temperature } else { $null }
            wear = if ($reliability) { $reliability.Wear } else { $null }
            read_errors_total = if ($reliability) { $reliability.ReadErrorsTotal } else { $null }
            read_errors_uncorrected = if ($reliability) { $reliability.ReadErrorsUncorrected } else { $null }
            write_errors_total = if ($reliability) { $reliability.WriteErrorsTotal } else { $null }
            write_errors_uncorrected = if ($reliability) { $reliability.WriteErrorsUncorrected } else { $null }
        }
        $uncorrectedValues = @($health.read_errors_uncorrected, $health.write_errors_uncorrected) |
            Where-Object { $null -ne $_ }
        $uncorrected = ($uncorrectedValues | Measure-Object -Sum).Sum
        if ($pd.HealthStatus -ne 'Healthy' -or $uncorrected -gt 0) {
            Add-Result 'ssd_health' 'BLOCKED' 'SSD_HEALTH_OR_UNCORRECTED_ERROR' $health
        } elseif (-not $reliability) {
            Add-Result 'ssd_health' 'MANUAL' 'SMART_COUNTERS_UNAVAILABLE' $health
        } elseif (($health.read_errors_total -as [long]) -gt 0 -or ($health.write_errors_total -as [long]) -gt 0) {
            Add-Result 'ssd_health' 'MANUAL' 'SSD_REPORTED_CORRECTED_ERRORS' $health
        } else { Add-Result 'ssd_health' 'PASS' 'SSD_COUNTERS_CLEAN' $health }
    }
} catch { Add-Result 'system_disk' 'MANUAL' 'SYSTEM_DISK_QUERY_FAILED' }

try {
    $installDate = (Get-CimInstance Win32_OperatingSystem).InstallDate
    $ids = @(7,11,17,18,51,55,129,153,157)
    $events = @(Get-WinEvent -FilterHashtable @{ LogName = 'System'; StartTime = $installDate; Id = $ids } -ErrorAction SilentlyContinue |
        Where-Object { $_.ProviderName -match '(?i)Disk|stornvme|Ntfs|volmgr|WHEA' })
    $fatalHardware = @($events | Where-Object { $_.ProviderName -match '(?i)WHEA' -and $_.Id -eq 18 }).Count
    $correctedPcie = @($events | Where-Object { $_.ProviderName -match '(?i)WHEA' -and $_.Id -eq 17 }).Count
    $uncorrelatedStorage = @($events | Where-Object { $_.ProviderName -notmatch '(?i)WHEA' -and $_.Id -in @(7,11,51,55,129,153,157) }).Count
    $eventData = @{ fatal_hardware_count = $fatalHardware; corrected_pcie_count = $correctedPcie
        uncorrelated_storage_count = $uncorrelatedStorage; since_install = $true }
    if ($fatalHardware -gt 0) { Add-Result 'storage_events' 'BLOCKED' 'FATAL_HARDWARE_EVENT' $eventData }
    elseif ($correctedPcie -gt 0 -or $uncorrelatedStorage -gt 0) {
        Add-Result 'storage_events' 'MANUAL' 'UNSCOPED_STORAGE_OR_PCIE_EVENT_REVIEW' $eventData
    }
    else { Add-Result 'storage_events' 'PASS' 'NO_STORAGE_ERRORS' $eventData }
} catch { Add-Result 'storage_events' 'MANUAL' 'STORAGE_EVENT_QUERY_FAILED' }

$pending = @(
    'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Component Based Servicing\RebootPending',
    'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\WindowsUpdate\Auto Update\RebootRequired'
) | Where-Object { Test-Path -LiteralPath $_ }
try {
    $renameOps = (Get-ItemProperty 'HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager' -Name PendingFileRenameOperations -ErrorAction SilentlyContinue).PendingFileRenameOperations
    if ($renameOps) { $pending += 'pending_file_rename' }
} catch { }
if (@($pending | Where-Object { $_ -ne 'pending_file_rename' }).Count -gt 0) {
    Add-Result 'pending_reboot' 'BLOCKED' 'REBOOT_REQUIRED' @{ signals = $pending.Count }
} elseif ($pending -contains 'pending_file_rename') {
    Add-Result 'pending_reboot' 'MANUAL' 'PENDING_FILE_RENAME_REVIEW' @{ signals = 1 }
}
else { Add-Result 'pending_reboot' 'PASS' 'NO_REBOOT_PENDING' }

try {
    $scheduler = Get-CimInstance Win32_Service -Filter "Name='Schedule'"
    if ($scheduler.State -ne 'Running' -or $scheduler.StartMode -ne 'Auto') {
        Add-Result 'task_scheduler' 'BLOCKED' 'TASK_SCHEDULER_NOT_READY'
    } else { Add-Result 'task_scheduler' 'PASS' 'TASK_SCHEDULER_READY' }
} catch { Add-Result 'task_scheduler' 'MANUAL' 'TASK_SCHEDULER_QUERY_FAILED' }

$sleepAc = Get-PowerAcSeconds 'STANDBYIDLE'
$hibernateAc = Get-PowerAcSeconds 'HIBERNATEIDLE'
$powerData = @{ sleep_ac_seconds = $sleepAc; hibernate_ac_seconds = $hibernateAc }
if ($null -eq $sleepAc -or $null -eq $hibernateAc) { Add-Result 'power_24x7' 'MANUAL' 'POWER_SETTINGS_UNKNOWN' $powerData }
elseif ($sleepAc -ne 0 -or $hibernateAc -ne 0) { Add-Result 'power_24x7' 'BLOCKED' 'SLEEP_OR_HIBERNATE_ENABLED' $powerData }
else { Add-Result 'power_24x7' 'PASS' 'AC_SLEEP_HIBERNATE_NEVER' $powerData }

try {
    $timeService = Get-Service W32Time -ErrorAction Stop
    $request = [Net.HttpWebRequest]::Create('https://www.microsoft.com/')
    $request.Method = 'HEAD'; $request.Timeout = 10000; $request.ReadWriteTimeout = 10000
    $response = $request.GetResponse()
    try { $serverDate = [DateTimeOffset]::Parse([string]$response.Headers['Date']) }
    finally { $response.Close() }
    $offsetSeconds = [math]::Abs(([DateTimeOffset]::UtcNow - $serverDate).TotalSeconds)
    if ($timeService.Status -ne 'Running' -or $offsetSeconds -gt 300) {
        Add-Result 'time_sync' 'MANUAL' 'TIME_SYNC_NOT_PROVEN'
    } else { Add-Result 'time_sync' 'PASS' 'TIME_SYNC_ACTIVE' @{ max_observed_offset_seconds = [math]::Round($offsetSeconds,1) } }
} catch { Add-Result 'time_sync' 'MANUAL' 'TIME_SYNC_QUERY_FAILED' }

$tlsResults = [ordered]@{}
foreach ($hostName in @('auth.openai.com','chatgpt.com','open.feishu.cn','github.com')) {
    $tlsResults[$hostName] = Test-TlsEndpoint $hostName
}
if (@($tlsResults.Values | Where-Object { -not $_ }).Count -gt 0) { Add-Result 'outbound_tls' 'BLOCKED' 'REQUIRED_TLS_ENDPOINT_FAILED' $tlsResults }
else { Add-Result 'outbound_tls' 'PASS' 'REQUIRED_TLS_ENDPOINTS_OK' $tlsResults }

$blockedEnvironment = @('OPENAI_API_KEY','ANTHROPIC_API_KEY','GOOGLE_API_KEY','GEMINI_API_KEY',
    'MOONSHOT_API_KEY','KIMI_API_KEY','KIMI_CODE_API_KEY','DEEPSEEK_API_KEY','OPENROUTER_API_KEY',
    'CODEX_API_KEY','OPENAI_BASE_URL','ANTHROPIC_BASE_URL','MOONSHOT_BASE_URL','KIMI_BASE_URL',
    'DEEPSEEK_BASE_URL','OPENROUTER_BASE_URL','OPENAI_EXTRA_USAGE','CODEX_EXTRA_USAGE',
    'OPENCLAW_HOME','OPENCLAW_STATE_DIR','OPENCLAW_CONFIG_PATH','OPENCLAW_WINDOWS_TASK_NAME')
$presentEnvironment = New-Object System.Collections.Generic.List[string]
foreach ($name in $blockedEnvironment) {
    foreach ($scope in @('Process','User','Machine')) {
        if ([Environment]::GetEnvironmentVariable($name, $scope)) { $presentEnvironment.Add($name); break }
    }
}
$presentEnvironment = @($presentEnvironment | Sort-Object -Unique)
if ($presentEnvironment.Count -gt 0) { Add-Result 'environment' 'BLOCKED' 'PAID_OR_STATE_OVERRIDE_PRESENT' @{ names = $presentEnvironment } }
else { Add-Result 'environment' 'PASS' 'NO_PAID_OR_STATE_OVERRIDE' }

$proxyNames = New-Object System.Collections.Generic.List[string]
foreach ($name in @('HTTP_PROXY','HTTPS_PROXY','ALL_PROXY')) {
    foreach ($scope in @('Process','User','Machine')) {
        if ([Environment]::GetEnvironmentVariable($name, $scope)) { $proxyNames.Add($name); break }
    }
}
try {
    $internet = Get-ItemProperty 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Internet Settings' -ErrorAction SilentlyContinue
    if ($internet.ProxyEnable -eq 1) { $proxyNames.Add('WININET_PROXY') }
} catch { }
try {
    $winHttp = (& netsh.exe winhttp show proxy 2>$null | Out-String)
    if ($LASTEXITCODE -eq 0 -and $winHttp -notmatch '(?i)direct access|直接访问') { $proxyNames.Add('WINHTTP_PROXY') }
} catch { }
$proxyNames = @($proxyNames | Sort-Object -Unique)
if ($proxyNames.Count -gt 0) { Add-Result 'network_proxy' 'PASS' 'PROXY_PRESENT_TLS_VALIDATED' @{ names = $proxyNames } }
else { Add-Result 'network_proxy' 'PASS' 'NO_PROXY_DETECTED' }

try {
    $listeners = @(Get-NetTCPConnection -LocalPort 18789 -State Listen -ErrorAction SilentlyContinue)
    if ($Phase -eq 'Host' -and $listeners.Count -gt 0) { Add-Result 'gateway_port' 'BLOCKED' 'PORT_18789_ALREADY_IN_USE' @{ listener_count = $listeners.Count } }
    elseif ($Phase -eq 'Host') { Add-Result 'gateway_port' 'PASS' 'PORT_18789_FREE' }
    elseif ($listeners.Count -ne 1 -or $listeners[0].LocalAddress -notin @('127.0.0.1','::1')) {
        Add-Result 'gateway_port' 'BLOCKED' 'RUNTIME_GATEWAY_LISTENER_INVALID' @{ listener_count = $listeners.Count }
    } else { Add-Result 'gateway_port' 'PASS' 'RUNTIME_GATEWAY_LOOPBACK_ONLY' }
} catch { Add-Result 'gateway_port' 'MANUAL' 'PORT_QUERY_FAILED' }

if ($Phase -eq 'Runtime') {
    foreach ($spec in @(
        @{ name='git'; commands=@('git.exe','git') },
        @{ name='node'; commands=@('node.exe','node') },
        @{ name='openclaw'; commands=@('openclaw.cmd','openclaw.exe','openclaw') }
    )) {
        $paths = @(Get-CommandPaths $spec.commands)
        if ($paths.Count -eq 0) { Add-Result ('runtime_' + $spec.name) 'BLOCKED' 'RUNTIME_MISSING' }
        elseif ($paths.Count -gt 1) { Add-Result ('runtime_' + $spec.name) 'BLOCKED' 'RUNTIME_PATH_AMBIGUOUS' @{ path_count = $paths.Count } }
        else { Add-Result ('runtime_' + $spec.name) 'PASS' 'RUNTIME_PATH_UNIQUE' }
    }
    $pythonReady = $false
    foreach ($name in @('py.exe','python.exe','python3.exe')) {
        $command = Get-Command $name -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $command -or $command.Source -match '\\WindowsApps\\') { continue }
        $prefix = @(); if ($name -eq 'py.exe') { $prefix = @('-3') }
        & $command.Source @prefix -c 'import sys; raise SystemExit(0 if sys.version_info >= (3,10) else 2)' 2>$null
        if ($LASTEXITCODE -eq 0) { $pythonReady = $true; break }
    }
    if ($pythonReady) { Add-Result 'runtime_python' 'PASS' 'PYTHON_310_OR_NEWER' }
    else { Add-Result 'runtime_python' 'BLOCKED' 'PYTHON_310_REQUIRED' }
    try {
        $nodeText = (& node.exe --version 2>$null | Out-String).Trim().TrimStart('v')
        $nodeVersion = [version]$nodeText
        $supported = (($nodeVersion.Major -eq 22 -and $nodeVersion -ge [version]'22.22.3') -or
            ($nodeVersion.Major -eq 24 -and $nodeVersion -ge [version]'24.15.0') -or
            ($nodeVersion.Major -eq 25 -and $nodeVersion -ge [version]'25.9.0'))
        if (-not $supported) { Add-Result 'node_version' 'BLOCKED' 'NODE_VERSION_UNSUPPORTED' @{ version = $nodeText } }
        else { Add-Result 'node_version' 'PASS' 'NODE_VERSION_SUPPORTED' @{ version = $nodeText } }
    } catch { Add-Result 'node_version' 'BLOCKED' 'NODE_VERSION_UNREADABLE' }
}

if ($Phase -eq 'Host') {
    $autologon = $false
    try { $autologon = (Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon' -Name AutoAdminLogon -ErrorAction SilentlyContinue).AutoAdminLogon -eq '1' }
    catch { }
    if ($autologon) { Add-Result 'unattended_login' 'MANUAL' 'AUTOLOGON_PRESENT_REVIEW_SECURITY' }
    else { Add-Result 'unattended_login' 'PASS' 'LOGIN_REQUIRED_AFTER_REBOOT' }
}

$verdict = if ($blocked -gt 0) { 'BLOCKED' } elseif ($manual -gt 0) { 'MANUAL_REQUIRED' } else { 'PASS' }
$report = [ordered]@{
    schema = 'v88.windows.preflight.v1'
    checked_at = (Get-Date).ToUniversalTime().ToString('o')
    phase = $Phase
    verdict = $verdict
    blocked_count = $blocked
    manual_count = $manual
    results = $results
    privacy = 'No serial number, account name, secret, asset, holding or raw log is included.'
}
$directory = Split-Path -Parent $OutputPath
New-Item -ItemType Directory -Path $directory -Force | Out-Null
[IO.File]::WriteAllText($OutputPath, ($report | ConvertTo-Json -Depth 12), (New-Object Text.UTF8Encoding($false)))
Write-Host ("PREFLIGHT_{0}: blocked={1}, manual={2}, report={3}" -f $verdict,$blocked,$manual,$OutputPath)
if ($blocked -gt 0) { exit 2 }
if ($manual -gt 0) { exit 3 }
exit 0
