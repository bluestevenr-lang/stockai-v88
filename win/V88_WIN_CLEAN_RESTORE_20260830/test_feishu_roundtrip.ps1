#requires -Version 5.1
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$OpenClawPath,
    [Parameter(Mandatory = $true)]
    [DateTimeOffset]$After,
    [string]$Agent = 'v88-gpt',
    [string]$Account = 'v88-gpt',
    [string]$Model = 'gpt-5.6-sol',
    [switch]$RequireHumanConfirmation,
    [ValidateRange(30,900)]
    [int]$TimeoutSeconds = 300
)

$ErrorActionPreference = 'Stop'

function Stop-Probe([string]$Message) { Write-Error $Message; exit 1 }

function Get-TextContent($Content) {
    $parts = New-Object System.Collections.Generic.List[string]
    foreach ($item in @($Content)) {
        if ($item -is [string]) { $parts.Add([string]$item); continue }
        if ($item -and $item.type -eq 'text' -and $item.text) { $parts.Add([string]$item.text) }
    }
    return ($parts -join "`n")
}

function Get-EventTime($Row) {
    foreach ($value in @($Row.timestamp, $Row.message.timestamp)) {
        if ($null -eq $value) { continue }
        try {
            if ($value -is [int] -or $value -is [long] -or $value -is [double] -or $value -is [decimal]) {
                $numeric = [double]$value
                if ($numeric -lt 100000000000) { $numeric *= 1000 }
                return [DateTimeOffset]::FromUnixTimeMilliseconds([long]$numeric)
            }
            return [DateTimeOffset]::Parse([string]$value)
        } catch { }
    }
    return $null
}

function Get-EventMilliseconds($Value) {
    if ($null -eq $Value) { return $null }
    if ($Value -is [int] -or $Value -is [long] -or $Value -is [double] -or $Value -is [decimal]) {
        $numeric = [double]$Value
        if ($numeric -lt 100000000000) { $numeric *= 1000 }
        return [long]$numeric
    }
    try { return [DateTimeOffset]::Parse([string]$Value).ToUnixTimeMilliseconds() }
    catch { return $null }
}

function Resolve-FeishuTranscripts([string]$SessionsDir) {
    $indexPath = Join-Path $SessionsDir 'sessions.json'
    if (-not (Test-Path -LiteralPath $indexPath -PathType Leaf)) { return @() }
    try { $index = Get-Content -LiteralPath $indexPath -Raw -Encoding UTF8 | ConvertFrom-Json }
    catch { return @() }
    $root = [IO.Path]::GetFullPath($SessionsDir).TrimEnd('\') + '\'
    $paths = New-Object System.Collections.Generic.List[string]
    foreach ($property in @($index.PSObject.Properties)) {
        $key = [string]$property.Name
        $entry = $property.Value
        $channels = @([string]$entry.channel, [string]$entry.lastChannel, [string]$entry.origin.provider,
            [string]$entry.deliveryContext.channel) | Where-Object { $_ }
        $accounts = @([string]$entry.lastAccountId, [string]$entry.origin.accountId,
            [string]$entry.deliveryContext.accountId) | Where-Object { $_ }
        $isFeishu = $channels -contains 'feishu' -or $key -match ('(?i)^agent:' + [regex]::Escape($Agent) + ':.*feishu:')
        $isFormalAccount = $accounts -contains $Account -or
            $key -match ('(?i):feishu:' + [regex]::Escape($Account) + ':')
        if (-not $isFeishu -or -not $isFormalAccount) { continue }
        $candidate = $null
        if ($entry.sessionFile) { $candidate = [string]$entry.sessionFile }
        elseif ($entry.sessionId) { $candidate = Join-Path $SessionsDir (([string]$entry.sessionId) + '.jsonl') }
        if (-not $candidate) { continue }
        if (-not [IO.Path]::IsPathRooted($candidate)) { $candidate = Join-Path $SessionsDir $candidate }
        try { $full = [IO.Path]::GetFullPath($candidate) } catch { continue }
        if (-not $full.StartsWith($root, [StringComparison]::OrdinalIgnoreCase) -or
            -not (Test-Path -LiteralPath $full -PathType Leaf)) { continue }
        $paths.Add($full)
    }
    return @($paths | Sort-Object -Unique)
}

if (-not (Test-Path -LiteralPath $OpenClawPath -PathType Leaf)) { Stop-Probe 'OpenClaw executable is missing.' }
$sessionsDir = Join-Path $env:USERPROFILE ('.openclaw\agents\{0}\sessions' -f $Agent)
$nonce = 'V88PHONE-' + ([guid]::NewGuid().ToString('N').Substring(0,12).ToUpperInvariant())
$started = [DateTimeOffset]::UtcNow
if ($started -lt $After) { $started = $After }
Write-Host ('PHONE_PROBE: send this exact text to the formal V88-GPT: {0}' -f $nonce)

$deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
while ([DateTimeOffset]::UtcNow -lt $deadline) {
    try {
        $channel = (& $OpenClawPath channels status --json) | ConvertFrom-Json
        $formal = @($channel.channelAccounts.feishu | Where-Object { $_.accountId -eq $Account })
        if ($formal.Count -ne 1 -or -not $formal[0].enabled -or -not $formal[0].running -or
            -not $formal[0].connected -or $formal[0].lastError) {
            Start-Sleep -Seconds 5
            continue
        }
        foreach ($path in @(Resolve-FeishuTranscripts $sessionsDir)) {
            $userSeen = $false
            $userAt = $null
            foreach ($line in Get-Content -LiteralPath $path -Encoding UTF8 -ErrorAction SilentlyContinue) {
                if ([string]::IsNullOrWhiteSpace($line)) { continue }
                try { $row = $line | ConvertFrom-Json } catch { continue }
                if ($row.type -ne 'message' -or -not $row.message) { continue }
                $eventAt = Get-EventTime $row
                if (-not $eventAt -or $eventAt -le $started) { continue }
                if (-not $userSeen -and $row.message.role -eq 'user' -and
                    (Get-TextContent $row.message.content) -match [regex]::Escape($nonce)) {
                    $userSeen = $true
                    $userAt = $eventAt
                    continue
                }
                if ($userSeen -and $row.message.role -eq 'assistant' -and $eventAt -gt $userAt -and
                    [string]$row.message.provider -eq 'openai' -and [string]$row.message.model -eq $Model -and
                    [string]$row.message.stopReason -notin @('error','aborted') -and
                    -not [string]::IsNullOrWhiteSpace((Get-TextContent $row.message.content)) -and
                    (Get-EventMilliseconds $formal[0].lastOutboundAt) -ge $eventAt.ToUnixTimeMilliseconds()) {
                    if ($RequireHumanConfirmation) {
                        $seen = (Read-Host 'Confirm the phone displayed the reply: type YES').Trim()
                        if ($seen -ne 'YES') { Stop-Probe 'Phone delivery was not confirmed by the user.' }
                    }
                    Write-Host 'PHONE_ROUNDTRIP_OK: the formal Feishu session received the nonce, GPT replied, and delivery was observed.'
                    exit 0
                }
            }
        }
    } catch { }
    Start-Sleep -Seconds 5
}

Stop-Probe 'No matching user nonce plus later GPT reply was found in the same formal Feishu session.'
