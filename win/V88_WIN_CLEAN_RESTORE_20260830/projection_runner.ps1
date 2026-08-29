#requires -Version 5.1
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Source,
    [Parameter(Mandatory = $true)][string]$Dest
)

$ErrorActionPreference = 'Stop'
$PackageRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$WinRoot = Split-Path -Parent $PackageRoot
$Script = Join-Path $WinRoot 'openclaw-v88\sync_v88_projection_win.py'

foreach ($name in @('OPENAI_API_KEY','ANTHROPIC_API_KEY','GOOGLE_API_KEY','GEMINI_API_KEY','MOONSHOT_API_KEY',
    'KIMI_API_KEY','KIMI_CODE_API_KEY','DEEPSEEK_API_KEY','OPENROUTER_API_KEY','CODEX_API_KEY',
    'OPENAI_BASE_URL','ANTHROPIC_BASE_URL','MOONSHOT_BASE_URL','KIMI_BASE_URL','DEEPSEEK_BASE_URL',
    'OPENROUTER_BASE_URL','OPENAI_EXTRA_USAGE','CODEX_EXTRA_USAGE')) {
    Remove-Item ("Env:{0}" -f $name) -ErrorAction SilentlyContinue
}

$python = $null; $prefix = @()
foreach ($name in @('py.exe','python.exe','python3.exe')) {
    $candidate = Get-Command $name -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $candidate -or $candidate.Source -match '\\WindowsApps\\') { continue }
    $candidatePrefix = @(); if ($name -eq 'py.exe') { $candidatePrefix = @('-3') }
    & $candidate.Source @candidatePrefix -c 'import sys; raise SystemExit(0 if sys.version_info >= (3,10) else 2)'
    if ($LASTEXITCODE -eq 0) { $python = $candidate.Source; $prefix = $candidatePrefix; break }
}
if (-not $python) { throw 'Python 3.10+ is unavailable.' }
& $python @prefix $Script --source ([IO.Path]::GetFullPath($Source)) --dest ([IO.Path]::GetFullPath($Dest))
if ($LASTEXITCODE -ne 0) { throw 'V88 privacy projection failed.' }
