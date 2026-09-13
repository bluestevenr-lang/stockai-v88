# V88 Windows initialization; existing installs use the checked updater.
$ErrorActionPreference = "Stop"
$Desktop = Join-Path $env:USERPROFILE 'Desktop'
foreach ($cmd in @('git','py')) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) { throw "Missing $cmd. Install Git for Windows and Python 3.12+." }
}
foreach ($entry in @(@('StockAI','stockai-v88'),@('ai-daily-report-v2','v88-daily-report'))) {
    $path = Join-Path $Desktop $entry[0]
    if (-not (Test-Path -LiteralPath $path)) {
        & git clone ("https://github.com/bluestevenr-lang/"+$entry[1]+'.git') $path
        if ($LASTEXITCODE -ne 0) { throw "Clone failed: $path" }
    }
}
$root = Join-Path $Desktop 'StockAI'
& py -3 (Join-Path $root 'win\update_v88.py')
if ($LASTEXITCODE -ne 0) { throw 'V88 update failed. See the error above.' }
$shell = New-Object -ComObject WScript.Shell
foreach ($entry in @(@('V88','启动V88.bat'),@('同步V88','同步V88.bat'))) {
    $link=$shell.CreateShortcut((Join-Path $Desktop ($entry[0]+'.lnk')))
    $link.TargetPath=Join-Path $root ('win\'+$entry[1])
    $link.WorkingDirectory=$root
    $link.Save()
}
Write-Host 'V88 installation verified. Use the desktop V88 shortcut.'
