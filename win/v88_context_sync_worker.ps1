# Current-session worker for the V88 GitHub, portfolio, and memory projection.
# The scheduled V88 keepalive takes over after the next Windows restart.
$ErrorActionPreference = 'Continue'
$Ctl = 'C:\Users\admin\Desktop\StockAI\win\v88ctl.ps1'
$Log = 'C:\Users\admin\.openclaw\tools\v88-context-sync.log'
$Created = $false
$Mutex = [Threading.Mutex]::new($true, 'Local\V88ContextSyncWorker', [ref]$Created)
if (-not $Created) { exit 0 }

try {
    while ($true) {
        try {
            $Result = & $Ctl sync 2>&1
            Add-Content -LiteralPath $Log -Encoding UTF8 -Value (
                "{0} OK {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), (($Result | Select-Object -Last 3) -join ' | ')
            )
        } catch {
            Add-Content -LiteralPath $Log -Encoding UTF8 -Value (
                "{0} FAIL {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $_.Exception.Message
            )
        }
        Start-Sleep -Seconds 600
    }
} finally {
    $Mutex.ReleaseMutex()
    $Mutex.Dispose()
}
