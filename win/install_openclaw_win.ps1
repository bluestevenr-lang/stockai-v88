
# ============================================================
# V88 OpenClaw Windows 主机安装器（GPT 方案 B）
#
# 做什么：
#   1. 安装受支持的 Node.js 与 OpenClaw
#   2. 安装 Codex/OpenAI 与飞书插件（不写任何密钥）
#   3. 建立只读、脱敏的 v88-mobile 工作区
#   4. 注册开机自启、失败重试的 OpenClaw Gateway
#   5. 可选地立即启动 GPT OAuth
#
# 不做什么：
#   - 不安装或默认启用 Kimi API。K3 仍由 V88 定时评审写入快照。
#   - 不启动第二套 V88 行情流水线，不重复推送飞书。
#   - 不把 GPT OAuth、飞书密钥或资产明文写进仓库/日志。
# ============================================================
$ErrorActionPreference = 'Stop'

function Test-Administrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-Administrator)) {
    $args = "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`""
    Start-Process -FilePath powershell.exe -Verb RunAs -ArgumentList $args
    exit 0
}

$StockAI = Split-Path -Parent $PSScriptRoot
$Desktop = Split-Path -Parent $StockAI
$Report = Join-Path $Desktop 'ai-daily-report-v2'
$Package = Join-Path $PSScriptRoot 'openclaw-v88'
$LogDir = Join-Path $PSScriptRoot 'logs'
New-Item -ItemType Directory -Force $LogDir | Out-Null
$LogFile = Join-Path $LogDir ("openclaw_install_{0}.log" -f (Get-Date -Format yyyyMMdd_HHmmss))

function Log([string]$Message) {
    $Message | Tee-Object -FilePath $LogFile -Append
}

function Refresh-Path {
    $machine = [Environment]::GetEnvironmentVariable('Path', 'Machine')
    $user = [Environment]::GetEnvironmentVariable('Path', 'User')
    $env:Path = "$machine;$user;$env:APPDATA\npm"
}

function Run-Native([string]$Label, [string]$File, [string[]]$Arguments) {
    Log $Label
    & $File @Arguments 2>&1 | ForEach-Object { Log "$_" }
    if ($LASTEXITCODE -ne 0) {
        throw "$Label 失败，退出码 $LASTEXITCODE"
    }
}

function Test-SupportedNode {
    $node = Get-Command node -ErrorAction SilentlyContinue
    if (-not $node) { return $false }
    $raw = (& node --version 2>$null).Trim().TrimStart('v')
    $parts = $raw.Split('.')
    if ($parts.Count -lt 3) { return $false }
    $major = [int]$parts[0]
    $minor = [int]$parts[1]
    $patch = [int]$parts[2]
    $node22 = ($major -eq 22 -and ($minor -gt 22 -or ($minor -eq 22 -and $patch -ge 3)))
    $node24 = ($major -eq 24 -and $minor -ge 15)
    $node25 = ($major -eq 25 -and $minor -ge 9)
    return ($node22 -or $node24 -or $node25)
}

function Resolve-Python {
    if (Get-Command python -ErrorAction SilentlyContinue) {
        return @{ File = 'python'; Prefix = @() }
    }
    if (Get-Command py -ErrorAction SilentlyContinue) {
        return @{ File = 'py'; Prefix = @('-3') }
    }
    throw '没有找到 Python。请先运行 win\初始化V88.ps1。'
}

try {
    Log "== OpenClaw Win 安装开始 $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') =="
    Log "StockAI: $StockAI"
    Log "V88 私仓: $Report"

    Refresh-Path
    if (-not (Test-SupportedNode)) {
        if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
            throw 'Node 版本不受支持，且系统没有 winget。请安装当前 Node.js LTS 后重跑。'
        }
        Log '[1/7] 安装或升级 Node.js LTS ...'
        & winget upgrade --id OpenJS.NodeJS.LTS -e --silent --accept-source-agreements --accept-package-agreements 2>&1 |
            ForEach-Object { Log "$_" }
        if ($LASTEXITCODE -ne 0) {
            & winget install --id OpenJS.NodeJS.LTS -e --silent --accept-source-agreements --accept-package-agreements 2>&1 |
                ForEach-Object { Log "$_" }
        }
        Refresh-Path
    }
    if (-not (Test-SupportedNode)) {
        throw 'Node 仍不满足 OpenClaw 要求（22.22.3+、24.15+ 或 25.9+）。请更新 Node 后重跑。'
    }
    Log "[1/7] Node OK: $(& node --version)"

    Run-Native '[2/7] 安装/更新 OpenClaw ...' 'npm' @('install', '-g', 'openclaw@latest')
    Refresh-Path
    if (-not (Get-Command openclaw -ErrorAction SilentlyContinue)) {
        throw 'OpenClaw 安装完成后仍不在 PATH。'
    }
    Log "[2/7] OpenClaw OK: $(& openclaw --version)"

    $pluginJson = (& openclaw plugins list --json 2>$null) -join "`n"
    $pluginDoc = $null
    try { $pluginDoc = $pluginJson | ConvertFrom-Json } catch {}
    $pluginRows = @()
    if ($pluginDoc -and $pluginDoc.plugins) { $pluginRows = @($pluginDoc.plugins) }
    foreach ($plugin in @(
        @{ Id = 'codex'; Package = '@openclaw/codex' },
        @{ Id = 'feishu'; Package = '@openclaw/feishu' }
    )) {
        $found = $pluginRows | Where-Object { $_.id -eq $plugin.Id -and $_.status -eq 'loaded' }
        if (-not $found) {
            Run-Native ("[3/7] 安装插件 {0} ..." -f $plugin.Id) 'openclaw' @('plugins', 'install', $plugin.Package)
        } else {
            Log ("[3/7] 插件已就绪: {0}" -f $plugin.Id)
        }
        Run-Native ("[3/7] 启用插件 {0} ..." -f $plugin.Id) 'openclaw' @('plugins', 'enable', $plugin.Id)
    }
    Run-Native '[3/7] 刷新插件注册表与修复配置 ...' 'openclaw' @('doctor', '--fix')

    if (-not (Test-Path $Report)) {
        throw "找不到 V88 私仓 $Report。请先运行 win\初始化V88.ps1。"
    }
    if (-not (Test-Path (Join-Path $Package 'sync_v88_projection_win.py'))) {
        throw "安装包缺少 win\openclaw-v88。请先让 Win 同步 StockAI 仓库。"
    }
    $Workspace = Join-Path $env:USERPROFILE '.openclaw\workspaces\v88-mobile'
    $Context = Join-Path $Workspace 'context'
    New-Item -ItemType Directory -Force $Workspace | Out-Null
    Copy-Item -Force (Join-Path $Package 'AGENTS.md') (Join-Path $Workspace 'AGENTS.md')
    $python = Resolve-Python
    $syncArgs = @($python.Prefix) + @(
        (Join-Path $Package 'sync_v88_projection_win.py'),
        '--source', (Join-Path $Report 'data'),
        '--dest', $Context
    )
    Run-Native '[4/7] 生成脱敏只读 V88 工作区 ...' $python.File $syncArgs

    Run-Native '[5/7] 设置本地网关模式 ...' 'openclaw' @('config', 'set', 'gateway.mode', 'local')
    $agentsJson = (& openclaw agents list --json 2>$null) -join "`n"
    $agents = @($agentsJson | ConvertFrom-Json)
    if (-not ($agents | Where-Object { $_.id -eq 'v88-mobile' })) {
        Run-Native '[5/7] 创建 v88-mobile 只读代理 ...' 'openclaw' @(
            'agents', 'add', 'v88-mobile', '--non-interactive',
            '--workspace', $Workspace, '--model', 'openai/gpt-5.6-sol'
        )
    } else {
        Log '[5/7] v88-mobile 代理已存在，更新安全配置。'
    }

    $ConfigPath = (& openclaw config file).Trim()
    $ConfigDoc = Get-Content -Raw -Encoding UTF8 $ConfigPath | ConvertFrom-Json
    $AgentList = @($ConfigDoc.agents.list)
    $AgentIndex = -1
    for ($i = 0; $i -lt $AgentList.Count; $i++) {
        if ($AgentList[$i].id -eq 'v88-mobile') { $AgentIndex = $i; break }
    }
    if ($AgentIndex -lt 0) { throw '找不到刚创建的 v88-mobile 代理。' }

    $ToolPolicy = @{
        profile = 'coding'
        allow = @('read')
        deny = @(
            'write', 'edit', 'apply_patch', 'exec', 'process', 'browser',
            'web_search', 'web_fetch', 'message', 'sessions_spawn',
            'sessions_send', 'cron', 'gateway', 'nodes', 'computer'
        )
        codeMode = $false
        elevated = @{ enabled = $false }
        exec = @{ mode = 'deny' }
        fs = @{ workspaceOnly = $true }
        message = @{
            allowCrossContextSend = $false
            crossContext = @{ allowWithinProvider = $false; allowAcrossProviders = $false }
            broadcast = @{ enabled = $false }
        }
    }
    $CodexPolicy = @{
        computerUse = @{ enabled = $false; autoInstall = $false }
        codexPlugins = @{
            enabled = $false
            allow_all_plugins = $false
            allow_destructive_actions = $false
        }
        appServer = @{
            mode = 'guardian'
            homeScope = 'agent'
            codeModeOnly = $false
            approvalPolicy = 'never'
            sandbox = 'read-only'
            approvalsReviewer = 'user'
        }
    }
    $Batch = @(
        @{ path = "agents.list[$AgentIndex].workspace"; value = $Workspace },
        @{ path = "agents.list[$AgentIndex].model"; value = 'openai/gpt-5.6-sol' },
        @{ path = "agents.list[$AgentIndex].tools"; value = $ToolPolicy },
        @{ path = "agents.list[$AgentIndex].subagents"; value = @{ allowAgents = @(); requireAgentId = $true } },
        @{ path = "agents.list[$AgentIndex].identity"; value = @{ name = '蓝一'; emoji = '🛡️'; theme = 'V88 只读会审' } },
        # ChatGPT/Codex OAuth must use the native Codex runtime.  Forcing the
        # generic OpenClaw runtime makes gpt-5.6-sol appear "unavailable" even
        # though the subscription is authenticated.
        @{ path = 'agents.defaults.models["openai/gpt-5.6-sol"].agentRuntime'; value = @{ id = 'codex' } },
        @{ path = 'plugins.entries.codex.enabled'; value = $true },
        @{ path = 'agents.defaults.skipBootstrap'; value = $true },
        @{ path = 'plugins.entries.codex.config'; value = $CodexPolicy },
        @{ path = 'tools.elevated.enabled'; value = $false },
        @{ path = 'channels.feishu.tools'; value = @{
            doc = $false; chat = $false; wiki = $false; drive = $false;
            perm = $false; scopes = $false; bitable = $false; base = $false
        } }
    )
    $BatchPath = Join-Path $env:TEMP 'openclaw_v88_config_batch.json'
    $Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText(
        $BatchPath,
        ($Batch | ConvertTo-Json -Depth 20),
        $Utf8NoBom
    )
    try {
        Run-Native '[5/7] 写入只读权限与 GPT 模型配置 ...' 'openclaw' @('config', 'set', '--batch-file', $BatchPath)
    } finally {
        Remove-Item -Force $BatchPath -ErrorAction SilentlyContinue
    }
    Run-Native '[5/7] 校验 OpenClaw 配置 ...' 'openclaw' @('config', 'validate')

    Run-Native '[6/7] 安装官方 Gateway 服务文件 ...' 'openclaw' @('gateway', 'install', '--force')
    $GatewayScript = Join-Path $env:USERPROFILE '.openclaw\gateway.cmd'
    if (-not (Test-Path $GatewayScript)) {
        throw "官方服务未生成 $GatewayScript"
    }
    $TaskName = 'OpenClaw Gateway'
    $Me = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
    $Action = New-ScheduledTaskAction `
        -Execute "$env:SystemRoot\System32\cmd.exe" `
        -Argument "/c `"$GatewayScript`""
    $Trigger = New-ScheduledTaskTrigger -AtStartup
    $Trigger.Delay = 'PT2M'
    $Principal = New-ScheduledTaskPrincipal -UserId $Me -LogonType S4U -RunLevel Highest
    $Settings = New-ScheduledTaskSettingsSet `
        -MultipleInstances IgnoreNew `
        -RestartCount 12 -RestartInterval (New-TimeSpan -Minutes 5) `
        -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
        -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
        -StartWhenAvailable
    Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger `
        -Principal $Principal -Settings $Settings -Force `
        -Description 'V88 OpenClaw Gateway: 开机+2分钟启动，失败每5分钟重试。' | Out-Null
    Start-ScheduledTask -TaskName $TaskName
    Log '[6/7] OpenClaw Gateway 已注册：开机+2分钟，无需用户登录，失败自动重试。'

    Log '[7/7] 安装主体完成。'
    Log '说明：本方案默认只使用 GPT 订阅；没有安装 Kimi API，也不会产生 Kimi API 费用。'
    Log 'K3 的评审结果由 V88 定时文件同步进本地只读工作区。'
    Log ''
    $startOauth = Read-Host '现在启动 GPT OAuth 授权吗？直接回车=是，输入 N=稍后'
    if ($startOauth -notmatch '^[Nn]$') {
        Write-Host '浏览器打开后，用你的 ChatGPT 账号完成授权。' -ForegroundColor Cyan
        & openclaw models auth login --provider openai
        if ($LASTEXITCODE -ne 0) {
            throw 'GPT OAuth 未完成。可稍后手动运行：openclaw models auth login --provider openai'
        }
        Run-Native '重启 Gateway 读取 GPT 凭据 ...' 'openclaw' @('gateway', 'restart')
        Log 'GPT OAuth 已完成。'
    } else {
        Log '稍后请运行：openclaw models auth login --provider openai'
    }

    Log ''
    Log '飞书暂未在 Win 启用，避免与当前 Mac 网关抢同一条消息。'
    Log 'Win 验收通过并停止 Mac 网关后，再双击：启用OpenClaw飞书-双击我.bat'
    Log "日志：$LogFile"
    Write-Host ''
    Write-Host '安装完成。请把本窗口最后结果告诉 GPT，再进行飞书主机切换。' -ForegroundColor Green
    Read-Host '按回车关闭'
} catch {
    Log ("[失败] {0}" -f $_.Exception.Message)
    Write-Host "失败。请把日志发给 GPT：$LogFile" -ForegroundColor Red
    Read-Host '按回车关闭'
    exit 1
}
