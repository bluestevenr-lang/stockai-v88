
# ============================================================
# V88 · OpenClaw Win 一键安装（2026-08-17 Kimi 准备）
# 由"安装OpenClaw-双击我.bat"调用，也可手动：
#   powershell -NoProfile -ExecutionPolicy Bypass -File install_openclaw_win.ps1
#
# 做什么：装 Node(缺才装) → 装 OpenClaw → doctor 初始化
#         → 装 Moonshot 官方插件 → 预写 kimi-k3 模型条目
#         → 注册登录自启的网关任务
# 不做什么：不配任何密钥。密钥两条命令需你亲手跑（见末尾输出）。
# ============================================================
$ErrorActionPreference = 'Continue'
$logDir = "$env:USERPROFILE\Desktop\StockAI\win\logs"
New-Item -ItemType Directory -Force $logDir | Out-Null
$log = "$logDir\openclaw_install_$(Get-Date -Format yyyyMMdd_HHmmss).log"
function Log($m) { $m | Tee-Object -FilePath $log -Append }

Log "== OpenClaw Win 安装开始 $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') =="

# 1) Node.js
$node = Get-Command node -ErrorAction SilentlyContinue
if (-not $node) {
    Log "[1/5] 未发现 Node.js，尝试 winget 安装 LTS ..."
    winget install --id OpenJS.NodeJS.LTS -e --accept-source-agreements --accept-package-agreements | Out-Null
    $env:Path = [Environment]::GetEnvironmentVariable('Path','Machine') + ';' + [Environment]::GetEnvironmentVariable('Path','User')
    $node = Get-Command node -ErrorAction SilentlyContinue
}
if (-not $node) {
    Log "[失败] 仍未找到 Node.js。请手动安装 LTS: https://nodejs.org/ 然后重跑本脚本。"
    exit 1
}
Log "[1/5] Node OK: $(node --version)"

# 2) OpenClaw 本体
Log "[2/5] 安装/更新 openclaw ..."
npm install -g openclaw 2>&1 | ForEach-Object { Log "$_" }
$env:Path += ";$env:APPDATA\npm"
$oc = Get-Command openclaw -ErrorAction SilentlyContinue
if (-not $oc) { Log "[失败] openclaw 安装后仍不在 PATH。"; exit 1 }
Log "[2/5] OpenClaw OK: $(openclaw --version)"

# 3) 初始化 + Moonshot 官方插件
Log "[3/5] doctor 初始化 + 安装 Moonshot 插件 ..."
openclaw doctor 2>&1 | ForEach-Object { Log "$_" }
openclaw plugins install @openclaw/moonshot-provider 2>&1 | ForEach-Object { Log "$_" }

# 4) 预写 moonshot/kimi-k3 模型条目（与 Mac 端同一份官方配置）
Log "[4/5] 写入 moonshot/kimi-k3 模型条目 ..."
$cfgPath = "$env:USERPROFILE\.openclaw\openclaw.json"
$mergeJs = @"
const fs=require('fs');
const p=process.argv[2];
let d={}; try{d=JSON.parse(fs.readFileSync(p,'utf8'))}catch(e){}
d.models=d.models||{}; d.models.mode=d.models.mode||'merge';
d.models.providers=d.models.providers||{};
d.models.providers.moonshot={baseUrl:'https://api.moonshot.cn/v1',api:'openai-completions',models:[{id:'kimi-k3',name:'Kimi K3',reasoning:true,input:['text','image','video'],contextWindow:1048576,maxTokens:8192,thinkingLevelMap:{off:null,minimal:'max',low:'max',medium:'max',high:'max',xhigh:'max',max:'max'},compat:{maxTokensField:'max_tokens',supportsUsageInStreaming:false,requiresStringContent:true,supportsReasoningEffort:true,supportedReasoningEfforts:['minimal','low','medium','high','xhigh','max']}}]};
fs.mkdirSync(require('path').dirname(p),{recursive:true});
fs.writeFileSync(p,JSON.stringify(d,null,2));
console.log('k3 entry written');
"@
$jsFile = "$env:TEMP\openclaw_k3_merge.js"
[System.IO.File]::WriteAllText($jsFile, $mergeJs)
node $jsFile $cfgPath | ForEach-Object { Log "$_" }

# 5) 注册登录自启网关任务（当前用户，无需管理员）
Log "[5/5] 注册 OpenClaw-Gateway 登录自启任务 ..."
$ocCmd = "$env:APPDATA\npm\openclaw.cmd"
if (Test-Path $ocCmd) {
    schtasks /create /f /tn "OpenClaw-Gateway" /sc ONLOGON /tr "`"$ocCmd`" gateway run" | Out-Null
    Log "    任务已注册: OpenClaw-Gateway (登录即运行 openclaw gateway run)"
} else {
    Log "    [跳过] 未找到 $ocCmd"
}

Log ""
Log "== 安装完成。还差两条命令需要你亲手跑（涉及密钥，脚本不代办）: =="
Log "  A. GPT 授权（弹浏览器，用你的 ChatGPT 账号点同意）:"
Log "     openclaw models auth login --provider openai"
Log "  B. 可选·Kimi API（先 platform.kimi.com 建 key 并充余额）:"
Log "     openclaw onboard --auth-choice moonshot-api-key-cn"
Log "  跑完后告诉 Kimi 一声即可。"
Log "== 日志文件: $log =="
