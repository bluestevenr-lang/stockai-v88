#!/bin/bash

# Reuse the running service; never kill all Streamlit instances or arbitrary ports.
cd /Users/bluesteven/Desktop/StockAI || exit 1
PYTHON_BIN="/Library/Frameworks/Python.framework/Versions/3.14/bin/python3"
[ -x "$PYTHON_BIN" ] || PYTHON_BIN="$(command -v python3)"
"$PYTHON_BIN" runtime_guard.py --port 8501 || exit 1

# 4. 等服务就绪后打开/刷新浏览器（最多等 20 秒）
#    【V88定则 2026-07-18】已开着 V88 标签页就在原标签刷新并前置，不再开新标签占资源；
#    刷新=导航到干净首页（洗掉 ?q= 等残留深链参数，防止每次启动都跳回同一只股）；
#    依次找 Chrome / Edge / Safari 里含 8501 的标签，全都没有才 open 新开。
reuse_tab() {
    /usr/bin/osascript <<'OSA' 2>/dev/null
set hit to false
tell application "System Events" to set runningApps to name of every process
if runningApps contains "Google Chrome" then
    tell application "Google Chrome"
        set wIdx to 0
        repeat with w in windows
            set wIdx to wIdx + 1
            set tIdx to 0
            repeat with t in tabs of w
                set tIdx to tIdx + 1
                if (URL of t contains "localhost:8501") or (URL of t contains "127.0.0.1:8501") then
                    set URL of t to "http://localhost:8501"
                    set active tab index of w to tIdx
                    set index of w to 1
                    set hit to true
                    exit repeat
                end if
            end repeat
            if hit then exit repeat
        end repeat
        if hit then activate
    end tell
end if
if (not hit) and (runningApps contains "Microsoft Edge") then
    tell application "Microsoft Edge"
        set wIdx to 0
        repeat with w in windows
            set wIdx to wIdx + 1
            set tIdx to 0
            repeat with t in tabs of w
                set tIdx to tIdx + 1
                if (URL of t contains "localhost:8501") or (URL of t contains "127.0.0.1:8501") then
                    set URL of t to "http://localhost:8501"
                    set active tab index of w to tIdx
                    set index of w to 1
                    set hit to true
                    exit repeat
                end if
            end repeat
            if hit then exit repeat
        end repeat
        if hit then activate
    end tell
end if
if (not hit) and (runningApps contains "Safari") then
    tell application "Safari"
        repeat with w in windows
            set tIdx to 0
            repeat with t in tabs of w
                set tIdx to tIdx + 1
                if (URL of t contains "localhost:8501") or (URL of t contains "127.0.0.1:8501") then
                    set URL of t to "http://localhost:8501"
                    tell w to set current tab to tab tIdx
                    set hit to true
                    exit repeat
                end if
            end repeat
            if hit then exit repeat
        end repeat
        if hit then activate
    end tell
end if
if hit then
    return "reused"
end if
return "none"
OSA
}

open_or_reuse() {
    if [ "$(reuse_tab)" = "reused" ]; then
        return 0
    fi
    open http://localhost:8501
}

for i in $(seq 1 20); do
    sleep 1
    if curl -s http://127.0.0.1:8501 > /dev/null 2>&1; then
        open_or_reuse
        exit 0
    fi
done

# 超时兜底：直接打开/刷新
open_or_reuse
