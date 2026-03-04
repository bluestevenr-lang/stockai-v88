#!/bin/bash

# 1. 停止旧进程
pkill -9 -f 'streamlit run app_v88_integrated.py' 2>/dev/null
pkill -9 -f 'run_v88.sh' 2>/dev/null
sleep 0.5

# 2. 后台启动 streamlit（绑定 127.0.0.1 避免防火墙弹框，日志写到 /tmp）
cd ~/Desktop/StockAI
nohup arch -arm64 /Library/Frameworks/Python.framework/Versions/3.14/bin/python3.14 \
    -m streamlit run app_v88_integrated.py \
    --server.address 127.0.0.1 \
    --server.headless true \
    --server.port 8501 \
    > /tmp/v88_streamlit.log 2>&1 &

echo $! > /tmp/v88_streamlit.pid

# 3. 等服务就绪后自动打开浏览器（最多等 15 秒）
for i in $(seq 1 15); do
    sleep 1
    if curl -s http://127.0.0.1:8501 > /dev/null 2>&1; then
        open http://localhost:8501
        exit 0
    fi
done

# 超时兜底：直接打开
open http://localhost:8501
