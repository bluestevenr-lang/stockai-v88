#!/bin/bash
printf '\033]0;V88-StockAI\007'
cd ~/Desktop/StockAI
arch -arm64 /Library/Frameworks/Python.framework/Versions/3.14/bin/python3.14 -m streamlit run app_v88_integrated.py
