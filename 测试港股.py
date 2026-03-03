#!/usr/bin/env python3
"""
V75.1 港股数据快速测试脚本
用于验证港股代码格式修复是否生效
"""

import os

# 设置代理（根据你的端口调整）
PROXY_PORT = "1082"
os.environ['HTTP_PROXY'] = f'http://127.0.0.1:{PROXY_PORT}'
os.environ['HTTPS_PROXY'] = f'http://127.0.0.1:{PROXY_PORT}'

try:
    import yfinance as yf
    print("✅ yfinance 已安装\n")
except ImportError:
    print("❌ yfinance 未安装，请运行：pip install yfinance")
    exit(1)

# 测试港股代码转换
def to_yf_cn_code(code):
    if not code: return code
    code = code.upper().strip()
    if code.endswith('.SS') or code.endswith('.SZ') or code.endswith('.HK'): 
        return code
    if code.endswith('.SH'): 
        return code[:-3] + '.SS'
    if code.isdigit():
        if len(code) == 5 or len(code) == 4:
            hk_code = str(int(code))  # 去掉前导零
            return f'{hk_code}.HK'
        if code.startswith('6') or code.startswith('5'): 
            return f'{code}.SS'
        if code.startswith('0') or code.startswith('3'): 
            return f'{code}.SZ'
    return code

# 测试股票列表
hk_stocks = [
    ("00700", "腾讯控股"),
    ("09988", "阿里巴巴-SW"),
    ("03690", "美团-W"),
    ("01810", "小米集团-W"),
    ("02318", "中国平安"),
]

print("=" * 70)
print("港股代码格式测试")
print("=" * 70)

for code, name in hk_stocks:
    converted = to_yf_cn_code(code)
    print(f"  {code} ({name}) → {converted}")

print("\n" + "=" * 70)
print("港股数据获取测试（这可能需要10-30秒...）")
print("=" * 70 + "\n")

success_count = 0
fail_count = 0

for code, name in hk_stocks:
    yf_code = to_yf_cn_code(code)
    print(f"测试 {name} ({code} → {yf_code})...")
    
    try:
        tk = yf.Ticker(yf_code)
        df = tk.history(period='1mo', timeout=15)
        
        if df is not None and len(df) > 0:
            last_price = df['Close'].iloc[-1]
            print(f"  ✅ 成功！获取到 {len(df)} 行数据")
            print(f"     最新价格: ${last_price:.2f}")
            print(f"     日期范围: {df.index[0].date()} ~ {df.index[-1].date()}\n")
            success_count += 1
        else:
            print(f"  ⚠️  返回数据为空\n")
            fail_count += 1
    except Exception as e:
        print(f"  ❌ 失败: {type(e).__name__}: {str(e)[:80]}\n")
        fail_count += 1

print("=" * 70)
print(f"测试完成！成功: {success_count}/{len(hk_stocks)}, 失败: {fail_count}/{len(hk_stocks)}")
print("=" * 70)

if success_count == len(hk_stocks):
    print("\n🎉 恭喜！港股数据获取完全正常！")
    print("   现在可以启动 Streamlit 应用测试环球行业热力图了。")
elif success_count > 0:
    print(f"\n⚠️  部分成功 ({success_count}/{len(hk_stocks)})。可能的原因：")
    print("   1. 代理连接不稳定")
    print("   2. Yahoo Finance 服务暂时不可用")
    print("   3. 某些股票可能停牌")
else:
    print("\n❌ 所有测试失败。请检查：")
    print("   1. 代理是否正在运行？")
    print(f"   2. 代理端口是否为 {PROXY_PORT}？（在脚本开头修改 PROXY_PORT）")
    print("   3. 网络连接是否正常？")
    print("\n   测试代理连接：")
    print(f"   curl --proxy http://127.0.0.1:{PROXY_PORT} https://www.google.com")
