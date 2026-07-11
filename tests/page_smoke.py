"""V88 页面级冒烟回测：真实执行整个 Streamlit 脚本（含全部UI渲染路径），
捕获任何运行时异常/页面错误框。改动后必须跑通此测试才允许说"修复完成"。"""
import sys
from streamlit.testing.v1 import AppTest

at = AppTest.from_file("app_v88_integrated.py", default_timeout=600)
at.run()
errs = [str(e.value)[:200] for e in at.exception] if at.exception else []
err_boxes = [str(b.value)[:160] for b in at.error] if hasattr(at, "error") else []
print("页面异常数:", len(errs))
for e in errs:
    print("  EXC:", e)
suspicious = [b for b in err_boxes if "not defined" in b or "Error" in b or "Traceback" in b]
for b in suspicious[:5]:
    print("  ERRBOX:", b)
sys.exit(1 if (errs or suspicious) else 0)
