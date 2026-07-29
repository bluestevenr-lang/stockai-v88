#!/usr/bin/env python3
"""V100 CI 强制规则检查：每文件≤300行，每函数≤50行，禁止版本号注释"""
import ast
import sys
from pathlib import Path

MAX_FILE_LINES = 300
MAX_FUNC_LINES = 50
VERSION_PATTERN = ["#V", "【V", "V88", "V89", "V90", "V91", "V92", "V93"]

errors = []

v100_dir = Path(__file__).parent
py_files = [f for f in v100_dir.rglob("*.py") if f.name != "ci_check.py"]

for filepath in py_files:
    lines = filepath.read_text(encoding="utf-8").splitlines()
    rel = filepath.relative_to(v100_dir)

    if len(lines) > MAX_FILE_LINES:
        errors.append(f"[行数超限] {rel}: {len(lines)}行 > {MAX_FILE_LINES}行")

    for i, line in enumerate(lines, 1):
        for pat in VERSION_PATTERN:
            if pat in line:
                errors.append(f"[版本注释] {rel}:{i}: {line.strip()[:60]}")
                break

    try:
        tree = ast.parse("\n".join(lines))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                end = getattr(node, "end_lineno", node.lineno)
                func_lines = end - node.lineno + 1
                if func_lines > MAX_FUNC_LINES:
                    errors.append(
                        f"[函数超限] {rel}: def {node.name}() "
                        f"L{node.lineno}-L{end} = {func_lines}行 > {MAX_FUNC_LINES}行"
                    )
    except SyntaxError as e:
        errors.append(f"[语法错误] {rel}: {e}")

if errors:
    print("❌ CI 检查失败：")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)
else:
    print(f"✅ CI 检查通过（{len(py_files)} 个文件）")
