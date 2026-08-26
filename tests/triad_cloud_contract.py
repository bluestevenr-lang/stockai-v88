import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class TriadCloudBoardTests(unittest.TestCase):
    def test_desktop_scan_cannot_publish_legacy_board(self):
        path = ROOT / "app_v88_integrated.py"
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        fn = next(
            node for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "_publish_scan_to_cloud"
        )
        block = ast.get_source_segment(source, fn) or ""
        self.assertNotIn("gh api", block)
        self.assertNotIn("subprocess", block)
        self.assertNotIn("_repo_path", block)
        self.assertIn("triad_selection_pub.json", block)

    def test_cloud_board_reads_only_central_triad_projection(self):
        source = (ROOT / "streamlit_app.py").read_text(encoding="utf-8")
        start = source.index('elif _nav == "🏆 全选榜单":')
        end = source.index('# ── 🔍 个股搜索', start)
        block = source[start:end]
        self.assertIn('pub_text("triad_selection_pub.json")', block)
        self.assertNotIn('pub_text("scan_latest.json")', block)
        self.assertNotIn("V88唯一统一分", block)
        self.assertIn("GPT × K3 × 经典书理", block)
        self.assertIn("非推荐·不可直接执行", block)


if __name__ == "__main__":
    unittest.main()
