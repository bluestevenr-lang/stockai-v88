"""
ocr_holdings.py — 券商持仓截图 → 纯文本(Apple Vision OCR)
通用底座：不认排版，只把图里所有文字按行、按位置抽出来，交给上层解析。
支持 A股(东财) / 美股(其他券商) 任意截图。离线、免费、中文一流。
用法: python3 ocr_holdings.py <图片路径>
"""
import sys
from pathlib import Path


def ocr_image(img_path: str) -> list[dict]:
    """返回 [{text, x, y, w, h, conf}]，按从上到下、从左到右排序。"""
    import Vision
    import Quartz
    from Foundation import NSURL

    url = NSURL.fileURLWithPath_(str(img_path))
    src = Quartz.CGImageSourceCreateWithURL(url, None)
    if not src:
        raise RuntimeError(f"无法读取图片: {img_path}")
    cg_img = Quartz.CGImageSourceCreateImageAtIndex(src, 0, None)

    results = []
    handler = Vision.VNImageRequestHandler.alloc().initWithCGImage_options_(cg_img, None)

    def _done(request, error):
        for obs in (request.results() or []):
            txt = obs.topCandidates_(1)[0].string()
            bb = obs.boundingBox()  # 归一化坐标，原点在左下
            results.append({
                "text": txt,
                "x": bb.origin.x,
                "y": 1.0 - bb.origin.y - bb.size.height,  # 转成原点在左上
                "w": bb.size.width,
                "h": bb.size.height,
                "conf": obs.confidence(),
            })

    req = Vision.VNRecognizeTextRequest.alloc().initWithCompletionHandler_(_done)
    req.setRecognitionLevel_(1)                    # accurate
    req.setUsesLanguageCorrection_(True)
    req.setRecognitionLanguages_(["zh-Hans", "en-US"])
    handler.performRequests_error_([req], None)

    results.sort(key=lambda r: (round(r["y"], 3), r["x"]))
    return results


def to_lines(boxes: list[dict], y_tol: float = 0.012) -> list[str]:
    """把同一水平线上的文字块拼成一行（按 y 聚类），便于阅读/解析。"""
    lines, cur, cur_y = [], [], None
    for b in boxes:
        if cur_y is None or abs(b["y"] - cur_y) <= y_tol:
            cur.append(b); cur_y = b["y"] if cur_y is None else cur_y
        else:
            lines.append("  ".join(x["text"] for x in sorted(cur, key=lambda z: z["x"])))
            cur, cur_y = [b], b["y"]
    if cur:
        lines.append("  ".join(x["text"] for x in sorted(cur, key=lambda z: z["x"])))
    return lines


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python3 ocr_holdings.py <图片路径>"); sys.exit(1)
    p = sys.argv[1]
    if not Path(p).exists():
        print(f"文件不存在: {p}"); sys.exit(1)
    boxes = ocr_image(p)
    print(f"=== OCR 抽出 {len(boxes)} 个文字块，按行还原 ===\n")
    for ln in to_lines(boxes):
        print(ln)
