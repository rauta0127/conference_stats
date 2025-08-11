#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Whova埋め込みのあるページで、指定タイトルが実際に描画されるかを検知する。
- すべてのiframeを含めてテキスト検索
- JS描画に時間がかかる想定でポーリング（デフォ90秒）
- 見つかったらstdoutに FOUND とURLを出してexit 0、見つからなければ NOT_FOUND でexit 2
- デバッグ用に最後のスクリーンショットも保存
"""

import argparse
import re
import sys
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright


def normalize(s: str) -> str:
    # 大文字小文字無視 & 空白詰め（記号は残す：誤検知防止）
    s = s or ""
    s = s.lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def search_in_frame(frame, target_norm: str) -> bool:
    # 1) Playwrightのテキストセレクタで直接探す（速い）
    try:
        loc = frame.get_by_text(target_norm, exact=False)
        if loc.count() > 0:
            # count>0 でも部分一致なので、inner_textを再チェックして厳密に
            try:
                for i in range(min(loc.count(), 5)):
                    txt = loc.nth(i).inner_text(timeout=1000)
                    if target_norm in normalize(txt):
                        return True
            except Exception:
                pass
    except Exception:
        pass

    # 2) body全文を読んで文字列検索（iframeでもOK）
    try:
        body_text = frame.evaluate("document.body ? document.body.innerText : ''")
        if target_norm in normalize(body_text):
            return True
    except Exception:
        pass

    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="チェック対象URL（Whova埋め込みページ）")
    ap.add_argument("--title", required=True, help="探すイベントタイトル（完全一致に近い形が望ましい）")
    ap.add_argument("--timeout", type=int, default=90, help="最大待ち秒数（デフォ90s）")
    ap.add_argument("--headful", action="store_true", help="ブラウザを可視化して動かす（デバッグ用）")
    args = ap.parse_args()

    target_norm = normalize(args.title)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_png = Path(f"whova_check_{ts}.png")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headful, slow_mo=0)
        context = browser.new_context(viewport={"width": 1400, "height": 1000})
        page = context.new_page()

        # まず到達
        page.goto(args.url, wait_until="domcontentloaded", timeout=60_000)

        # ネットワーク静穏はWhovaが継続通信する場合があるので使いすぎない
        # 軽く待ってからポーリング開始
        page.wait_for_timeout(1500)

        found = False
        elapsed = 0
        step_ms = 1500
        max_ms = args.timeout * 1000

        while elapsed <= max_ms and not found:
            # メイン + すべてのフレームで検索
            frames = [page.main_frame] + page.frames
            for fr in frames:
                try:
                    if "whova" in (fr.url or "").lower():
                        # Whovaフレームなら短く待ってDOM安定
                        page.wait_for_timeout(300)
                except Exception:
                    pass

                if search_in_frame(fr, target_norm):
                    found = True
                    break

            if found:
                break

            page.wait_for_timeout(step_ms)
            elapsed += step_ms

        # スクリーンショット保存（デバッグ用）
        try:
            page.screenshot(path=str(out_png), full_page=True)
        except Exception:
            pass

        if found:
            print(f"FOUND\t{args.url}")
            print(f"[debug] screenshot: {out_png}")
            context.close()
            browser.close()
            sys.exit(0)
        else:
            print("NOT_FOUND")
            print(f"[debug] screenshot: {out_png}")
            context.close()
            browser.close()
            sys.exit(2)


if __name__ == "__main__":
    main()
