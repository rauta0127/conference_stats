#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, csv, re, sys, time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from urllib.parse import urljoin, urlparse
from datetime import datetime

from playwright.sync_api import sync_playwright, Error as PWError, TimeoutError as PWTimeout


@dataclass
class Event:
    title: str
    time: str
    location: str
    tags: List[str]
    url: str


TIME_PAT = re.compile(r"(?i)\b\d{1,2}:\d{2}\s?(?:am|pm)\s?[-–~—]\s?\d{1,2}:\d{2}\s?(?:am|pm)\b")


def nrm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def to_abs(base: str, href: Optional[str]) -> str:
    if not href:
        return ""
    return urljoin(base, href)


def log(m):
    print(f"[LOG] {m}", flush=True)


def pick_whova_frame(page):
    # whovaドメイン or embedded/event を優先
    for _ in range(8):
        frames = page.frames
        for f in frames:
            u = (f.url or "").lower()
            if "whova.com" in u or "embedded/event" in u:
                return f
        page.wait_for_timeout(800)
    # 見つからなければメイン
    return page.main_frame


def wait_sessions(frame, min_cnt=5, timeout_ms=60_000) -> int:
    """div.session が出るまで待つ（SPAの再描画にも耐える）"""
    start = time.time()
    last = -1
    while (time.time() - start) * 1000 < timeout_ms:
        try:
            cnt = frame.locator("div.session").count()
        except PWError:
            return 0
        if cnt != last:
            log(f"  session count={cnt}")
            last = cnt
        if cnt >= min_cnt:
            return cnt
        # 読み込み促進
        try:
            frame.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except PWError:
            pass
        frame.page.wait_for_timeout(800)
    return last if last >= 0 else 0


def extract_one(frame, session_nth, base_url: str) -> Optional[Event]:
    s = frame.locator("div.session").nth(session_nth)

    # タイトル
    title = ""
    try:
        # span.session-title が第一候補
        title_locator = s.locator("span.session-title")
        if title_locator.count() > 0:
            title = nrm(title_locator.first.inner_text(timeout=1500))
        if not title:
            # hタグやa strong などフォールバック
            for sel in ["h1, h2, h3", "a[title]", "a strong", "strong", "a"]:
                loc = s.locator(sel)
                if loc.count() > 0:
                    t = nrm(loc.first.inner_text(timeout=800))
                    if t:
                        title = t
                        break
    except PWError:
        pass

    # 時間
    time_str = ""
    try:
        # Whovaの時間は左カラムにあることが多い
        # 1) div.session-time 内のテキスト
        time_loc = s.locator("div.session-time, div.time, .time")
        if time_loc.count() > 0:
            t = nrm(time_loc.first.inner_text(timeout=800))
            m = TIME_PAT.search(t)
            time_str = m.group(0) if m else t
        # 2) フォールバック：カード全文から時間パターン
        if not time_str:
            m = TIME_PAT.search(nrm(s.inner_text(timeout=800)))
            if m:
                time_str = m.group(0)
    except PWError:
        pass

    # 場所
    location = ""
    try:
        loc_loc = s.locator("div.session-location, .location")
        if loc_loc.count() > 0:
            location = nrm(loc_loc.first.inner_text(timeout=800))
        else:
            # 'Location:' 文を拾う
            txt = nrm(s.inner_text(timeout=800))
            m = re.search(r"(?i)\b(?:Location|Room|Hall|Venue)\s*:\s*(.+)", txt)
            if m:
                location = nrm(m.group(1))
    except PWError:
        pass

    # タグ
    tags: List[str] = []
    try:
        chips = s.locator(".session-tracks >> * , [class*='tag'], [class*='chip'], [class*='badge'], [class*='label']")
        c = min(chips.count(), 20)
        for i in range(c):
            t = nrm(chips.nth(i).inner_text(timeout=600))
            if not t:
                continue
            if re.search(r"(?i)\b(location|room|hall|venue|time|am|pm)\b", t):
                continue
            if 2 <= len(t) <= 40 and t not in tags:
                tags.append(t)
    except PWError:
        pass

    # URL（詳細）
    url = ""
    try:
        # 「view more detailed information」リンクや最初のaを優先
        a_loc = s.locator("a:has-text('view more detailed information'), a")
        if a_loc.count() > 0:
            href = a_loc.first.get_attribute("href", timeout=800)
            if href:
                absu = to_abs(base_url, href)
                if urlparse(absu).scheme:
                    url = absu
    except PWError:
        pass

    # 空カードは捨てる
    if not any([title, time_str, location, url]):
        return None
    return Event(title=title, time=time_str, location=location, tags=tags, url=url)


def save_csv(events: List[Event], out_path: str):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["title", "time", "location", "tags", "url"])
        for e in events:
            w.writerow([e.title, e.time, e.location, ";".join(e.tags), e.url])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", default="kdd2025_events.csv")
    ap.add_argument("--timeout", type=int, default=150)
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    dbgdir = Path("debug_out")
    if args.debug:
        dbgdir.mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headful)
        context = browser.new_context(viewport={"width": 1440, "height": 1100})
        page = context.new_page()

        log(f"Open: {args.url}")
        page.goto(args.url, wait_until="domcontentloaded", timeout=60_000)

        # Whovaのiframeへ
        fr = pick_whova_frame(page)
        log(f"Use frame: {fr.url or '(main)'}")

        # セッション（div.session）が現れるまで待機＆スクロール
        cnt = wait_sessions(fr, min_cnt=5, timeout_ms=min(args.timeout * 1000, 90_000))
        log(f"Final session count seen={cnt}")

        # 追加ロード促進（数回スクロール）
        for _ in range(8):
            try:
                fr.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except PWError:
                pass
            page.wait_for_timeout(500)

        # デバッグダンプ
        if args.debug:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            try:
                page.screenshot(path=str(dbgdir / f"shot_{ts}.png"), full_page=True)
            except Exception:
                pass
            try:
                (dbgdir / f"dump_{ts}.html").write_text(fr.content(), encoding="utf-8")
            except Exception:
                pass

        # 収集
        events: List[Event] = []
        try:
            total = fr.locator("div.session").count()
            log(f"Iterate sessions total={total}")
            for i in range(total):
                try:
                    ev = extract_one(fr, i, base_url=args.url)
                    if ev:
                        events.append(ev)
                except PWError:
                    continue
        except PWError as e:
            log(f"enumeration failed: {e}")

        # 去重（title+time）
        seen = set()
        uniq: List[Event] = []
        for e in events:
            key = (nrm(e.title).lower(), nrm(e.time).lower())
            if key in seen:
                continue
            seen.add(key)
            uniq.append(e)

        save_csv(uniq, args.out)
        log(f"[OK] Saved {len(uniq)} events -> {args.out}")

        context.close()
        browser.close()


if __name__ == "__main__":
    main()
