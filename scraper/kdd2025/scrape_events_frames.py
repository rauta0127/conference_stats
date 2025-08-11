#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, csv, re, sys
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
from typing import List, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout


@dataclass
class Event:
    title: str
    time: str
    location: str
    tags: List[str]
    url: str


TIME_PAT = re.compile(
    r"(?i)\b(?:mon|tue|wed|thu|fri|sat|sun|aug|sep|sept|oct|nov|dec)?\.?\s*"
    r"(?:\d{1,2}/\d{1,2})?\s*"
    r"(\d{1,2}:\d{2}\s?(?:AM|PM))\s?[-–~—]\s?(\d{1,2}:\d{2}\s?(?:AM|PM))"
)
LOC_PAT = re.compile(r"(?i)\b(location|room|hall|venue)\s*:?\s*(.+)")
TAG_CLASS_HINTS = ("tag", "chip", "label", "badge", "category", "pill")


def log(m):
    print(f"[LOG] {m}", flush=True)


def nrm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def to_abs(base: str, href: Optional[str]) -> str:
    if not href:
        return ""
    return urljoin(base, href)


def list_frames(page):
    infos = []
    for f in page.frames:
        infos.append((f.url, f.name or ""))
    return infos


def wait_until_any_content(frame, timeout_ms: int) -> bool:
    # Whovaは描画後、body.innerTextが長くなる
    elapsed = 0
    step = 1000
    while elapsed <= timeout_ms:
        try:
            txt = frame.evaluate("document.body ? document.body.innerText : ''")
        except Exception:
            txt = ""
        if txt and len(txt) > 200 and "Loading" not in txt:
            return True
        frame.page.wait_for_timeout(step)
        elapsed += step
    return False


def auto_scroll_frame(frame, rounds=12, step_px=1400, pause_ms=600):
    last_h = 0
    same = 0
    for _ in range(rounds):
        try:
            h = frame.evaluate("document.documentElement.scrollHeight || document.body.scrollHeight || 0")
        except Exception:
            break
        if h == 0:
            break
        y = 0
        while y < h:
            try:
                frame.evaluate(f"window.scrollTo(0,{y});")
            except Exception:
                pass
            frame.page.wait_for_timeout(pause_ms)
            y += step_px
        if h == last_h:
            same += 1
        else:
            same = 0
        last_h = h
        if same >= 2:
            break
    return last_h


def extract_via_html(html: str, base_url: str) -> List[Event]:
    soup = BeautifulSoup(html, "lxml")
    wrappers = soup.select("[role='listitem'], article, section, li, div.event, div.card, div")
    cands = []
    seen = set()
    for w in wrappers:
        if not w.select_one("a[href]"):
            continue
        text = nrm(w.get_text(" "))
        has_time = bool(TIME_PAT.search(text))
        has_loc = bool(LOC_PAT.search(text))
        title_el = None
        for sel in ("h1", "h2", "h3", "a[title]", "a strong", "strong", "a"):
            el = w.select_one(sel)
            if el and nrm(el.get_text()):
                title_el = el
                break
        if title_el or has_time or has_loc:
            key = (w.get("id", ""), nrm(text)[:200])
            if key in seen:
                continue
            seen.add(key)
            cands.append(w)

    events: List[Event] = []
    for card in cands:
        # title
        title = ""
        title_el = None
        for sel in ("h1", "h2", "h3", "a[title]", "a strong", "strong", "a"):
            el = card.select_one(sel)
            if el and nrm(el.get_text()):
                title_el = el
                title = nrm(el.get_text())
                break
        if not title:
            lines = [nrm(x) for x in card.get_text("\n").split("\n")]
            lines = [ln for ln in lines if len(ln) >= 8]
            if lines:
                title = max(lines, key=len)

        # url
        url = ""
        if title_el:
            a = title_el if title_el.name == "a" else (title_el.find_parent("a") or title_el.find("a"))
            if a and a.has_attr("href"):
                url = to_abs(base_url, a["href"])
        if not url:
            a = card.select_one("a[href]")
            if a:
                url = to_abs(base_url, a["href"])
        if url and not urlparse(url).scheme:
            url = ""

        # time
        full = nrm(card.get_text(" "))
        m = TIME_PAT.search(full)
        time_str = nrm(m.group(0)) if m else ""

        # location
        location = ""
        m2 = LOC_PAT.search(full)
        if m2:
            location = nrm(m2.group(2))
            if len(location) > 80 and "," in location:
                location = location.split(",")[0].strip()

        # tags
        tags: List[str] = []
        tag_like = []
        for hint in TAG_CLASS_HINTS:
            tag_like.extend(card.select(f"[class*='{hint}']"))
        for t in tag_like:
            txt = nrm(t.get_text())
            if not txt:
                continue
            if re.search(r"(?i)\b(location|room|hall|venue|time|am|pm)\b", txt):
                continue
            if 2 <= len(txt) <= 40 and txt not in tags:
                tags.append(txt)

        if title or url:
            events.append(Event(title=title, time=time_str, location=location, tags=tags, url=url))
    return events


def extract_via_text(frame_text: str) -> List[Event]:
    """
    iframeのinnerTextだけで最低限抽出するフォールバック。
    - タイトル行: 60字以内の長め行、直後行に時間パターンがある、などをヒューリスティックで束ねる
    - URLは基本空になりやすい（WhovaはJSハンドラ）。後段で公式ページURLに置換する想定
    """
    lines = [nrm(x) for x in frame_text.splitlines()]
    lines = [ln for ln in lines if ln]
    events: List[Event] = []
    i = 0
    while i < len(lines):
        title = lines[i]
        # タイトルっぽさ: 英字混じり/句読点少なめ/そこそこ長い
        if 8 <= len(title) <= 180 and not title.lower().startswith(("location:", "room:", "hall:", "time:")):
            # 次の数行に時間・場所が来る？
            time_str, location = "", ""
            j = i + 1
            scan_limit = min(i + 6, len(lines))
            while j < scan_limit:
                if not time_str and TIME_PAT.search(lines[j]):
                    time_str = TIME_PAT.search(lines[j]).group(0)
                if not location:
                    m = LOC_PAT.search(lines[j])
                    if m:
                        location = nrm(m.group(2))
                j += 1
            # 何かしら拾えたらイベントとみなす
            if time_str or location:
                events.append(Event(title=title, time=nrm(time_str), location=nrm(location), tags=[], url=""))
                i = j
                continue
        i += 1
    return events


def save_csv(events: List[Event], out: str):
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["title", "time", "location", "tags", "url"])
        for e in events:
            w.writerow([e.title, e.time, e.location, ";".join(e.tags), e.url])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", default="kdd2025_events.csv")
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    dbgdir = Path("debug_out")
    if args.debug:
        dbgdir.mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headful, slow_mo=0)
        context = browser.new_context(viewport={"width": 1440, "height": 1100})
        page = context.new_page()

        log(f"Open: {args.url}")
        page.goto(args.url, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(1500)

        # すべてのフレーム列挙
        frs = list_frames(page)
        log("Frames on load:")
        for idx, (u, n) in enumerate(frs):
            log(f"  [{idx}] url={u or '(empty)'} name={n!r}")

        # Whova系フレーム優先（URLにwhova含む/未知だが目視で増えるフレーム）
        def is_candidate(u: str) -> bool:
            u = (u or "").lower()
            return ("whova" in u) or ("agenda" in u) or ("widget" in u)

        frames = [f for f in page.frames]
        # 読み込みが遅いことがあるので、少し待ちながら再収集
        for _ in range(6):
            frames = page.frames
            if any(is_candidate(f.url) for f in frames):
                break
            page.wait_for_timeout(1000)

        # 各フレームでトライ（メイン→候補iframeの順）
        search_order = []
        main_f = page.main_frame
        search_order.append(main_f)
        for f in frames:
            if f is not main_f and is_candidate(f.url):
                search_order.append(f)

        all_events: List[Event] = []
        for idx, fr in enumerate(search_order):
            log(f">> Inspect frame[{idx}] {fr.url or '(main)'}")
            # まず待機＆オートスクロール
            ready = wait_until_any_content(fr, timeout_ms=args.timeout * 1000 // 2)
            log(f"   content ready: {ready}")
            auto_scroll_frame(fr, rounds=10, step_px=1400, pause_ms=600)

            # デバッグダンプ
            if args.debug:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                try:
                    fr.page.screenshot(path=str(dbgdir / f"shot_frame{idx}_{ts}.png"), full_page=True)
                except Exception:
                    pass
                try:
                    html = fr.content()
                    Path(dbgdir / f"dump_frame{idx}_{ts}.html").write_text(html, encoding="utf-8")
                except Exception:
                    html = ""

            # 1) HTMLパース
            ev_html: List[Event] = []
            try:
                html = fr.content()
                ev_html = extract_via_html(html, base_url=args.url)
                log(f"   HTML parsed events: {len(ev_html)}")
            except Exception as e:
                log(f"   HTML parse error: {e}")

            # 2) テキストフォールバック
            ev_text: List[Event] = []
            try:
                txt = fr.evaluate("document.body ? document.body.innerText : ''")
                ev_text = extract_via_text(txt)
                log(f"   TEXT parsed events: {len(ev_text)}")
            except Exception as e:
                log(f"   TEXT parse error: {e}")

            # マージ（title+timeで重複除去、URLはHTML側を優先）
            merged: List[Event] = []
            by_key = {}
            for e in ev_html + ev_text:
                key = (nrm(e.title).lower(), nrm(e.time).lower())
                if key in by_key:
                    # URLが空なら埋める
                    if not by_key[key].url and e.url:
                        by_key[key].url = e.url
                    # location/tagsを補完
                    if not by_key[key].location and e.location:
                        by_key[key].location = e.location
                    if e.tags:
                        by_key[key].tags = sorted(set(by_key[key].tags + e.tags))
                else:
                    by_key[key] = e
            merged = list(by_key.values())
            log(f"   merged events this frame: {len(merged)}")

            all_events.extend(merged)

        # 全体去重（title+time）
        uniq = []
        seen = set()
        for e in all_events:
            key = (nrm(e.title).lower(), nrm(e.time).lower())
            if key in seen:
                continue
            seen.add(key)
            uniq.append(e)

        save_csv(uniq, args.out)
        log(f"Saved {len(uniq)} events -> {args.out}")

        context.close()
        browser.close()


if __name__ == "__main__":
    main()
