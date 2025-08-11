#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, csv, re, sys, time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse
from datetime import datetime

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Error as PWError


@dataclass
class Event:
    title: str
    time: str
    location: str
    tags: List[str]
    url: str


TIME_PAT = re.compile(
    r"(?i)\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun|Aug|Sep|Sept|Oct|Nov|Dec)?\.?\s*"
    r"(?:\d{1,2}/\d{1,2})?\s*"
    r"(\d{1,2}:\d{2}\s?(?:AM|PM))\s?[-–~—]\s?(\d{1,2}:\d{2}\s?(?:AM|PM))"
)
LOC_PAT = re.compile(r"(?im)\b(?:Location|Room|Hall|Venue)\s*:\s*(.+)")
TAG_HINTS = ("tag", "chip", "label", "badge", "category", "pill")


def log(s):
    print(f"[LOG] {s}", flush=True)


def nrm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def to_abs(base: str, href: Optional[str]) -> str:
    return urljoin(base, href or "")


def looks_like_event(text: str) -> bool:
    t = nrm(text)
    return bool(re.search(r"(?i)\b(AM|PM)\b", t) and TIME_PAT.search(t))


def extract_from_html(html: str, base_url: str) -> Event:
    soup = BeautifulSoup(html, "lxml")
    # title
    title, title_el = "", None
    for sel in ("h1", "h2", "h3", "a[title]", "a strong", "strong", "a"):
        el = soup.select_one(sel)
        if el and nrm(el.get_text()):
            title_el = el
            title = nrm(el.get_text())
            break
    if not title:
        lines = [nrm(x) for x in soup.get_text("\n").split("\n")]
        lines = [ln for ln in lines if len(ln) >= 8 and not ln.lower().startswith(("location:", "room:", "hall:", "venue:", "time:"))]
        if lines:
            title = max(lines, key=len)
    # url
    url = ""
    if title_el:
        a = title_el if title_el.name == "a" else (title_el.find_parent("a") or title_el.find("a"))
        if a and a.has_attr("href"):
            url = to_abs(base_url, a["href"])
    if not url:
        a = soup.select_one("a[href]")
        if a:
            url = to_abs(base_url, a.get("href"))
    if url and not urlparse(url).scheme:
        url = ""
    # time
    full = nrm(soup.get_text(" "))
    m = TIME_PAT.search(full)
    time_str = nrm(m.group(0)) if m else ""
    # location
    location = ""
    m2 = LOC_PAT.search(full)
    if m2:
        location = nrm(m2.group(1))
        if len(location) > 80 and "," in location:
            location = location.split(",")[0].strip()
    # tags
    tags: List[str] = []
    for hint in TAG_HINTS:
        for t in soup.select(f"[class*='{hint}']"):
            txt = nrm(t.get_text())
            if not txt:
                continue
            if re.search(r"(?i)\b(location|room|hall|venue|time|am|pm)\b", txt):
                continue
            if 2 <= len(txt) <= 40 and txt not in tags:
                tags.append(txt)
    return Event(title=title, time=time_str, location=location, tags=tags, url=url)


def save_csv(events: List[Event], out_path: str):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["title", "time", "location", "tags", "url"])
        for e in events:
            w.writerow([e.title, e.time, e.location, ";".join(e.tags), e.url])


def pick_whova_frame(page, retry_ms: int = 60000):
    """whova/agenda/widget をURLにもつframe、または listitem 数が最多のframeを選ぶ"""
    start = time.time()
    best = None
    best_count = -1
    while (time.time() - start) * 1000 < retry_ms:
        frames = page.frames
        # 1) URLヒント
        whovas = [f for f in frames if any(k in (f.url or "").lower() for k in ("whova", "agenda", "widget"))]
        if whovas:
            return whovas[0]
        # 2) listitem最多
        for f in frames:
            try:
                cnt = f.get_by_role("listitem").count()
            except Exception:
                cnt = 0
            if cnt > best_count:
                best_count = cnt
                best = f
        if best and best_count >= 10:
            return best
        page.wait_for_timeout(1000)
    return best or page.main_frame


def wait_for_items(frame, min_items: int, timeout_ms: int) -> int:
    """リストアイテム数がmin_itemsに達するまでポーリング"""
    elapsed = 0
    step = 1500
    last_cnt = 0
    while elapsed <= timeout_ms:
        try:
            cnt = frame.get_by_role("listitem").count()
        except PWError:
            return 0  # frameが失効したら上位で取り直す
        if cnt >= min_items:
            return cnt
        if cnt != last_cnt:
            last_cnt = cnt
            log(f"  listitem count={cnt}")
        frame.page.wait_for_timeout(step)
        elapsed += step
    return last_cnt


def scrape(url: str, timeout_s: int, min_items: int, headful: bool, debug: bool) -> List[Event]:
    dbgdir = Path("debug_out")
    if debug:
        dbgdir.mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headful)
        context = browser.new_context(viewport={"width": 1440, "height": 1100})
        context.set_default_timeout(30_000)
        page = context.new_page()

        log(f"Open: {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(1200)

        all_events: List[Event] = []
        deadline = time.time() + timeout_s

        while time.time() < deadline:
            # フレームを選び直す（再描画で失効しがち）
            fr = pick_whova_frame(page, retry_ms=8_000)
            log(f"Use frame: {fr.url or '(main)'}")
            try:
                # 下まで数回スクロール（読み込み促進）
                for _ in range(6):
                    fr.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(600)
            except PWError:
                log("  frame scrolled failed, re-pick next loop")
                continue

            # 指定件数に達するまで待機
            cnt = wait_for_items(fr, min_items=min_items, timeout_ms=10_000)
            log(f"  observed listitems={cnt}")

            # デバッグダンプ
            if debug:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                try:
                    page.screenshot(path=str(dbgdir / f"shot_{ts}.png"), full_page=True)
                except Exception:
                    pass
                try:
                    (dbgdir / f"dump_{ts}.html").write_text(fr.content(), encoding="utf-8")
                except Exception:
                    pass

            # 抽出（各アイテムは個別tryで落ちない）
            events: List[Event] = []
            try:
                items = fr.get_by_role("listitem")
                total = items.count()
                log(f"  iterate items total={total}")
                for i in range(total):
                    it = items.nth(i)
                    try:
                        txt = it.inner_text(timeout=3000)
                        if not looks_like_event(txt):
                            continue
                        html = it.inner_html(timeout=3000)
                        ev = extract_from_html(html, base_url=url)
                        if not ev.title:
                            ev.title = nrm(txt.splitlines()[0] if txt else "")
                        events.append(ev)
                    except PWError:
                        # 要素が消えた/再描画された → スキップ
                        continue
            except PWError as e:
                log(f"  enumerate error: {e}")
                # 再ループしてフレームを取り直す
                continue

            # マージ（title+time）
            bykey = {}
            for e in events:
                key = (nrm(e.title).lower(), nrm(e.time).lower())
                if key in bykey:
                    m = bykey[key]
                    if not m.url and e.url:
                        m.url = e.url
                    if not m.location and e.location:
                        m.location = e.location
                    if e.tags:
                        m.tags = sorted(set(m.tags + e.tags))
                else:
                    bykey[key] = e
            merged = list(bykey.values())
            log(f"  extracted {len(merged)} events this round")

            # 収集できたら返す。0件が続く場合は、残り時間があれば再トライ
            if merged:
                all_events = merged
                break

            page.wait_for_timeout(1200)

        context.close()
        browser.close()
        return all_events


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", default="kdd2025_events.csv")
    ap.add_argument("--timeout", type=int, default=150, help="overall seconds to keep retrying")
    ap.add_argument("--min-items", type=int, default=20, help="wait until at least this many listitems appear")
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    try:
        events = scrape(args.url, args.timeout, args.min_items, args.headful, args.debug)
    except Exception as e:
        log(f"[FATAL] {e}")
        sys.exit(1)

    # 去重
    uniq = []
    seen = set()
    for e in events:
        key = (nrm(e.title).lower(), nrm(e.time).lower())
        if key in seen:
            continue
        seen.add(key)
        uniq.append(e)

    if not uniq:
        log("[WARN] No events extracted. Check debug_out/* and consider adjusting patterns/selectors.")
    save_csv(uniq, args.out)
    log(f"[OK] Saved {len(uniq)} events -> {args.out}")


if __name__ == "__main__":
    main()
