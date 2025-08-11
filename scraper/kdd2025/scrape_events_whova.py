#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
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


# ゆるい時間表現（AM/PM）
TIME_PAT = re.compile(
    r"(?i)\b(?:mon|tue|wed|thu|fri|sat|sun|aug|sep|sept|oct|nov|dec)?\.?\s*"
    r"(?:\d{1,2}/\d{1,2})?\s*"
    r"(\d{1,2}:\d{2}\s?(?:AM|PM))\s?[-–~—]\s?(\d{1,2}:\d{2}\s?(?:AM|PM))"
)
# 「Location: XXX」「Room: YYY」など
LOCATION_PAT = re.compile(r"(?i)\b(location|room|hall|venue)\s*:?\s*(.+)")
TAG_CLASS_HINTS = ("tag", "chip", "label", "badge", "category", "pill")


def log(msg: str):
    print(f"[LOG] {msg}", flush=True)


def nrm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def to_abs(base: str, href: Optional[str]) -> str:
    if not href:
        return ""
    return urljoin(base, href)


def wait_for_whova_render(page, timeout_ms: int):
    """
    #whova-agendawidget が 'Loading...' 状態から実体に置き換わる/子要素が増えるのを待つ。
    """
    page.wait_for_timeout(1000)
    try:
        page.wait_for_selector("#whova-agendawidget", timeout=30_000)
    except PWTimeout:
        return False

    elapsed = 0
    step = 1000
    while elapsed <= timeout_ms:
        try:
            txt = page.locator("#whova-agendawidget").inner_text(timeout=2000)
        except Exception:
            txt = ""
        # Loading が消えて、文字数が増えていればOK目安
        if "Loading" not in txt and len(txt) > 200:
            return True
        page.wait_for_timeout(step)
        elapsed += step
    return False


def auto_scroll(page, container_selector: str = "body", max_rounds=12, step_px=1400, pause_ms=700):
    """
    メインフレーム内でオートスクロール（Whovaは無限ロード気味なため）
    """
    last_h = 0
    same = 0
    for _ in range(max_rounds):
        try:
            h = page.evaluate(f"document.querySelector('{container_selector}')?.scrollHeight || document.body.scrollHeight || 0")
        except Exception:
            break
        if h == 0:
            break
        y = 0
        while y < h:
            try:
                page.evaluate(f"window.scrollTo(0, {y});")
            except Exception:
                pass
            page.wait_for_timeout(pause_ms)
            y += step_px
        if h == last_h:
            same += 1
        else:
            same = 0
        last_h = h
        if same >= 2:
            break
    return last_h


def find_cards(html: str):
    """
    カード候補抽出：
    - Whovaはdiv大量。a[href]を含み、時間/場所テキストを近傍に持つブロックを拾う。
    - まとまり要素（article/section/li/div[role='listitem']/div.card/div.event など）を広めに拾う。
    """
    soup = BeautifulSoup(html, "html.parser")
    wrappers = soup.select("[role='listitem'], article, section, li, div.event, div.card, div")
    cands = []
    seen = set()
    for w in wrappers:
        if not w.select_one("a[href]"):
            continue
        text = nrm(w.get_text(" "))
        has_time = bool(TIME_PAT.search(text))
        has_loc = bool(LOCATION_PAT.search(text))
        # タイトル候補（見出し or 太字 or aタグテキスト）
        title_el = None
        for sel in ("h1", "h2", "h3", "a[title]", "a strong", "strong", "a"):
            el = w.select_one(sel)
            if el and nrm(el.get_text()):
                title_el = el
                break
        if title_el or has_time or has_loc:
            key = (w.get("id", ""), nrm(w.get_text())[:200])
            if key in seen:
                continue
            seen.add(key)
            cands.append(w)
    return cands


def extract_event(card, base_url: str) -> Event:
    # タイトル
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

    # URL（Whovaのイベント詳細や外部詳細）
    url = ""
    if title_el:
        a = title_el if title_el.name == "a" else (title_el.find_parent("a") or title_el.find("a"))
        if a and a.has_attr("href"):
            url = to_abs(base_url, a["href"])
    if not url:
        a = card.select_one("a[href]")
        if a:
            url = to_abs(base_url, a["href"])

    # Whovaの中にはJSハンドラ用ダミーリンクもあるので、schemelessを排除
    if url and not urlparse(url).scheme:
        url = ""

    # time
    full = nrm(card.get_text(" "))
    m = TIME_PAT.search(full)
    time_str = nrm(m.group(0)) if m else ""

    # location
    location = ""
    for line in full.split():
        pass
    m2 = LOCATION_PAT.search(full)
    if m2:
        # group(2)が中身を含むことが多い
        location = nrm(m2.group(2))
        # 長すぎる場合は区切りで短縮
        if len(location) > 80 and "," in location:
            location = location.split(",")[0].strip()

    # tags（小さなピル/ラベルっぽい要素）
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

    return Event(title=title or "", time=time_str, location=location, tags=tags, url=url or "")


def save_csv(events: List[Event], out_path: str):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["title", "time", "location", "tags", "url"])
        for ev in events:
            w.writerow([ev.title, ev.time, ev.location, ";".join(ev.tags), ev.url])


def main():
    ap = argparse.ArgumentParser(description="Scrape KDD2025 events from Whova-embedded page")
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", default="kdd2025_events.csv")
    ap.add_argument("--timeout", type=int, default=90, help="max seconds to wait for Whova render")
    ap.add_argument("--headful", action="store_true", help="show browser window")
    ap.add_argument("--debug", action="store_true", help="dump HTML and screenshot to debug_out/")
    args = ap.parse_args()

    debug_dir = Path("debug_out")
    if args.debug:
        debug_dir.mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headful)
        context = browser.new_context(viewport={"width": 1440, "height": 1100})
        page = context.new_page()

        log(f"Open: {args.url}")
        page.goto(args.url, wait_until="domcontentloaded", timeout=60_000)

        ok = wait_for_whova_render(page, timeout_ms=args.timeout * 1000)
        log(f"Whova render ready: {ok}")

        # 追加ロード分のため軽くスクロール
        height = auto_scroll(page, max_rounds=12, step_px=1400, pause_ms=700)
        log(f"Scrolled contentHeight={height}")

        # 抽出対象は #whova-agendawidget の中身を優先
        try:
            html = page.locator("#whova-agendawidget").inner_html(timeout=3000)
        except Exception:
            html = page.content()

        # デバッグ保存
        if args.debug:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            (debug_dir / f"dump_{ts}.html").write_text(html, encoding="utf-8")
            try:
                page.screenshot(path=str(debug_dir / f"shot_{ts}.png"), full_page=True)
            except Exception:
                pass
            log(f"Saved debug_out/dump_{ts}.html (and screenshot)")

        # カード抽出
        cards = find_cards(html)
        log(f"Candidate cards: {len(cards)}")

        events: List[Event] = []
        for c in cards:
            ev = extract_event(c, base_url=args.url)
            # title or url が空のゴミは除外
            if nrm(ev.title) or nrm(ev.url):
                events.append(ev)

        # 去重（title+url）
        uniq = []
        seen = set()
        for ev in events:
            key = (nrm(ev.title).lower(), ev.url.lower())
            if key in seen:
                continue
            seen.add(key)
            uniq.append(ev)

        save_csv(uniq, args.out)
        log(f"Saved {len(uniq)} events -> {args.out}")

        context.close()
        browser.close()


if __name__ == "__main__":
    main()
