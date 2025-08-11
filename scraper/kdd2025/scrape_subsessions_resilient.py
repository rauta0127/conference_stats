#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, csv, re, sys, time, random
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Error as PWError, TimeoutError as PWTimeout

@dataclass
class SubEvent:
    parent_title: str
    parent_time: str
    parent_location: str
    parent_tags: str
    parent_url: str
    title: str
    time: str
    location: str
    url: str

def nrm(s:str)->str: return re.sub(r"\s+"," ", (s or "").strip())
TIME_PAT12 = re.compile(r"(?i)\b\d{1,2}:\d{2}\s?(?:am|pm)\s?[-–—]\s?\d{1,2}:\d{2}\s?(?:am|pm)\b")
WHOVA_SESSION = re.compile(r"https?://(?:www\.)?whova\.com/embedded/session/", re.I)

# ---- Rate limiting / Backoff ----
class RateLimiter:
    def __init__(self, max_rps: float, jitter_ms: Tuple[int,int]):
        self.interval = 1.0 / max(0.01, max_rps)
        self.jmin, self.jmax = jitter_ms
        self._last = 0.0
    def wait(self):
        now = time.time()
        delta = self.interval - (now - self._last)
        if delta > 0: time.sleep(delta)
        if self.jmax > 0:
            time.sleep(random.uniform(self.jmin/1000.0, self.jmax/1000.0))
        self._last = time.time()

class Backoff:
    def __init__(self, base=2.0, factor=2.0, cap=90.0):
        self.base, self.factor, self.cap = base, factor, cap
        self.n = 0
    def reset(self): self.n = 0
    def sleep(self, why=""):
        t = min(self.cap, self.base * (self.factor ** self.n)) * random.uniform(0.85, 1.15)
        self.n += 1
        print(f"[BACKOFF] sleep ~{t:.1f}s {why}", flush=True)
        time.sleep(t)

# ---- CSV helpers ----
def find_url_column(header: List[str]) -> Optional[str]:
    lowers = [h.lower() for h in header]
    for cand in ("url","link","href"):
        if cand in lowers:
            return header[lowers.index(cand)]
    return None

def read_parent_events(path: Path) -> Tuple[List[Dict[str,str]], Optional[str]]:
    rows: List[Dict[str,str]] = []
    with path.open(encoding="utf-8") as f:
        r = csv.DictReader(f)
        cols = r.fieldnames or []
        url_col = find_url_column(cols)
        for row in r:
            rows.append(row)
    return rows, url_col

def save_csv(rows: List[SubEvent], out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "parent_title","parent_time","parent_location","parent_tags","parent_url",
            "title","time","location","url"
        ])
        for e in rows:
            w.writerow([
                e.parent_title, e.parent_time, e.parent_location, e.parent_tags, e.parent_url,
                e.title, e.time, e.location, e.url
            ])

def load_processed_parent_urls(ck_path: Path) -> Set[str]:
    if not ck_path.exists(): return set()
    seen: Set[str] = set()
    try:
        with ck_path.open(encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                u = nrm(row.get("parent_url",""))
                if u: seen.add(u)
    except Exception:
        pass
    return seen

# ---- Page helpers ----
def wait_subsessions_ready(page, timeout_ms=35000):
    start = time.time()
    swings = 0
    last_h = 0
    while (time.time() - start) * 1000 < timeout_ms:
        # 1) 代表セレクタ
        if page.locator("a.session-sub-title").count() > 0:
            return True
        # 2) ゆるめ
        if page.locator(".session-subs, .session-subs-list").count() > 0:
            if page.locator(".session-subs-list a[href*='/embedded/session/']").count() > 0:
                return True
        # 3) テキストでの見え方
        if page.get_by_text("Subsessions", exact=False).count() > 0 and page.locator("a[href*='/embedded/session/']").count() > 0:
            return True

        # 読み込み促進（上下にスイング）
        try:
            h = page.evaluate("document.body && document.body.scrollHeight || 0") or 0
            if h and h != last_h:
                for y in range(0, h, 1200):
                    page.evaluate(f"window.scrollTo(0,{y})")
                    page.wait_for_timeout(150)
                swings = 0
                last_h = h
            else:
                # 下端→上端を往復して再描画を促す
                if swings % 2 == 0:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                else:
                    page.evaluate("window.scrollTo(0, 0)")
                swings += 1
        except Exception:
            pass
        page.wait_for_timeout(350)
    return False

def extract_subsessions_html(html: str, base_url: str) -> List[Tuple[str,str,str,str]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Tuple[str,str,str,str]] = []
    items = soup.select(".session-subs-list .session-sub")
    if not items:
        # ゆるめのフォールバック
        for a in soup.select("a.session-sub-title"):
            items.append(a.parent or a)

    for it in items:
        a = it.select_one("a.session-sub-title") or it.select_one("a[href*='/embedded/session/']")
        title = nrm(a.get_text()) if a and nrm(a.get_text()) else ""
        url = urljoin(base_url, a["href"]) if a and a.has_attr("href") else ""
        t_el = it.select_one(".session-sub-time, .sub-time, .time")
        time_str = nrm(t_el.get_text()) if t_el else ""
        m = TIME_PAT12.search(time_str);  time_str = m.group(0) if m else time_str
        l_el = it.select_one(".session-sub-location, .sub-location, .location")
        location = nrm(l_el.get_text()) if l_el else ""
        if any([title, time_str, location, url]):
            out.append((title, time_str, location, url))

    # 去重
    uniq, seen = [], set()
    for t,ti,lo,u in out:
        key = (t.lower(), ti.lower(), lo.lower(), u.lower())
        if key in seen: continue
        seen.add(key); uniq.append((t,ti,lo,u))
    return uniq

# ---- Main ----
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_csv", default="kdd2025_events.csv")
    ap.add_argument("--out", dest="out_csv", default="kdd2025_subsessions.csv")
    ap.add_argument("--max-rps", type=float, default=0.4)
    ap.add_argument("--jitter-ms", nargs=2, type=int, default=[400,1200])
    ap.add_argument("--checkpoint-every", type=int, default=20)
    ap.add_argument("--rotate-every", type=int, default=30)
    ap.add_argument("--ua-list", default="")
    ap.add_argument("--proxy-list", default="")
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--list-targets", action="store_true", help="対象の親URL一覧を表示して終了")
    args = ap.parse_args()

    in_path = Path(args.in_csv)
    out_path = Path(args.out_csv)
    ck_path = out_path.with_suffix(".partial.csv")

    if not in_path.exists():
        print(f"[FATAL] input CSV not found: {in_path}", flush=True); sys.exit(1)

    parents, url_col = read_parent_events(in_path)
    if not url_col:
        print(f"[FATAL] URL column not found in CSV headers. headers={list(parents[0].keys()) if parents else []}", flush=True); sys.exit(1)

    # 対象抽出
    targets = []
    for p in parents:
        u = nrm(p.get(url_col,""))
        if WHOVA_SESSION.search(u):
            targets.append(p)

    print(f"[LOG] rows_in={len(parents)} | session_pages={len(targets)} | url_col='{url_col}'", flush=True)

    # 0件なら理由説明
    if len(targets) == 0:
        sample_urls = [nrm(p.get(url_col,"")) for p in parents[:10]]
        print("[HINT] No 'embedded/session' URLs found.", flush=True)
        print("       Sample URLs from CSV:", *sample_urls, sep="\n       ")
        print("       → 親CSVのURLが 'embedded/event/...' になっていないか確認してください。", flush=True)
        return

    if args.list_targets:
        print("[TARGETS] first 20:", flush=True)
        for i, p in enumerate(targets[:20]):
            print(f"  {i+1:>2}: {nrm(p.get(url_col,''))}", flush=True)
        return

    # 既処理親URL（partial）
    processed_parents = load_processed_parent_urls(ck_path)

    # UA/Proxy
    def load_lines(p):
        if not p: return []
        path = Path(p)
        if not path.exists(): return []
        return [nrm(x) for x in path.read_text(encoding="utf-8").splitlines() if nrm(x)]
    uas = load_lines(args.ua_list) or [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    ]
    proxies = load_lines(args.proxy_list)
    ua_i = pr_i = 0
    def next_rr(seq, idx):
        if not seq: return None, idx
        return seq[idx % len(seq)], idx + 1

    rate = RateLimiter(args.max_rps, tuple(args.jitter_ms))
    backoff = Backoff()

    results: List[SubEvent] = []
    # partialの内容は続きでそのまま活用
    if ck_path.exists():
        try:
            with ck_path.open(encoding="utf-8") as f:
                r = csv.DictReader(f)
                for row in r:
                    results.append(SubEvent(
                        parent_title=row.get("parent_title",""),
                        parent_time=row.get("parent_time",""),
                        parent_location=row.get("parent_location",""),
                        parent_tags=row.get("parent_tags",""),
                        parent_url=row.get("parent_url",""),
                        title=row.get("title",""),
                        time=row.get("time",""),
                        location=row.get("location",""),
                        url=row.get("url",""),
                    ))
        except Exception:
            pass

    with sync_playwright() as p:
        def new_context():
            nonlocal ua_i, pr_i
            ua, ua_i = next_rr(uas, ua_i)
            pr, pr_i = next_rr(proxies, pr_i)
            print(f"[CTX] new | UA={ua[:30]}... | proxy={pr or 'none'}", flush=True)
            if pr:
                browser = p.chromium.launch(headless=not args.headful, proxy={"server": pr})
            else:
                browser = p.chromium.launch(headless=not args.headful)
            context = browser.new_context(
                user_agent=ua,
                viewport={"width": random.choice([1366,1440,1600]), "height": random.choice([900,1000,1050])},
                extra_http_headers={"Accept-Language": random.choice(["en-US,en;q=0.9","en-GB,en;q=0.9"])},
            )
            page = context.new_page()
            return browser, context, page

        browser, context, page = new_context()
        processed_count = 0

        try:
            for idx, row in enumerate(targets):
                parent_url = nrm(row.get(url_col,""))
                if parent_url in processed_parents:
                    continue

                # ローテーション
                if args.rotate_every > 0 and processed_count > 0 and processed_count % args.rotate_every == 0:
                    try: context.close(); browser.close()
                    except Exception: pass
                    browser, context, page = new_context()

                parent_title = nrm(row.get("title","") or row.get("Title",""))
                parent_time  = nrm(row.get("time","")  or row.get("Time",""))
                parent_location = nrm(row.get("location","") or row.get("Location",""))
                parent_tags  = nrm(row.get("tags","")  or row.get("Tags",""))

                print(f"[OPEN] {idx+1}/{len(targets)} {parent_title[:60]}…", flush=True)
                try:
                    page.goto(parent_url, wait_until="domcontentloaded", timeout=60_000)
                except PWError as e:
                    print(f"[WARN] goto failed: {e}", flush=True)
                    backoff.sleep("goto failed"); rate.wait(); continue

                rate.wait()
                ok = wait_subsessions_ready(page, timeout_ms=35_000)

                # ここを「okでなくても一応パースしてみる」に変更
                try:
                    html = page.content()
                except Exception:
                    html = ""

                subs = extract_subsessions_html(html, base_url=parent_url)

                if not subs:
                    # うまく取れなかったらデバッグを残す（1件目だけでも）
                    from datetime import datetime
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    Path("debug_out").mkdir(exist_ok=True)
                    try: page.screenshot(path=f"debug_out/subs_fail_{ts}.png", full_page=True)
                    except Exception: pass
                    try: Path(f"debug_out/subs_fail_{ts}.html").write_text(html or "", encoding="utf-8")
                    except Exception: pass
                    print("[WARN] subs not parsed; dumped debug_out/*.html", flush=True)
                else:
                    for (stitle, stime, sloc, surl) in subs:
                        results.append(SubEvent(
                            parent_title=parent_title, parent_time=parent_time,
                            parent_location=parent_location, parent_tags=parent_tags,
                            parent_url=parent_url,
                            title=stitle, time=stime, location=sloc, url=surl
                        ))

                processed_count += 1

                if processed_count % 5 == 0 or processed_count == len(targets):
                    print(f"[PROG] processed={processed_count}/{len(targets)} | rows={len(results)}", flush=True)

                if args.checkpoint_every > 0 and processed_count % args.checkpoint_every == 0:
                    save_csv(results, ck_path)
                    print(f"[CKPT] -> {ck_path.name} ({len(results)} rows)", flush=True)

                rate.wait()

        finally:
            try:
                context.close(); browser.close()
            except Exception:
                pass

    # 去重（parent_url + sub title + time）
    uniq: List[SubEvent] = []
    seen: Set[Tuple[str,str,str]] = set()
    for e in results:
        key = (e.parent_url, e.title.lower(), e.time.lower())
        if key in seen: continue
        seen.add(key); uniq.append(e)

    save_csv(uniq, out_path)
    print(f"[OK] Saved {len(uniq)} subsessions -> {out_path}", flush=True)

if __name__ == "__main__":
    main()
