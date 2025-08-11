#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import csv
import re
import time
import random
from dataclasses import dataclass
from pathlib import Path
from typing import List
from typing import Optional
from typing import Tuple
from typing import Set
from urllib.parse import urljoin
from urllib.parse import urlparse
from datetime import datetime
from playwright.sync_api import sync_playwright
from playwright.sync_api import Error as PWError


# ---------- Data model ----------
@dataclass
class Event:
    title: str
    time: str
    location: str
    tags: List[str]
    url: str


def nrm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


TIME_PAT = re.compile(r"(?i)\b\d{1,2}:\d{2}\s?(?:am|pm)\s?[-–—]\s?\d{1,2}:\d{2}\s?(?:am|pm)\b")


# ---------- Rate limiter & Backoff ----------
class RateLimiter:
    def __init__(self, max_rps: float, jitter_ms: Tuple[int, int]):
        self.interval = 1.0 / max(0.01, max_rps)
        self.jmin, self.jmax = jitter_ms
        self._last = 0.0

    def wait(self):
        now = time.time()
        delta = self.interval - (now - self._last)
        if delta > 0:
            time.sleep(delta)
        # jitter
        if self.jmax > 0:
            time.sleep(random.uniform(self.jmin / 1000.0, self.jmax / 1000.0))
        self._last = time.time()


class Backoff:
    def __init__(self, base=1.0, factor=2.0, cap=60.0):
        self.base, self.factor, self.cap = base, factor, cap
        self.n = 0

    def reset(self):
        self.n = 0

    def sleep(self, note=""):
        t = min(self.cap, self.base * (self.factor**self.n)) * random.uniform(0.8, 1.2)
        self.n += 1
        print(f"[BACKOFF] sleeping ~{t:.1f}s {note}", flush=True)
        time.sleep(t)


# ---------- UA / Proxy rotation ----------
def load_lines(p: Optional[str]) -> List[str]:
    if not p:
        return []
    path = Path(p)
    if not path.exists():
        return []
    return [nrm(x) for x in path.read_text(encoding="utf-8").splitlines() if nrm(x)]


DEFAULT_UAS = [
    # 代表的なChromium系
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
]


def next_round_robin(seq: List[str], idx: int) -> Tuple[Optional[str], int]:
    if not seq:
        return None, idx
    return seq[idx % len(seq)], idx + 1


# ---------- Whova helpers (embed → direct fallback) ----------
def get_whova_frame_or_open_direct(page, first_timeout_ms=30000):
    # try to get iframe
    deadline = time.time() + first_timeout_ms / 1000.0
    whova_iframe = None
    while time.time() < deadline and whova_iframe is None:
        loc = page.locator("iframe[src*='whova']")
        if loc.count() > 0:
            whova_iframe = loc.first
            break
        page.wait_for_timeout(500)

    target_frame = None
    if whova_iframe is not None:
        try:
            target_frame = whova_iframe.content_frame()
        except Exception:
            target_frame = None

    if target_frame is not None:
        try:
            if target_frame.locator("div.session").count() > 0:
                return target_frame, False
        except Exception:
            pass

    # open direct
    try:
        src = page.locator("iframe[src*='whova']").first.get_attribute("src", timeout=5000)
    except Exception:
        src = None
    if src:
        page.goto(src, wait_until="domcontentloaded", timeout=60000)
        return page.main_frame, True

    return page.main_frame, False


def wait_sessions_with_watchdog(frame, page, min_cnt=5, overall_ms=60000, rate: Optional[RateLimiter] = None):
    start = time.time()
    last = -1
    while (time.time() - start) * 1000 < overall_ms:
        try:
            body_text = frame.evaluate("document.body && document.body.innerText ? document.body.innerText : ''")
        except Exception:
            body_text = ""
        if "Loading" in body_text and "Whova" in body_text:
            page.wait_for_timeout(800)
            continue

        try:
            cnt = frame.locator("div.session").count()
        except Exception:
            cnt = 0

        if cnt != last:
            print(f"[LOG]  session count={cnt}", flush=True)
            last = cnt
        if cnt >= min_cnt:
            return cnt

        try:
            frame.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            pass
        if rate:
            rate.wait()
        else:
            page.wait_for_timeout(700)
    return last if last >= 0 else 0


# ---------- Extraction (content-col only for title etc.) ----------
def extract_event_from_session(frame, session_nth, base_url: str) -> Optional[Event]:
    s = frame.locator("div.session").nth(session_nth)

    try:
        s.scroll_into_view_if_needed(timeout=1500)
        frame.page.wait_for_timeout(120)
    except Exception:
        pass

    content = s.locator("div.content-col")
    timecol = s.locator("div.time-col")

    # time
    time_str = ""
    try:
        if timecol.count():
            t = nrm(timecol.first.inner_text(timeout=1000))
            m = TIME_PAT.search(t)
            time_str = m.group(0) if m else t
        if not time_str:
            m = TIME_PAT.search(nrm(s.inner_text(timeout=1000)))
            if m:
                time_str = m.group(0)
    except Exception:
        pass

    # title (content-col only)
    title = ""
    try:
        for sel in [
            "div.session-title-row-left span.session-title",
            "span.session-title",
            ".session-title",
            "h1, h2, h3",
            "a[title]",
            "a strong",
            "strong",
        ]:
            loc = content.locator(sel) if content.count() else s.locator(sel)
            if loc.count() > 0:
                t = nrm(loc.first.inner_text(timeout=1200))
                if t and not TIME_PAT.fullmatch(t):
                    title = t
                    break

        if not title and content.count():
            head = content.get_by_role("heading")
            if head.count() > 0:
                t = nrm(head.first.inner_text(timeout=800))
                if t and not TIME_PAT.search(t):
                    title = t

        if not title:
            txt = content.inner_text(timeout=1500) if content.count() else s.inner_text(timeout=1500)
            lines = [nrm(x) for x in (txt or "").splitlines()]
            cand = [ln for ln in lines if len(ln) >= 6 and not TIME_PAT.search(ln) and not re.match(r"(?i)^(location|room|hall|venue)\s*[:：]", ln) and not ln.lower().startswith("session chair:")]
            if cand:
                title = cand[0]
    except Exception:
        pass

    # location
    location = ""
    try:
        ll = content.locator("div.session-location, .session-location, .location") if content.count() else s.locator("div.session-location, .session-location, .location")
        if ll.count() > 0:
            location = nrm(ll.first.inner_text(timeout=800))
        else:
            txt = nrm(content.inner_text(timeout=800)) if content.count() else nrm(s.inner_text(timeout=800))
            m = re.search(r"(?i)\b(?:Location|Room|Hall|Venue)\s*:\s*(.+)", txt)
            if m:
                location = nrm(m.group(1))
    except Exception:
        pass

    # tags
    tags: List[str] = []
    try:
        chips = (
            content.locator(".session-tracks >> *, [class*='tag'], [class*='chip'], [class*='badge'], [class*='label']")
            if content.count()
            else s.locator(".session-tracks >> *, [class*='tag'], [class*='chip'], [class*='badge'], [class*='label']")
        )
        c = min(chips.count(), 20)
        for i in range(c):
            t = nrm(chips.nth(i).inner_text(timeout=600))
            if not t:
                continue
            if re.search(r"(?i)\b(location|room|hall|venue|time|am|pm)\b", t):
                continue
            if 2 <= len(t) <= 40 and t not in tags:
                tags.append(t)
    except Exception:
        pass

    # url (normal link)
    url = ""
    try:
        a_loc = content.locator("a[href]:has-text('view more detailed information'), a[href]") if content.count() else s.locator("a[href]:has-text('view more detailed information'), a[href]")
        if a_loc.count() > 0:
            href = a_loc.first.get_attribute("href", timeout=800)
            if href:
                tmp = urljoin(base_url, href)
                if urlparse(tmp).scheme:
                    url = tmp
    except Exception:
        pass

    # subsessions fallback (card-scoped)
    if not url:
        try:
            clickable = s.locator("span.session-subs, .session-subs").first
            if clickable.count() > 0:
                old = frame.url
                with frame.expect_navigation(timeout=6000):
                    clickable.click(force=True)
                url = frame.url or ""
                if old:
                    try:
                        frame.goto(old, wait_until="domcontentloaded", timeout=15000)
                    except Exception:
                        pass
                if url and not urlparse(url).scheme:
                    url = ""
        except Exception:
            pass

    if not any([title, time_str, location, url]):
        return None
    if title and TIME_PAT.search(title):
        title = nrm(TIME_PAT.sub("", title)).strip(" -–—")
        if not title:
            return None

    return Event(title=title, time=time_str, location=location, tags=tags, url=url)


# ---------- CSV helpers ----------
def save_csv(events: List[Event], out_path: Path):
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["title", "time", "location", "tags", "url"])
        for e in events:
            w.writerow([e.title, e.time, e.location, ";".join(e.tags), e.url])


# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", default="kdd2025_events.csv")
    ap.add_argument("--timeout", type=int, default=180)
    ap.add_argument("--max-rps", type=float, default=0.6)
    ap.add_argument("--jitter-ms", nargs=2, type=int, default=[200, 800])
    ap.add_argument("--rotate-every", type=int, default=60, help="recreate browser context & rotate UA/proxy every N items")
    ap.add_argument("--checkpoint-every", type=int, default=50)
    ap.add_argument("--ua-list", default="")
    ap.add_argument("--proxy-list", default="")
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_path = Path(args.out)
    ck_path = out_path.with_suffix(".partial.csv")

    uas = load_lines(args.ua_list) or DEFAULT_UAS
    proxies = load_lines(args.proxy_list)
    ua_idx = prx_idx = 0

    rate = RateLimiter(args.max_rps, tuple(args.jitter_ms))
    backoff = Backoff(base=2, factor=2, cap=90)

    # resume support: load existing partial to skip keys
    seen_keys: Set[Tuple[str, str]] = set()
    if ck_path.exists():
        try:
            for row in csv.DictReader(ck_path.open(encoding="utf-8")):
                key = (nrm(row.get("title", "")).lower(), nrm(row.get("time", "")).lower())
                if key != ("", ""):
                    seen_keys.add(key)
        except Exception:
            pass

    with sync_playwright() as p:
        total_collected: List[Event] = []

        def new_context():
            nonlocal ua_idx, prx_idx
            ua, ua_idx = next_round_robin(uas, ua_idx)
            proxy = None
            if proxies:
                proxy, prx_idx = next_round_robin(proxies, prx_idx)
            print(f"[CTX] new context | UA={ua[:30]}... | proxy={proxy or 'none'}", flush=True)
            if proxy:
                browser = p.chromium.launch(headless=not args.headful, proxy={"server": proxy})
            else:
                browser = p.chromium.launch(headless=not args.headful)
            context = browser.new_context(
                viewport={"width": random.choice([1366, 1440, 1600]), "height": random.choice([900, 1000, 1050])},
                user_agent=ua,
                extra_http_headers={"Accept-Language": random.choice(["en-US,en;q=0.9", "en-GB,en;q=0.9"])},
            )
            page = context.new_page()
            return browser, context, page

        browser, context, page = new_context()
        try:
            print(f"[LOG] Open: {args.url}", flush=True)
            page.goto(args.url, wait_until="domcontentloaded", timeout=60_000)
            rate.wait()

            fr, opened_direct = get_whova_frame_or_open_direct(page, first_timeout_ms=30_000)
            print(f"[LOG] Use frame: {fr.url or '(main)'} | opened_direct={opened_direct}", flush=True)

            cnt = wait_sessions_with_watchdog(fr, page, min_cnt=5, overall_ms=min(args.timeout * 1000, 90_000), rate=rate)
            print(f"[LOG] Final session count seen={cnt}", flush=True)
            if cnt <= 0:
                backoff.sleep("[no sessions visible]")

            # iterate sessions
            total = fr.locator("div.session").count()
            print(f"[LOG] Iterate sessions total={total}", flush=True)

            t0 = time.time()
            for i in range(total):
                rate.wait()
                # rotate context periodically (be gentle)
                if args.rotate_every > 0 and i > 0 and i % args.rotate_every == 0:
                    try:
                        context.close()
                        browser.close()
                    except Exception:
                        pass
                    browser, context, page = new_context()
                    page.goto(args.url, wait_until="domcontentloaded", timeout=60_000)
                    rate.wait()
                    fr, opened_direct = get_whova_frame_or_open_direct(page, first_timeout_ms=15_000)
                    wait_sessions_with_watchdog(fr, page, min_cnt=5, overall_ms=20_000, rate=rate)

                try:
                    ev = extract_event_from_session(fr, i, base_url=args.url)
                except PWError as e:
                    print(f"[WARN] extract failed at {i}: {e}", flush=True)
                    backoff.sleep()
                    continue

                if ev:
                    key = (nrm(ev.title).lower(), nrm(ev.time).lower())
                    if key not in seen_keys:
                        total_collected.append(ev)
                        seen_keys.add(key)

                # progress
                if (i + 1) % 10 == 0 or (i + 1) == total:
                    elapsed = time.time() - t0
                    per = elapsed / max(1, (i + 1))
                    eta = per * (total - (i + 1))
                    last = (total_collected[-1].title[:60] + "…") if total_collected else "-"
                    print(f"[PROG] {i + 1}/{total} | kept={len(total_collected)} | {per:.2f}s/it | ETA~{eta:.1f}s | last='{last}'", flush=True)

                # checkpoint
                if args.checkpoint_every > 0 and (i + 1) % args.checkpoint_every == 0:
                    save_csv(total_collected, ck_path)
                    print(f"[CKPT] saved -> {ck_path.name} ({len(total_collected)} rows)", flush=True)

            # write final
            save_csv(total_collected, out_path)
            print(f"[OK] Saved {len(total_collected)} events -> {out_path}", flush=True)

            # debug dump
            if args.debug:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                try:
                    page.screenshot(path=str(Path("debug_out") / f"shot_{ts}.png"), full_page=True)
                except Exception:
                    pass
                try:
                    Path("debug_out").mkdir(exist_ok=True)
                except Exception:
                    pass
                try:
                    Path("debug_out", f"dump_{ts}.html").write_text(fr.content(), encoding="utf-8")
                except Exception:
                    pass

        finally:
            try:
                context.close()
                browser.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
