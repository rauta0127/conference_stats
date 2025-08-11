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


# 12時間表記（en dash/em dash/ハイフン対応）
TIME_PAT = re.compile(r"(?i)\b\d{1,2}:\d{2}\s?(?:am|pm)\s?[-–—]\s?\d{1,2}:\d{2}\s?(?:am|pm)\b")


def nrm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def to_abs(base: str, href: Optional[str]) -> str:
    if not href:
        return ""
    return urljoin(base, href)


def log(m):
    print(f"[LOG] {m}", flush=True)


def pick_whova_frame(page):
    for _ in range(10):
        for f in page.frames:
            u = (f.url or "").lower()
            if "whova.com" in u or "embedded/event" in u:
                return f
        page.wait_for_timeout(800)
    return page.main_frame


# 1) フレーム拾い + 直開きフォールバック
def get_whova_frame_or_open_direct(page, first_timeout_ms=30000):
    """KDDページでwhova iframeを探し、見つかったらそのframeを返す。
    30s待っても div.session が出ない場合は、iframeのsrcを取得して top-level に直接 goto。
    直開き後は page.main_frame を返す。
    """
    # まず iframe を待つ
    deadline = time.time() + (first_timeout_ms / 1000)
    whova_iframe = None
    while time.time() < deadline and whova_iframe is None:
        for el in page.locator("iframe[src*='whova']").all():
            whova_iframe = el
            break
        if whova_iframe is None:
            page.wait_for_timeout(500)

    # iframeが見つからない→とりあえずメインフレーム
    target_frame = None
    if whova_iframe is not None:
        try:
            target_frame = whova_iframe.content_frame()
        except Exception:
            target_frame = None

    # そのまま試し、div.sessionが出ればOK
    if target_frame is not None:
        try:
            cnt = target_frame.locator("div.session").count()
            if cnt and cnt > 0:
                return target_frame, False  # False=直開きじゃない
        except Exception:
            pass

    # --- 直開きフォールバック ---
    try:
        src = page.locator("iframe[src*='whova']").first.get_attribute("src", timeout=5000)
    except Exception:
        src = None
    if src:
        page.goto(src, wait_until="domcontentloaded", timeout=60000)
        # 直開き時はメインフレームでそのまま扱う
        return page.main_frame, True

    # 最後の手段：メインフレームを返す
    return page.main_frame, False


# 2) 待機をリトライ + “Loading…” 監視 + リロード
def wait_sessions_with_watchdog(frame, page, min_cnt=5, overall_ms=60000, reload_tries=2):
    """div.sessionが min_cnt 出るまでポーリング。出なければ reload を数回。"""
    tries = 0
    while tries <= reload_tries:
        start = time.time()
        last = -1
        while (time.time() - start) * 1000 < overall_ms:
            try:
                # “Loading” だけで埋まっていないかもチェック
                body_txt = frame.evaluate("document.body && document.body.innerText ? document.body.innerText : ''")
            except Exception:
                body_txt = ""
            if "Loading" in body_txt and "Powered by Whova" in body_txt:
                page.wait_for_timeout(800)
                continue

            try:
                cnt = frame.locator("div.session").count()
            except Exception:
                cnt = 0

            if cnt != last:
                log(f"  session count={cnt}")
                last = cnt
            if cnt >= min_cnt:
                return cnt

            # 読み込み促進
            try:
                frame.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except Exception:
                pass
            page.wait_for_timeout(700)

        # ここまでで出なければ reload
        tries += 1
        log(f"  watchdog: reload #{tries}")
        page.reload(wait_until="domcontentloaded", timeout=60000)
        # 直開きをもう一度試す（埋め込み→直開きの順）
        fr, opened_direct = get_whova_frame_or_open_direct(page, first_timeout_ms=5000)
        frame = fr
    return 0


# --- Subsessionsクリック: カード内に限定してURLを取る ---
def get_url_from_click(frame, card_locator, wait_ms: int = 6000) -> str:
    try:
        clickable = card_locator.locator("span.session-subs, .session-subs").first
        if clickable.count() == 0:
            return ""
        old_url = frame.url
        with frame.expect_navigation(timeout=wait_ms):
            clickable.click(force=True)
        new_url = frame.url or ""
        try:
            if old_url:
                frame.goto(old_url, wait_until="domcontentloaded", timeout=15000)
        except Exception:
            pass
        return new_url if (new_url and urlparse(new_url).scheme) else ""
    except (PWTimeout, PWError):
        return ""


# --- 1件抽出（タイトル=content-col限定 / 時間=time-col限定 / 進捗ログ用名寄せ） ---
def extract_one(frame, session_nth, base_url: str) -> Optional[Event]:
    s = frame.locator("div.session").nth(session_nth)

    # 可視化してから抽出（未描画対策）
    try:
        s.scroll_into_view_if_needed(timeout=1500)
        frame.page.wait_for_timeout(120)  # レイアウト安定
    except Exception:
        pass

    content = s.locator("div.content-col")
    timecol = s.locator("div.time-col")

    # --- Time（左列優先） ---
    time_str = ""
    try:
        if timecol.count():
            t = nrm(timecol.first.inner_text(timeout=800))
            m = TIME_PAT.search(t)
            time_str = m.group(0) if m else t
        if not time_str:
            m = TIME_PAT.search(nrm(s.inner_text(timeout=800)))
            if m:
                time_str = m.group(0)
    except Exception:
        pass

    # --- Title（右列に限定して探索） ---
    title = ""
    try:
        sel_list = [
            "div.session-title-row-left span.session-title",
            "span.session-title",
            ".session-title",
            "h1, h2, h3",
            "a[title]",
            "a strong",
            "strong",
        ]
        for sel in sel_list:
            loc = content.locator(sel) if content.count() else s.locator(sel)
            if loc.count() > 0:
                t = nrm(loc.first.inner_text(timeout=1200))
                if t and not TIME_PAT.fullmatch(t):
                    title = t
                    break

        # 見出しロール（右列内）
        if not title and content.count():
            try:
                head = content.get_by_role("heading")
                if head.count() > 0:
                    t = nrm(head.first.inner_text(timeout=800))
                    if t and not TIME_PAT.search(t):
                        title = t
            except Exception:
                pass

        # content-col のテキストから先頭の“タイトルらしい”行だけを採用
        if not title:
            txt = ""
            if content.count():
                try:
                    txt = content.inner_text(timeout=1500)
                except Exception:
                    txt = ""
            else:
                # 最悪時は全体から time-col のテキストを除いて使う
                txt_all = nrm(s.inner_text(timeout=1500))
                txt_time = nrm(timecol.inner_text(timeout=800)) if timecol.count() else ""
                txt = "\n".join([ln for ln in txt_all.splitlines() if nrm(ln) != txt_time])

            lines = [nrm(x) for x in (txt or "").splitlines()]
            cand = [
                ln
                for ln in lines
                if len(ln) >= 6
                and not TIME_PAT.search(ln)  # 時間行は除外
                and not re.match(r"(?i)^(location|room|hall|venue)\s*[:：]", ln)
                and not ln.lower().startswith("session chair:")
            ]
            if cand:
                title = cand[0]
    except Exception:
        pass

    # --- Location（右列内） ---
    location = ""
    try:
        ll = content.locator("div.session-location, .session-location, .location") if content.count() else s.locator("div.session-location, .session-location, .location")
        if ll.count() > 0:
            location = nrm(ll.first.inner_text(timeout=800))
        else:
            base_txt = nrm(content.inner_text(timeout=800)) if content.count() else nrm(s.inner_text(timeout=800))
            m = re.search(r"(?i)\b(?:Location|Room|Hall|Venue)\s*:\s*(.+)", base_txt)
            if m:
                location = nrm(m.group(1))
    except Exception:
        pass

    # --- Tags（右列内） ---
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

    # --- URL（通常リンク → Subsessions クリック） ---
    url = ""
    try:
        a_loc = content.locator("a[href]:has-text('view more detailed information'), a[href]") if content.count() else s.locator("a[href]:has-text('view more detailed information'), a[href]")
        if a_loc.count() > 0:
            href = a_loc.first.get_attribute("href", timeout=800)
            if href:
                tmp = to_abs(base_url, href)
                if urlparse(tmp).scheme:
                    url = tmp
    except Exception:
        pass

    if not url:
        url = get_url_from_click(frame, s, wait_ms=8000)

    # 空カードは捨てる
    if not any([title, time_str, location, url]):
        return None
    # タイトルがまだ時間っぽい場合は捨てておく（品質担保）
    if title and TIME_PAT.search(title):
        # 最後の抵抗：タイトルから時間部分を除去
        title = nrm(TIME_PAT.sub("", title)).strip(" -–—")
        if not title:
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
    ap.add_argument("--log-every", type=int, default=10)
    ap.add_argument("--checkpoint-every", type=int, default=50)
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
        # 3) main() の先頭付近（ページ到達後）を置き換え
        page.goto(args.url, wait_until="domcontentloaded", timeout=60_000)

        # 埋め込み or 直開きのフレームを取得
        fr, opened_direct = get_whova_frame_or_open_direct(page, first_timeout_ms=30_000)
        log(f"Use frame: {fr.url or '(main)'}  | opened_direct={opened_direct}")

        # div.session が現れるまで watchdog で待つ
        cnt = wait_sessions_with_watchdog(fr, page, min_cnt=5, overall_ms=min(args.timeout * 1000, 90_000), reload_tries=2)
        log(f"Final session count seen={cnt}")

        events: List[Event] = []
        try:
            total = fr.locator("div.session").count()
            log(f"Iterate sessions total={total}")
            t0 = time.time()
            for i in range(total):
                try:
                    ev = extract_one(fr, i, base_url=args.url)
                    if ev:
                        events.append(ev)
                except PWError:
                    continue

                if (i + 1) % max(1, args.log_every) == 0 or (i + 1) == total:
                    elapsed = time.time() - t0
                    per = elapsed / (i + 1)
                    eta = per * (total - (i + 1))
                    sample = (events[-1].title[:60] + "…") if events else "-"
                    log(f"  progress {i + 1}/{total} | extracted={len(events)} | {per:.2f}s/it | ETA ~{eta:.1f}s | last='{sample}'")

                if args.checkpoint_every > 0 and (i + 1) % args.checkpoint_every == 0:
                    ck = Path(args.out).with_suffix(".partial.csv")
                    save_csv(events, ck.as_posix())
                    log(f"  checkpoint saved -> {ck.name} ({len(events)} rows)")

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

        if args.debug:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            try:
                page.screenshot(path=str(dbgdir / f"shot_{ts}.png"), full_page=True)
            except Exception:
                pass
            try:
                Path(dbgdir / f"dump_{ts}.html").write_text(fr.content(), encoding="utf-8")
            except Exception:
                pass

        context.close()
        browser.close()


if __name__ == "__main__":
    main()
