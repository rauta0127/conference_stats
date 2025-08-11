import argparse
import csv
import sys
from dataclasses import dataclass
from typing import List, Optional, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class Event:
    title: str
    time: str
    location: str
    tags: List[str]
    url: str


def make_session() -> requests.Session:
    """リトライ付きの requests.Session を作成"""
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")})
    return session


def text_or_attr(a_tag) -> Optional[str]:
    """リンクの見出し候補（テキスト→title属性→aria-label）の順で取得"""
    txt = (a_tag.get_text(" ", strip=True) or "").strip()
    if txt:
        return txt
    for attr in ("title", "aria-label"):
        val = (a_tag.get(attr) or "").strip()
        if val:
            return val
    return None


def looks_like_schedule_link(href: str) -> bool:
    """スケジュール関連っぽいリンクだけに絞るゆるいフィルタ"""
    href_lower = href.lower()
    # 予定・プログラム系の単語が入っているリンクを優先
    keywords = ["program", "schedule", "workshop", "tutorial", "poster", "keynote", "industry", "doctoral", "research", "events", "agenda", "competition", "challenge", "social"]
    return any(k in href_lower for k in keywords)


def parse_items(html: str, base_url: str) -> List[Item]:
    soup = BeautifulSoup(html, "lxml")

    # 探索候補: WordPressの典型構造や、表/グリッドの可能性
    candidate_selectors = [
        # メイン本文
        "main .entry-content",
        "article .entry-content",
        "div#content",
        "main",
        "article",
        # テーブル/グリッド
        ".table-responsive",
        "table",
        ".schedule",
        ".schedule-table",
        ".grid",
        ".cards",
        # フォールバック: ページ全体
        "body",
    ]

    seen: Set[str] = set()
    items: List[Item] = []

    for sel in candidate_selectors:
        container = soup.select_one(sel)
        if not container:
            continue

        for a in container.find_all("a", href=True):
            href = a["href"].strip()
            if not href or href.startswith("#") or href.lower().startswith("mailto:"):
                continue

            # 相対URL→絶対URL
            full_url = urljoin(base_url, href)

            # スケジュールに関係ないフッターやSNS/外部リンクなどを一部除外
            if any(bad in full_url.lower() for bad in ["facebook.com", "twitter.com", "linkedin.com", "instagram.com"]):
                continue

            # タイトル候補
            title = text_or_attr(a)
            if not title:
                continue

            # スケジュールらしさのフィルタ（強すぎると取り漏れるので緩め）
            if not looks_like_schedule_link(full_url):
                continue

            key = (title, full_url)
            if key in seen:
                continue
            seen.add(key)
            items.append(Item(title=title, url=full_url))

        # ある程度集まったら終了（フォールバックに行き過ぎないように）
        if len(items) >= 20:
            break

    # タイトルでソートして安定化
    items.sort(key=lambda x: (x.title.lower(), x.url))
    return items


def save_csv(items: List[Item], out_path: str) -> None:
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "url"])
        for it in items:
            writer.writerow([it.title, it.url])


def main():
    parser = argparse.ArgumentParser(description="Scrape KDD 2025 Schedule links")
    parser.add_argument(
        "--url",
        default="https://kdd2025.kdd.org/schedule-at-a-glance/",
        help="Target URL (default: KDD 2025 Schedule at a Glance)",
    )
    parser.add_argument(
        "--out",
        default="kdd2025_schedule_links.csv",
        help="Output CSV path",
    )
    args = parser.parse_args()

    session = make_session()
    try:
        resp = session.get(args.url, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[ERROR] Failed to fetch: {e}", file=sys.stderr)
        sys.exit(1)

    items = parse_items(resp.text, base_url=args.url)

    if not items:
        print("[WARN] 取得できたリンクが0件でした。ページ構造の変更やJavaScriptレンダリングが必要な可能性があります。")

    save_csv(items, args.out)
    print(f"[OK] Saved {len(items)} items to {args.out}")


if __name__ == "__main__":
    main()
