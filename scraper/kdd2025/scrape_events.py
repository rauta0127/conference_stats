#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
KDD 2025 Schedule at a Glance（Whova埋め込み）からイベントを収集しCSV保存。

抽出フィールド:
  - title: str
  - time: str
  - location: str
  - tags: List[str]（;区切りでCSV保存）
  - url: str

設計メモ:
  - WhovaはJS後描画 & iframe内なのでPlaywrightが必要
  - 画面内スクロールで追加ロードされることがあるため、複数回スクロール
  - セレクタは変更に強いように「タイトル見出し」「時間表記らしきテキスト」
    「Location:」「タグっぽいchip」などをヒューリスティックに抽出
"""

import argparse
import csv
import re
import sys
from dataclasses import dataclass, asdict
from typing import List, Optional
from urllib.parse import urljoin

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
    r"(?i)\b(?:mon|tue|wed|thu|fri|sat|sun|aug|sept|sep|oct|nov|dec|[a-z]{3}\.)?[^0-9a-z]{0,3}"
    r"(\d{1,2}:\d{2}\s?(?:AM|PM))\s?[-–~—]\s?(\d{1,2}:\d{2}\s?(?:AM|PM))"
)

LOCATION_PAT = re.compile(r"(?i)\b(location|room|hall|venue)\s*:?\s*(.+)$")
TAG_CLASS_HINTS = ("tag", "chip", "label", "badge", "category", "pill")


def normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def to_abs(base: str, href: Optional[str]) -> str:
    if not href:
        return ""
    return urljoin(base, href)


def wait_for_whova_frame(page, timeout_ms: int):
    # Whovaのiframeがロードされるのを待つ
    # srcに "whova" を含むiframeを狙う
    page.wait_for_timeout(1000)
    frames = page.frames
    whova_frames = [f for f in frames if "whova" in (f.url or "").lower()]
    if whova_frames:
        return whova_frames[0]

    # まだなら少し待って再取得
    page.wait_for_timeout(1500)
    frames = page.frames
    whova_frames = [f for f in frames if "whova" in (f.url or "").lower()]
    if whova_frames:
        return whova_frames[0]

    # セレクタで待つ（念のため）
    try:
        page.frame_locator("iframe[src*='whova']").first.wait_for(timeout=timeout_ms)
    except PWTimeout:
        pass

    # 最後に再収集
    for _ in range(5):
        frames = page.frames
        whova_frames = [f for f in frames if "whova" in (f.url or "").lower()]
        if whova_frames:
            return whova_frames[0]
        page.wait_for_timeout(1000)

    return None


def auto_scroll_frame(frame, max_rounds=15, step_px=1200, pause_ms=800):
    # 無限スクロール系の読み込みを想定し、下まで何度かスクロール
    last_h = 0
    same_count = 0
    for _ in range(max_rounds):
        try:
            h = frame.evaluate("document.body ? document.body.scrollHeight : 0")
        except Exception:
            break
        if h == 0:
            break

        # 画面最下部へ
        for y in range(0, h, step_px):
            try:
                frame.evaluate(f"window.scrollTo(0, {y});")
            except Exception:
                pass
            frame.page.wait_for_timeout(pause_ms)

        if h == last_h:
            same_count += 1
        else:
            same_count = 0
        last_h = h
        if same_count >= 2:
            break


def find_candidate_cards(html: str):
    """
    なるべく構造に依存しすぎないカード抽出。
    - listitem/section/article/divなど「まとまり」を候補に
    - 見出し(h1/h2/h3/a strong)や時間表記、Location, タグっぽい要素を持つもの
    """
    soup = BeautifulSoup(html, "html.parser")

    candidates = []
    # よくある包み要素
    wrappers = soup.select("article, section, li, div")
    for w in wrappers:
        # タイトル候補
        title_el = None
        for sel in ["h1", "h2", "h3", "a[title]", "a strong", "strong"]:
            el = w.select_one(sel)
            if el and normalize_ws(el.get_text()):
                title_el = el
                break

        # 時間候補
        text = normalize_ws(w.get_text(" "))
        has_time = bool(TIME_PAT.search(text))
        has_location = bool(LOCATION_PAT.search(text))
        has_link = w.select_one("a[href]") is not None

        # タイトル or 時間 or 場所があり、リンクもあるものを候補に
        if (title_el or has_time or has_location) and has_link:
            candidates.append(w)

    # 重複除去（idやテキスト長）
