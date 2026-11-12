#!/usr/bin/env python3
"""Collect public GST updates from official Indian Government websites and sync to Firebase RTDB.

This collector respects ordinary HTTP responses. It does not bypass access controls,
CAPTCHAs, logins, or anti-bot restrictions. A source that returns 403/429 is recorded
as unavailable for that run; other official sources continue to sync.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
import firebase_admin
from firebase_admin import credentials, db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("gst-news-sync")

DATABASE_URL_DEFAULT = "https://skc-dashboard-3eaca-default-rtdb.asia-southeast1.firebasedatabase.app"
MAX_STORED_ITEMS = 700
RETENTION_DAYS = 730
REQUEST_TIMEOUT = 35

SOURCES: list[dict[str, str]] = [
    {
        "key": "gstn",
        "name": "GST Portal (GSTN)",
        "url": "https://www.gst.gov.in/newsandupdates",
        "domain": "gst.gov.in",
    },
    {
        "key": "gstn_advisories",
        "name": "GST Portal – Advisories & Releases",
        "url": "https://services.gst.gov.in/services/advisory/advisoryandreleases",
        "domain": "gst.gov.in",
    },
    {
        "key": "cbic_tip",
        "name": "CBIC Tax Information Portal",
        "url": "https://taxinformation.cbic.gov.in/",
        "domain": "taxinformation.cbic.gov.in",
    },
    {
        "key": "cbic",
        "name": "CBIC",
        "url": "https://www.cbic.gov.in/",
        "domain": "cbic.gov.in",
    },
    {
        "key": "gstcouncil",
        "name": "GST Council",
        "url": "https://www.gstcouncil.gov.in/en/what-s-new",
        "domain": "gstcouncil.gov.in",
    },
    {
        "key": "gstcouncil_press",
        "name": "GST Council Press Releases",
        "url": "https://www.gstcouncil.gov.in/en/press-release",
        "domain": "gstcouncil.gov.in",
    },
    {
        "key": "dggi",
        "name": "DGGI",
        "url": "https://dggi.cbic.gov.in/dggi-ui/news",
        "domain": "dggi.cbic.gov.in",
    },
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SKConsultantGSTNews/1.0; +https://github.com/FAIZAN/skc-dashboard)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
}

DATE_PATTERNS = [
    re.compile(r"\b\d{1,2}[./-]\d{1,2}[./-]\d{4}\b"),
    re.compile(r"\b\d{4}-\d{1,2}-\d{1,2}\b"),
    re.compile(r"\b\d{1,2}[-./ ]+[A-Za-z]{3,9}[-./ ,]+\d{4}\b", re.I),
    re.compile(r"\b\d{1,2}\s+[A-Za-z]{3,9}\s*,?\s*\d{4}\b", re.I),
    re.compile(r"\b[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}\b", re.I),
]

SKIP_TITLE = re.compile(
    r"^(home|home page|about us|contact us|login|log in|register|sitemap|privacy policy|"
    r"terms and conditions|read more|view all|view details|click here|search|menu|skip to content)$",
    re.I,
)
SKIP_UNRELATED = re.compile(r"\b(vacancy|recruitment|job opening|empanelment|career opportunity|walk.?in interview)\b", re.I)
GST_TERMS = re.compile(
    r"\b(GST|GSTN|CGST|SGST|IGST|UTGST|GSTR|goods and services tax|central tax|integrated tax|"
    r"input tax credit|e-?invoice|e-?way bill|composition levy|GST Council)\b", re.I
)


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def approved_url(candidate: str, base_url: str, allowed_domain: str) -> str | None:
    url = urljoin(base_url, candidate)
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https":
        return None
    if not (host == allowed_domain or host.endswith("." + allowed_domain)):
        return None
    if parsed.path.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".svg", ".css", ".js")):
        return None
    if parsed.path in ("", "/"):
        return None
    return parsed._replace(fragment="").geturl()


def parse_publication_date(text: str) -> str:
    text = clean_text(text)
    for pattern in DATE_PATTERNS:
        match = pattern.search(text)
        if match:
            try:
                parsed = date_parser.parse(match.group(0), dayfirst=True, fuzzy=False)
                if 2000 <= parsed.year <= datetime.now(timezone.utc).year + 1:
                    return parsed.date().isoformat()
            except (ValueError, OverflowError, TypeError):
                continue
    return ""


def extract_context(anchor) -> str:
    candidates = []
    for tag_name in ("tr", "li", "article", "section"):
        parent = anchor.find_parent(tag_name)
        if parent:
            candidates.append(clean_text(parent.get_text(" ", strip=True)))
    parent = anchor.parent
    depth = 0
    while parent is not None and depth < 4:
        txt = clean_text(parent.get_text(" ", strip=True))
        if len(txt) >= 45:
            candidates.append(txt)
        parent = parent.parent
        depth += 1
    for candidate in candidates:
        if len(candidate) >= 35 and len(candidate) <= 1800:
            return candidate
    return clean_text(anchor.get_text(" ", strip=True))


def category_for(source_key: str, title: str, context: str) -> str:
    text = f"{title} {context}".lower()
    if source_key in ("gstn", "gstn_advisories"):
        return "GSTN Advisory"
    if source_key == "cbic_tip":
        if "circular" in text:
            return "CBIC Circular"
        return "CBIC Notification"
    if source_key.startswith("gstcouncil"):
        return "GST Council Update"
    if source_key == "dggi":
        return "DGGI Enforcement"
    if source_key == "cbic":
        return "CBIC Press Release"
    return "Other GST Update"


def is_relevant(source_key: str, title: str, context: str, href: str) -> bool:
    blob = f"{title} {context} {href}"
    if SKIP_TITLE.match(title) or SKIP_UNRELATED.search(title):
        return False
    if source_key == "gstn":
        # GST Portal news items are published under this stable detail path.
        return "/newsandupdates/read/" in href.lower() and len(title) >= 12
    if source_key == "gstn_advisories":
        # GSTN Advisories & Releases detail pages use /read/<numeric-id>.
        # Only accept links from this official listing that resolve to its detail path.
        return "/services/advisoryandreleases/read/" in href.lower() and len(title) >= 12
    if source_key == "cbic_tip":
        # Keep only actual GST/central-tax notification or circular entries, not portal navigation.
        has_doc = "/view-pdf/" in href.lower() or "notification" in title.lower() or "circular" in title.lower()
        return has_doc and bool(GST_TERMS.search(blob)) and len(title) >= 12
    if source_key.startswith("gstcouncil"):
        return bool(GST_TERMS.search(blob) or source_key.startswith("gstcouncil")) and len(title) >= 12
    if source_key == "dggi":
        return bool(GST_TERMS.search(blob)) and len(title) >= 16
    if source_key == "cbic":
        return bool(GST_TERMS.search(blob)) and len(title) >= 16
    return bool(GST_TERMS.search(blob)) and len(title) >= 16


def summarize_context(title: str, context: str) -> str:
    summary = clean_text(context)
    if title:
        summary = re.sub(re.escape(title), "", summary, count=1, flags=re.I).strip(" -–—:|·")
    summary = re.sub(r"\b(?:View PDF|Read More|Read more|Download)\b", "", summary, flags=re.I)
    summary = clean_text(summary)
    if len(summary) > 420:
        summary = summary[:417].rsplit(" ", 1)[0] + "…"
    return summary


def collect_from_source(source: dict[str, str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    session = requests.Session()
    response = session.get(source["url"], headers=HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    content_type = response.headers.get("Content-Type", "")
    if "html" not in content_type.lower() and "text" not in content_type.lower():
        raise RuntimeError(f"Unexpected content type: {content_type}")

    soup = BeautifulSoup(response.text, "html.parser")
    candidates: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    # Rows are preferred because they usually contain the title and date together.
    anchors = soup.find_all("a", href=True)
    for anchor in anchors:
        title = clean_text(anchor.get_text(" ", strip=True) or anchor.get("title", ""))
        if len(title) > 320:
            title = title[:317].rsplit(" ", 1)[0] + "…"
        href = approved_url(anchor.get("href", ""), source["url"], source["domain"])
        if not href or not title:
            continue
        context = extract_context(anchor)
        if not is_relevant(source["key"], title, context, href):
            continue
        if href in seen_urls:
            continue
        seen_urls.add(href)

        published_date = parse_publication_date(context) or parse_publication_date(title)
        # Honour machine-readable time elements when a row does not show a plain date.
        if not published_date:
            time_el = anchor.find_parent().find("time") if anchor.find_parent() else None
            if time_el:
                raw_date = time_el.get("datetime") or time_el.get_text(" ", strip=True)
                try:
                    published_date = date_parser.parse(raw_date, dayfirst=True).date().isoformat()
                except (ValueError, TypeError, OverflowError):
                    pass

        published_at = 0
        if published_date:
            parsed = datetime.fromisoformat(published_date).replace(tzinfo=timezone.utc)
            published_at = int(parsed.timestamp() * 1000)

        item_id = hashlib.sha256(href.encode("utf-8")).hexdigest()[:32]
        candidates.append({
            "id": item_id,
            "title": title,
            "summary": summarize_context(title, context),
            "url": href,
            "sourceKey": source["key"],
            "source": source["name"],
            "category": category_for(source["key"], title, context),
            "publishedDate": published_date,
            "publishedAt": published_at,
        })

    # Keep the most relevant links. The official pages generally list newest items first.
    candidates = candidates[:160]
    result = {
        "status": "ok",
        "httpStatus": response.status_code,
        "itemsFound": len(candidates),
        "checkedAt": datetime.now(timezone.utc).isoformat(),
        "page": source["url"],
    }
    LOG.info("%s: %d GST update(s)", source["name"], len(candidates))
    return candidates, result


def init_firebase():
    raw_service_account = os.environ.get("FIREBASE_SERVICE_ACCOUNT", "").strip()
    if not raw_service_account:
        raise RuntimeError("Missing GitHub Actions secret: FIREBASE_SERVICE_ACCOUNT")
    try:
        service_account = json.loads(raw_service_account)
    except json.JSONDecodeError as exc:
        raise RuntimeError("FIREBASE_SERVICE_ACCOUNT must contain the full service-account JSON") from exc

    database_url = os.environ.get("FIREBASE_DATABASE_URL", DATABASE_URL_DEFAULT).strip()
    if not database_url:
        raise RuntimeError("FIREBASE_DATABASE_URL is empty")
    if not firebase_admin._apps:
        certificate = credentials.Certificate(service_account)
        firebase_admin.initialize_app(certificate, {"databaseURL": database_url})
    return db.reference("gstNewsUpdates"), db.reference("gstNewsSyncStatus")


def main() -> int:
    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    all_items: dict[str, dict[str, Any]] = {}
    source_results: dict[str, dict[str, Any]] = {}
    success_count = 0

    for source in SOURCES:
        try:
            items, status = collect_from_source(source)
            success_count += 1
            source_results[source["key"]] = {"name": source["name"], **status}
            for item in items:
                item["syncedAt"] = now_ms
                all_items[item["id"]] = item
        except Exception as exc:  # Keep the remaining independent sources running.
            LOG.warning("Source unavailable (%s): %s", source["name"], exc)
            source_results[source["key"]] = {
                "name": source["name"],
                "status": "error",
                "error": str(exc)[:280],
                "checkedAt": now.isoformat(),
                "page": source["url"],
            }

    if success_count == 0:
        LOG.error("All official sources were unavailable. Firebase feed was not overwritten.")
        return 2

    news_ref, status_ref = init_firebase()
    existing = news_ref.get() or {}
    merged: dict[str, dict[str, Any]] = dict(existing)
    merged.update(all_items)

    # Keep a bounded, two-year archive. Items without a source date are retained by sync time.
    cutoff_ms = int((now - timedelta(days=RETENTION_DAYS)).timestamp() * 1000)
    merged = {
        key: value for key, value in merged.items()
        if int((value or {}).get("publishedAt") or (value or {}).get("syncedAt") or 0) >= cutoff_ms
    }
    ordered = sorted(
        merged.items(),
        key=lambda pair: int((pair[1] or {}).get("publishedAt") or (pair[1] or {}).get("syncedAt") or 0),
        reverse=True,
    )[:MAX_STORED_ITEMS]
    merged = dict(ordered)

    # One multi-location write keeps the feed consistent for dashboard listeners.
    news_ref.set(merged)
    run_status = "success" if success_count == len(SOURCES) else "partial"
    status_ref.set({
        "runStatus": run_status,
        "lastSyncAt": now_ms,
        "lastSyncIso": now.isoformat(),
        "itemCount": len(merged),
        "newOrUpdatedThisRun": len(all_items),
        "successfulSources": success_count,
        "totalSources": len(SOURCES),
        "schedule": "Daily at approximately 09:00 Asia/Kolkata",
        "sourceResults": source_results,
        "note": "Official-source collection only. If a government website blocks automated access, that source is marked unavailable for this run.",
    })
    LOG.info("Firebase GST news feed updated: %d stored item(s), %d/%d sources available", len(merged), success_count, len(SOURCES))
    return 0 if success_count else 2


if __name__ == "__main__":
    sys.exit(main())
