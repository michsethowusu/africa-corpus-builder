"""
YouVersion Bible Text Scraper — Africa edition (local-text harvester)
=====================================================================
HTTP-only, chapter-level, fully automated.

This edition takes a CSV of YouVersion versions and scrapes EVERY row in it,
one language/version at a time, with NO interactive prompts.  You never type a
version code — point it at a CSV and walk away.

    python youversion_parallel_text_builder.py                      # uses default CSV
    python youversion_parallel_text_builder.py path/to/versions.csv # custom CSV

It harvests ALL local-language verses for each version (it does NOT pair against
English here).  Pivot translations (English / French / Arabic / Chinese /
Portuguese) are fetched once by `build_pivot_caches.py` and joined onto the
local text later by `build_and_push_hf.py`.  Decoupling this way means a single
scrape feeds both the parallel dataset and the monolingual dataset.

It fetches one chapter at a time from bible.com's internal JSON API and splits
it into verses using the data-usfm markers in the returned HTML — ~30× fewer
requests than per-verse scraping.  No Chrome / Selenium required.

API endpoint:
    https://nodejs.bible.com/api/bible/chapter/3.1?id={version}&reference={BOOK}.{ch}

INPUT CSV
---------
Required columns:  version_id, lang_code, lang_name
Optional columns:  viable (skipped only when literally "false"), abbr
Any other columns (e.g. country, has_text, version_title) are ignored.

OUTPUT LAYOUT
-------------
    {OUTPUT_ROOT}/
        progress.json
        testament_status.json
        {LANG_NAME}_{LANG_CODE}_v{VERSION_ID}.csv     # columns: verse_key, version_id, local

Requires: requests, beautifulsoup4, lxml
"""

import sys
import subprocess
import os

# ─────────────────────────────────────────────
# BOOTSTRAP
# ─────────────────────────────────────────────

REQUIRED_PACKAGES = ["requests", "beautifulsoup4", "lxml"]

def _install_packages():
    import_names = {"beautifulsoup4": "bs4"}
    missing = []
    for pkg in REQUIRED_PACKAGES:
        try:
            __import__(import_names.get(pkg, pkg))
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"\n  Installing missing packages: {', '.join(missing)} ...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet"] + missing)
        print("  Packages installed.\n")

_install_packages()

# ── Imports ───────────────────────────────────────────────────────────────────
import csv
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

import requests
from bs4 import BeautifulSoup

# Shared scraping helpers live in youversion_common.py
from youversion_common import (
    REQUEST_HEADERS, ALL_BOOK_CODES, BOOK_CHAPTERS,
    NUM_WORKERS, get_chapter_verses, get_version_chapters,
    clean_text, build_session_pool,
)


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

DEFAULT_VERSIONS_CSV = "youversion_africa_versions.csv"

OUTPUT_ROOT           = "./african_bible_parallel_text_datasets"
PROGRESS_FILE         = os.path.join(OUTPUT_ROOT, "progress.json")
TESTAMENT_STATUS_FILE = os.path.join(OUTPUT_ROOT, "testament_status.json")

CSV_FIELDNAMES      = ["verse_key", "version_id", "local"]
CHAPTER_DONE_SUFFIX = ".__done__"

# ── Locks ─────────────────────────────────────────────────────────────────────
PROG_LOCK = threading.Lock()

_CSV_LOCKS:      dict = {}
_CSV_LOCKS_META = threading.Lock()

def get_lang_csv_lock(csv_path: str) -> threading.Lock:
    with _CSV_LOCKS_META:
        if csv_path not in _CSV_LOCKS:
            _CSV_LOCKS[csv_path] = threading.Lock()
        return _CSV_LOCKS[csv_path]


# ─────────────────────────────────────────────
# PROGRESS
# ─────────────────────────────────────────────

def load_global_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {int(k): v for k, v in data.items()}
    return {}

def save_global_progress_locked(progress: dict):
    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    out = {str(k): v for k, v in progress.items()}
    tmp = PROGRESS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    os.replace(tmp, PROGRESS_FILE)

def is_chapter_done(book, chapter, done_set) -> bool:
    with PROG_LOCK:
        return f"{book}.{chapter}{CHAPTER_DONE_SUFFIX}" in done_set

def mark_chapter_done(version_num, book, chapter, progress_dict, done_set):
    with PROG_LOCK:
        done_set.add(f"{book}.{chapter}{CHAPTER_DONE_SUFFIX}")
        progress_dict[version_num] = list(done_set)

def flush_progress(progress_dict):
    with PROG_LOCK:
        save_global_progress_locked(progress_dict)


# ─────────────────────────────────────────────
# TESTAMENT STATUS
# ─────────────────────────────────────────────

def load_testament_status() -> dict:
    if os.path.exists(TESTAMENT_STATUS_FILE):
        with open(TESTAMENT_STATUS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {int(k): v for k, v in data.items()}
    return {}

def save_testament_status(status: dict):
    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    out = {str(k): v for k, v in status.items()}
    with open(TESTAMENT_STATUS_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)


# ─────────────────────────────────────────────
# CSV HELPERS
# ─────────────────────────────────────────────

def lang_csv_name(lang_name: str, lang_code: str, version_num: int) -> str:
    return f"{lang_name}_{lang_code}_v{version_num}".replace(" ", "_").replace("/", "-") + ".csv"

def lang_csv_path(lang_name: str, lang_code: str, version_num: int) -> str:
    return os.path.join(OUTPUT_ROOT, lang_csv_name(lang_name, lang_code, version_num))

def save_verses(rows: list, csv_path: str):
    """Append a batch of {verse_key, version_id, local} rows to a version CSV."""
    if not rows:
        return
    lock = get_lang_csv_lock(csv_path)
    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    with lock:
        write_header = not os.path.exists(csv_path)
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
            if write_header:
                writer.writeheader()
            writer.writerows(rows)


# ─────────────────────────────────────────────
# CHAPTER WORKER
# ─────────────────────────────────────────────

def process_chapter(book, chapter, version_num, csv_path,
                    progress_dict, done_set, session_queue: Queue):
    stats = {"verses": 0}
    session = session_queue.get()
    try:
        local_verses = get_chapter_verses(session, version_num, book, chapter)
        if not local_verses:
            mark_chapter_done(version_num, book, chapter, progress_dict, done_set)
            flush_progress(progress_dict)
            return stats

        rows = []
        for verse_num in sorted(local_verses.keys()):
            local_text = clean_text(local_verses[verse_num])
            if not local_text:
                continue
            rows.append({
                "verse_key":  f"{book}.{chapter}.{verse_num}",
                "version_id": version_num,
                "local":      local_text,
            })
        save_verses(rows, csv_path)
        stats["verses"] += len(rows)
        mark_chapter_done(version_num, book, chapter, progress_dict, done_set)
    finally:
        session_queue.put(session)

    flush_progress(progress_dict)
    return stats


# ─────────────────────────────────────────────
# PROBE TESTAMENT
# ─────────────────────────────────────────────

# Representative books probed per testament (fallback path only). Several are
# tried — not just one — so a testament isn't skipped when only an unusual book
# is present, e.g. an "NT + Psalms" edition whose only OT book is Psalms.
OT_PROBE_BOOKS = ["GEN", "PSA", "ISA", "EXO", "PRO"]
NT_PROBE_BOOKS = ["MAT", "JHN", "ACT", "REV"]


def probe_testament(label: str, probe_books: list, version_num: int,
                    session: requests.Session) -> bool:
    """Return True if the version has content in chapter 1 of ANY probe book."""
    for book in probe_books:
        print(f"  [{label} probe] fetching {book}.1 ...")
        if get_chapter_verses(session, version_num, book, 1):
            print(f"  [{label} probe] content found in {book}")
            return True
    print(f"  [{label} probe] no content in any probe book — skipping testament")
    return False


def _fallback_chapter_list(version_num, session_queue, testament_status):
    """Probe-based (book, chapter) list, used only when metadata is unavailable.

    Returns None if neither testament has content.
    """
    OT_BOOKS = ALL_BOOK_CODES[:39]
    cached = testament_status.get(version_num)
    probe_session = session_queue.get()
    try:
        if cached and "ot" in cached:
            ot_ok = cached["ot"]
        else:
            ot_ok = probe_testament("OT", OT_PROBE_BOOKS, version_num, probe_session)
            testament_status.setdefault(version_num, {})["ot"] = ot_ok
            save_testament_status(testament_status)
        if cached and "nt" in cached:
            nt_ok = cached["nt"]
        else:
            nt_ok = probe_testament("NT", NT_PROBE_BOOKS, version_num, probe_session)
            testament_status.setdefault(version_num, {})["nt"] = nt_ok
            save_testament_status(testament_status)
    finally:
        session_queue.put(probe_session)

    if not ot_ok and not nt_ok:
        return None

    chapters = []
    for book in ALL_BOOK_CODES:
        in_ot = book in OT_BOOKS
        if (in_ot and not ot_ok) or (not in_ot and not nt_ok):
            continue
        for chapter in range(1, BOOK_CHAPTERS.get(book, 0) + 1):
            chapters.append((book, chapter))
    return chapters


# ─────────────────────────────────────────────
# MAIN PER-VERSION PROCESSING
# ─────────────────────────────────────────────

def build_dataset_for_bible(version_num, lang_code, lang_name,
                            session_queue, progress_dict, testament_status):
    print(f"\n{'='*60}")
    print(f"  Processing: {lang_name} ({lang_code}) — version {version_num}")
    print(f"{'='*60}")

    csv_path = lang_csv_path(lang_name, lang_code, version_num)
    done_set = set(progress_dict.get(version_num, []))
    stats    = {"verses": 0}

    # Ask the version metadata for the exact book/chapter inventory and scrape
    # precisely those chapters. Falls back to probing only if metadata fails.
    meta_session = session_queue.get()
    try:
        inventory = get_version_chapters(meta_session, version_num)
    finally:
        session_queue.put(meta_session)

    if inventory is not None:
        print(f"\n  Metadata: version contains {len(inventory)} canonical chapter(s)")
        candidate_chapters = inventory
    else:
        print("\n  Metadata unavailable — falling back to probe + static table")
        candidate_chapters = _fallback_chapter_list(
            version_num, session_queue, testament_status)
        if candidate_chapters is None:
            print(f"  No content found — skipping {lang_name} ({lang_code}).")
            return stats

    flush_progress(progress_dict)

    tasks            = []
    skipped_chapters = 0
    for book, chapter in candidate_chapters:
        if is_chapter_done(book, chapter, done_set):
            skipped_chapters += 1
        else:
            tasks.append((book, chapter))

    if skipped_chapters:
        print(f"  Skipped {skipped_chapters} already-completed chapters")

    workers = min(NUM_WORKERS, session_queue.qsize())
    print(f"  Processing {len(tasks)} chapters across {workers} workers ...")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(process_chapter, book, chapter, version_num,
                        csv_path, progress_dict, done_set, session_queue):
                (book, chapter)
            for book, chapter in tasks
        }
        for fut in as_completed(futures):
            book, chapter = futures[fut]
            try:
                cs = fut.result()
                stats["verses"] += cs["verses"]
            except Exception as e:
                print(f"  {book}.{chapter} failed: {e}")

    flush_progress(progress_dict)
    print(f"\n  {lang_name} ({lang_code}) v{version_num}: {stats['verses']} verses -> {csv_path}")
    return stats


# ─────────────────────────────────────────────
# VERSIONS CSV
# ─────────────────────────────────────────────

def load_versions_csv(csv_path: str) -> list:
    entries = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"version_id", "lang_code", "lang_name"}
        missing_cols = required - set(reader.fieldnames or [])
        if missing_cols:
            raise ValueError(
                f"CSV {csv_path} is missing required column(s): "
                f"{', '.join(sorted(missing_cols))}"
            )
        for row in reader:
            vid = (row.get("version_id") or "").strip()
            if not vid.isdigit():
                continue
            if (row.get("viable", "") or "").strip().lower() == "false":
                continue
            entries.append((int(vid), (row["lang_code"] or "").strip(),
                            (row["lang_name"] or "").strip()))
    return entries


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_VERSIONS_CSV

    if not os.path.exists(csv_path):
        print(f"Input CSV not found: {csv_path}")
        print(f"Usage: python {os.path.basename(sys.argv[0])} [path/to/versions.csv]")
        sys.exit(1)

    all_entries = load_versions_csv(csv_path)
    if not all_entries:
        print(f"No viable versions found in {csv_path}. Exiting.")
        return

    print(f"Loaded {len(all_entries)} language version(s) from {csv_path}")
    print("Scraping every row automatically — no version code prompt.\n")

    print(f"Spinning up {NUM_WORKERS} HTTP sessions ...")
    session_queue = build_session_pool(NUM_WORKERS)

    progress         = load_global_progress()
    testament_status = load_testament_status()
    grand_total      = 0

    for idx, (version_num, lang_code, lang_name) in enumerate(all_entries, 1):
        print(f"\n########## [{idx}/{len(all_entries)}] "
              f"{lang_name} ({lang_code}) v{version_num} ##########")
        try:
            stats = build_dataset_for_bible(
                version_num, lang_code, lang_name,
                session_queue, progress, testament_status,
            )
        except Exception as e:
            print(f"  !! {lang_name} ({lang_code}) v{version_num} failed: {e}")
            stats = {"verses": 0}
        grand_total += stats["verses"]

    print(f"\n{'='*60}")
    print(f"All done!  Total local verses harvested: {grand_total}")
    print(f"   Output root   : {os.path.abspath(OUTPUT_ROOT)}")
    print(f"   Progress file : {os.path.abspath(PROGRESS_FILE)}")
    print(f"\nNext: run  build_pivot_caches.py  then  build_and_push_hf.py")


if __name__ == "__main__":
    main()
