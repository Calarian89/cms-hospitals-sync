#!/usr/bin/env python3
"""Sync CMS Provider Data datasets with theme "Hospitals".

Downloads each dataset's CSV in parallel, converts the column names to
snake_case, and only re-downloads datasets whose `modified` date has changed
since the last successful download (tracked in state.json).

Usage:
    python cms_hospitals_sync.py [--out-dir DIR] [--workers N] [--force]

Daily schedule:
    cron (06:00):   0 6 * * * python3 /path/to/cms_hospitals_sync.py --out-dir /path/to/data
    Windows:        schtasks /create /tn "CMS Hospitals Sync" /sc daily /st 06:00 /tr "python C:\\path\\cms_hospitals_sync.py --out-dir C:\\path\\data"

Optional: set CMS_SYNC_CONTACT (email or URL) to identify yourself to CMS in the
User-Agent. Needs only Python 3.8+ and the standard library.
Exits 1 if any dataset failed; failed datasets are retried on the next run.
"""
import argparse
import csv
import io
import json
import logging
import os
import re
import sys
import tempfile
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
THEME = "Hospitals"
CONTACT = os.environ.get("CMS_SYNC_CONTACT", "+https://github.com/Calarian89/cms-hospitals-sync")

csv.field_size_limit(sys.maxsize)
log = logging.getLogger("cms_sync")


def snake_case(name):
    name = re.sub(r"['’`]", "", name)  # "Patients’ rating" -> "patients_rating"
    return re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower() or "column"


def snake_headers(headers):
    """snake_case each header; suffix _2, _3... if two collapse to the same name."""
    seen, out = {}, []
    for h in headers:
        base = snake_case(h)
        seen[base] = seen.get(base, 0) + 1
        out.append(base if seen[base] == 1 else f"{base}_{seen[base]}")
    return out


def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": f"cms-hospitals-sync/1.0 ({CONTACT})"})
    return urllib.request.urlopen(req, timeout=120)


def has_theme(item):
    return THEME in item.get("theme", [])


def csv_url(item):
    for dist in item.get("distribution", []):
        url = dist.get("downloadURL", "")
        if url.lower().endswith(".csv"):
            return url
    return None


def load_state(path):
    if path.exists():
        return json.loads(path.read_text())
    return {"last_run": None, "datasets": {}}


def save_state(path, state):
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    with os.fdopen(fd, "w") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def needs_download(item, state, out_dir, force):
    prev = state["datasets"].get(item["identifier"], {})
    exists = (out_dir / f"{item['identifier']}.csv").exists()
    return force or not exists or prev.get("modified") != item.get("modified")


def download(item, url, out_dir):
    """Stream one CSV to disk with snake_case headers. Returns the row count."""
    final = out_dir / f"{item['identifier']}.csv"
    fd, tmp = tempfile.mkstemp(dir=out_dir, suffix=".part")
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as dst, http_get(url) as resp:
            src = io.TextIOWrapper(resp, encoding="utf-8-sig", newline="")
            reader, writer = csv.reader(src), csv.writer(dst)
            header = next(reader, None)
            if header is None:
                raise ValueError("empty CSV")
            writer.writerow(snake_headers(header))
            rows = 0
            for row in reader:
                writer.writerow(row)
                rows += 1
        os.replace(tmp, final)
        return rows
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def main():
    ap = argparse.ArgumentParser(description="Sync CMS Hospitals datasets.")
    ap.add_argument("--out-dir", default="cms_hospitals", type=Path)
    ap.add_argument("--workers", default=8, type=int)
    ap.add_argument("--force", action="store_true", help="re-download everything")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    state_path = args.out_dir / "state.json"
    state = load_state(state_path)
    run_started = datetime.now(timezone.utc).isoformat(timespec="seconds")

    with http_get(METASTORE_URL) as resp:
        items = [i for i in json.load(resp) if has_theme(i)]
    log.info("%d datasets with theme %r (last run: %s)", len(items), THEME, state["last_run"])

    todo = []
    for item in items:
        url = csv_url(item)
        if url is None:
            log.warning("%s: no CSV download, skipping", item["identifier"])
        elif needs_download(item, state, args.out_dir, args.force):
            todo.append((item, url))
    log.info("%d to download, %d unchanged", len(todo), len(items) - len(todo))

    failures = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(download, item, url, args.out_dir): item for item, url in todo}
        for fut in as_completed(futures):
            item = futures[fut]
            try:
                rows = fut.result()
            except Exception as e:
                failures += 1
                log.error("%s (%s): FAILED: %s", item["identifier"], item["title"], e)
                continue
            state["datasets"][item["identifier"]] = {
                "title": item["title"],
                "modified": item.get("modified"),
                "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "rows": rows,
            }
            save_state(state_path, state)
            log.info("%s: %d rows", item["identifier"], rows)

    state["last_run"] = run_started
    save_state(state_path, state)
    log.info("done: %d downloaded, %d failed", len(todo) - failures, failures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
