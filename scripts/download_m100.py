from __future__ import annotations
import concurrent.futures
import hashlib
import json
import os
import shutil
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import quote

RECORD_ID = "7541722"
API_URL = f"https://zenodo.org/api/records/{RECORD_ID}"
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "raw" / "tarballs"
CHUNK = 1024 * 1024
WORKERS = 1
PRINT_LOCK = threading.Lock()

def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "M100-Risk-Project/0.1 (course research)"})
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.load(response)

def md5_file(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()

def download_one(item: dict) -> tuple[str, int, str]:
    name = item["key"]
    final_path = OUT / name
    part_path = OUT / (name + ".part")
    expected_size = int(item["size"])
    expected_md5 = item["checksum"].split(":", 1)[-1]
    if final_path.exists() and final_path.stat().st_size == expected_size:
        actual = md5_file(final_path)
        if actual == expected_md5:
            with PRINT_LOCK: print(f"[verified existing] {name}", flush=True)
            return name, expected_size, actual
        final_path.unlink()
    url = item.get("links", {}).get("self") or f"https://zenodo.org/api/records/{RECORD_ID}/files/{quote(name)}/content"
    for attempt in range(1, 6):
        offset = part_path.stat().st_size if part_path.exists() else 0
        headers = {"User-Agent": "M100-Risk-Project/0.1 (course research)"}
        if offset:
            headers["Range"] = f"bytes={offset}-"
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=120) as response:
                status = getattr(response, "status", response.getcode())
                mode = "ab" if offset and status == 206 else "wb"
                if mode == "wb": offset = 0
                downloaded = offset
                last_note = time.monotonic()
                with part_path.open(mode) as out:
                    while True:
                        chunk = response.read(CHUNK)
                        if not chunk: break
                        out.write(chunk)
                        downloaded += len(chunk)
                        if time.monotonic() - last_note >= 20:
                            with PRINT_LOCK: print(f"[downloading] {name}: {downloaded/expected_size:.0%}", flush=True)
                            last_note = time.monotonic()
            if part_path.stat().st_size != expected_size:
                raise IOError(f"size mismatch {part_path.stat().st_size} != {expected_size}")
            actual = md5_file(part_path)
            if actual != expected_md5:
                raise IOError(f"MD5 mismatch: {actual} != {expected_md5}")
            part_path.replace(final_path)
            with PRINT_LOCK: print(f"[downloaded + verified] {name} ({expected_size/1e6:.1f} MB)", flush=True)
            return name, expected_size, actual
        except Exception as exc:
            with PRINT_LOCK: print(f"[retry {attempt}/5] {name}: {exc}", flush=True)
            if attempt == 5: raise
            time.sleep(min(60 * attempt, 300))
    raise RuntimeError(f"could not download {name}")

def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    manifest_path = ROOT / "data" / "source_manifest.json"
    try:
        record = fetch_json(API_URL)
        if record.get("id") != int(RECORD_ID): raise RuntimeError("Zenodo returned a different record")
        files = [f for f in record["files"] if f["key"].endswith(".tar")]
    except Exception as exc:
        if not manifest_path.exists(): raise
        cached = json.loads(manifest_path.read_text(encoding="utf-8"))
        if cached.get("doi") != "10.5281/zenodo.7541722" or cached.get("license") != "cc-by-4.0": raise
        print(f"Zenodo metadata temporarily unavailable ({exc}); resuming from the saved official source manifest.", flush=True)
        record = {"id": int(RECORD_ID), "doi": cached["doi"], "metadata": {"title": cached["title"], "license": {"id": cached["license"]}}}
        files = [{"key": f["key"], "size": f["size"], "checksum": f["checksum"], "links": {"self": f["url"]}} for f in cached["files"]]
    total = sum(int(f["size"]) for f in files)
    if record.get("metadata", {}).get("license", {}).get("id") != "cc-by-4.0":
        raise RuntimeError("Unexpected/missing dataset license; stop and inspect metadata")
    free = shutil.disk_usage(ROOT).free
    if free < int(total * 2.1) + 5 * 1024**3:
        raise RuntimeError(f"Need about {total*2.1/1024**3+5:.1f} GB free for downloads and extraction; found {free/1024**3:.1f} GB")
    manifest = {
        "title": record["metadata"]["title"], "doi": record["doi"],
        "license": record["metadata"]["license"]["id"],
        "source": API_URL, "downloaded_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "expected_archive_bytes": total,
        "files": [{"key": f["key"], "size": f["size"], "checksum": f["checksum"], "url": f["links"]["self"]} for f in files],
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Dataset: {manifest['title']} | DOI {record['doi']} | {manifest['license']}", flush=True)
    print(f"Files: {len(files)} tar archives | {total/1e9:.2f} GB | {free/1024**3:.1f} GB free", flush=True)
    failures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(download_one, item): item["key"] for item in files}
        for future in concurrent.futures.as_completed(futures):
            try: future.result()
            except Exception as exc: failures.append((futures[future], str(exc)))
    if failures:
        raise RuntimeError("Download failures: " + repr(failures))
    print(f"All {len(files)} archives downloaded and MD5-verified.", flush=True)
if __name__ == "__main__": main()



