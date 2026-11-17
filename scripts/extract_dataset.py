from __future__ import annotations
import argparse
import concurrent.futures
import tarfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
TARS = ROOT / "data" / "raw" / "tarballs"
DEST = ROOT / "data" / "raw" / "parquet"

def safe_extract(tar_path: Path) -> tuple[str, int]:
    rack = tar_path.stem
    target_root = DEST / f"rack_{rack}"
    target_root.mkdir(parents=True, exist_ok=True)
    n = 0
    with tarfile.open(tar_path, "r:") as tf:
        for member in tf:
            rel = PurePosixPath(member.name)
            if rel.is_absolute() or ".." in rel.parts:
                raise ValueError(f"Unsafe path in {tar_path.name}: {member.name}")
            if member.issym() or member.islnk() or member.isdev():
                raise ValueError(f"Unexpected link/device in {tar_path.name}: {member.name}")
            out = (target_root / Path(*rel.parts)).resolve()
            if target_root.resolve() not in out.parents and out != target_root.resolve():
                raise ValueError(f"Archive path escapes extraction folder: {member.name}")
            tf.extract(member, target_root, set_attrs=False)
            if member.isfile(): n += 1
    return tar_path.name, n

def main() -> None:
    parser = argparse.ArgumentParser(description="Safely extract the M100 rack tar files")
    parser.add_argument('--racks', nargs='*', help='Optional archive IDs to extract now, e.g. --racks 37 17')
    args = parser.parse_args()
    DEST.mkdir(parents=True, exist_ok=True)
    archives = sorted(TARS.glob("*.tar"), key=lambda p: p.name)
    if args.racks is not None:
        requested = set(args.racks)
        archives = [p for p in archives if p.stem in requested]
        missing = requested - {p.stem for p in archives}
        if missing: raise SystemExit(f"Archives not downloaded yet: {sorted(missing)}")
    elif len(archives) != 50:
        raise SystemExit(f"Expected 50 verified tar archives, found {len(archives)}. Run scripts/download_m100.py first.")
    print(f"Extracting {len(archives)} archives into {DEST}", flush=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        for name, count in pool.map(safe_extract, archives):
            print(f"[extracted] {name}: {count} files", flush=True)
    parquet_files = list(DEST.rglob("*.parquet"))
    print(f"Currently extracted {len(parquet_files)} Parquet files.", flush=True)
    if not parquet_files: raise SystemExit("No Parquet files found after extraction")

if __name__ == "__main__": main()
