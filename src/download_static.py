#!/usr/bin/env python3
"""
download_static.py
==================
One-off downloader for the two *static* BODS catalogues used in this project:

  1. Timetables  (GTFS format, per region)  -> scheduled arrival/departure times
  2. Disruptions (SIRI-SX, per region)      -> incidents affecting services

Unlike the live vehicle-location feed, these are snapshots that BODS refreshes
on a schedule (timetables twice daily, disruptions every minute), so they can be
fetched whenever needed. Neither requires an API key.

Run this once at the start of the project, and ideally once more near the end so
the timetable snapshot overlaps the period covered by the collected AVL data.

Usage
-----
    python src/download_static.py --region north_west
    python src/download_static.py --region yorkshire --outdir data/raw
"""

from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

import requests

GTFS_URL = "https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/{region}/"
DISRUPTIONS_URL = "https://data.bus-data.dft.gov.uk/disruptions/download/bulk_archive"

# BODS GTFS region slugs
REGIONS = [
    "all", "england", "scotland", "wales",
    "north_east", "north_west", "yorkshire", "east_midlands", "west_midlands",
    "east_anglia", "london", "south_east", "south_west",
]


def download(url: str, dest: Path, label: str) -> bool:
    """Stream a file to disk with simple progress reporting."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"\n>>> {label}")
    print(f"    {url}")

    try:
        with requests.get(url, stream=True, timeout=120) as response:
            if response.status_code != 200:
                print(f"    FAILED: HTTP {response.status_code}")
                return False

            total = int(response.headers.get("Content-Length", 0))
            written = 0
            with open(dest, "wb") as fh:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)
                    written += len(chunk)
                    if total:
                        pct = 100 * written / total
                        print(f"\r    {written/1e6:8.1f} MB / {total/1e6:.1f} MB "
                              f"({pct:5.1f}%)", end="", flush=True)
                    else:
                        print(f"\r    {written/1e6:8.1f} MB", end="", flush=True)
            print()
    except requests.RequestException as exc:
        print(f"\n    FAILED: {exc}")
        return False

    print(f"    Saved to {dest}  ({dest.stat().st_size/1e6:.1f} MB)")
    return True


def inspect_zip(path: Path) -> None:
    """Print the contents of a downloaded zip so record scale is visible early."""
    try:
        with zipfile.ZipFile(path) as zf:
            print(f"    Contents of {path.name}:")
            for info in sorted(zf.infolist(), key=lambda i: -i.file_size)[:15]:
                print(f"      {info.filename:<28} {info.file_size/1e6:9.2f} MB "
                      f"uncompressed")
    except zipfile.BadZipFile:
        print(f"    WARNING: {path.name} is not a valid zip file.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Download static BODS datasets.")
    parser.add_argument("--region", default="north_west", choices=REGIONS,
                        help="GTFS region to download (default: north_west)")
    parser.add_argument("--outdir", type=Path, default=Path("data/raw"),
                        help="Where to save the downloads")
    parser.add_argument("--skip-disruptions", action="store_true")
    args = parser.parse_args()

    print("=" * 68)
    print("BODS static data download")
    print("=" * 68)

    gtfs_path = args.outdir / f"gtfs_{args.region}.zip"
    ok = download(GTFS_URL.format(region=args.region), gtfs_path,
                  f"Timetables (GTFS) - {args.region}")
    if ok:
        inspect_zip(gtfs_path)
    else:
        print("\n    If this failed, download manually from:")
        print("    https://data.bus-data.dft.gov.uk/timetable/download/")
        print(f"    and save the file as {gtfs_path}")

    if not args.skip_disruptions:
        dis_path = args.outdir / "disruptions.zip"
        if download(DISRUPTIONS_URL, dis_path, "Disruptions (SIRI-SX)"):
            inspect_zip(dis_path)
        else:
            print("\n    If this failed, download manually from:")
            print("    https://data.bus-data.dft.gov.uk/disruptions/download/")

    print("\nDone.")


if __name__ == "__main__":
    main()
