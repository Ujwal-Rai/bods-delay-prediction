#!/usr/bin/env python3
"""
collect_avl.py
==============
Continuous collector for real-time bus location (AVL) data from the UK
Department for Transport's Bus Open Data Service (BODS).

BODS publishes the *current* position of every bus in Great Britain, refreshed
every 10 seconds, but it does NOT keep a public historical archive. This script
polls the SIRI-VM datafeed endpoint on a fixed interval and appends every
vehicle observation to hourly gzip-compressed CSV files, building the historical
dataset that the analytics pipeline needs.

Design notes for the report
---------------------------
* Credentials are read from the BODS_API_KEY environment variable (or a local
  .env file that is git-ignored). No key is ever hard-coded -- this satisfies
  the "no hard-coded credentials" requirement of the brief.
* Output is partitioned by hour. Spark can then read data/raw/avl/*.csv.gz as a
  single DataFrame while retaining natural file-level parallelism.
* The script is crash-tolerant and resumable: it appends to whatever hour-file
  is current, so it can be stopped and restarted freely without data loss.
* Duplicate observations (the feed returns the same RecordedAtTime if a vehicle
  has not reported since the last poll) are deliberately NOT removed here.
  De-duplication happens in Spark so that the raw capture stays a faithful
  record and the data-quality step is visible in the pipeline.

Usage
-----
    python src/collect_avl.py                     # defaults: Greater Manchester, 30s
    python src/collect_avl.py --once              # single poll, for testing
    python src/collect_avl.py --interval 60       # gentler polling
    python src/collect_avl.py --bbox -1.90,53.70,-1.30,53.95 --region west_yorkshire
"""

from __future__ import annotations

import argparse
import csv
import gzip
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from lxml import etree

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

BODS_DATAFEED_URL = "https://data.bus-data.dft.gov.uk/api/v1/datafeed"

# Bounding box format required by BODS: minLongitude,minLatitude,maxLongitude,maxLatitude
# Default covers Greater Manchester (pairs with the "north_west" GTFS region).
DEFAULT_BBOX = "-2.75,53.32,-1.90,53.70"
DEFAULT_INTERVAL = 30
DEFAULT_OUTDIR = Path("data/raw/avl")

# Fields extracted from each SIRI-VM <VehicleActivity> element.
# Order here defines the CSV column order.
COLUMNS = [
    "poll_time_utc",            # when THIS script made the request
    "recorded_at_time",         # when the vehicle reported its position
    "valid_until_time",
    "item_identifier",
    "line_ref",                 # service/route number as the operator refers to it
    "published_line_name",      # public-facing route number, e.g. "192"
    "direction_ref",
    "operator_ref",             # National Operator Code (NOC) - join key to timetables
    "origin_ref",
    "origin_name",
    "destination_ref",
    "destination_name",
    "origin_aimed_departure_time",   # scheduled start -> key for matching to a GTFS trip
    "destination_aimed_arrival_time",
    "longitude",
    "latitude",
    "bearing",
    "occupancy",
    "block_ref",
    "vehicle_journey_ref",
    "dated_vehicle_journey_ref",
    "data_frame_ref",
    "vehicle_ref",              # unique bus identifier
    "ticket_machine_service_code",
    "journey_code",
]

# Map CSV column -> the SIRI element local-name to look for.
# Namespace-agnostic matching is used because operators vary in prefix usage.
_SIMPLE_FIELDS = {
    "recorded_at_time": "RecordedAtTime",
    "valid_until_time": "ValidUntilTime",
    "item_identifier": "ItemIdentifier",
    "line_ref": "LineRef",
    "published_line_name": "PublishedLineName",
    "direction_ref": "DirectionRef",
    "operator_ref": "OperatorRef",
    "origin_ref": "OriginRef",
    "origin_name": "OriginName",
    "destination_ref": "DestinationRef",
    "destination_name": "DestinationName",
    "origin_aimed_departure_time": "OriginAimedDepartureTime",
    "destination_aimed_arrival_time": "DestinationAimedArrivalTime",
    "longitude": "Longitude",
    "latitude": "Latitude",
    "bearing": "Bearing",
    "occupancy": "Occupancy",
    "block_ref": "BlockRef",
    "vehicle_journey_ref": "VehicleJourneyRef",
    "dated_vehicle_journey_ref": "DatedVehicleJourneyRef",
    "data_frame_ref": "DataFrameRef",
    "vehicle_ref": "VehicleRef",
    "ticket_machine_service_code": "TicketMachineServiceCode",
    "journey_code": "JourneyCode",
}

_STOP = False


# --------------------------------------------------------------------------
# Credential handling
# --------------------------------------------------------------------------

def load_api_key() -> str:
    """Read the BODS API key from the environment or a local .env file."""
    key = os.environ.get("BODS_API_KEY")
    if key:
        return key.strip()

    env_path = Path(".env")
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, _, value = line.partition("=")
            if name.strip() == "BODS_API_KEY":
                return value.strip().strip('"').strip("'")

    sys.exit(
        "ERROR: No BODS API key found.\n"
        "  Register free at https://data.bus-data.dft.gov.uk/account/signup/\n"
        "  then create a file called .env next to this project containing:\n"
        "      BODS_API_KEY=your_key_here\n"
        "  (or set the BODS_API_KEY environment variable)"
    )


# --------------------------------------------------------------------------
# SIRI-VM parsing
# --------------------------------------------------------------------------

def _local_name(tag) -> str:
    """Strip any XML namespace from a tag name."""
    if not isinstance(tag, str):
        return ""
    return tag.rsplit("}", 1)[-1]


def parse_siri_vm(xml_bytes: bytes, poll_time: str) -> list[dict]:
    """
    Turn a SIRI-VM response into a list of flat dictionaries, one per vehicle.

    The SIRI schema nests fields several levels deep and operators differ in how
    much of the optional structure they populate. Rather than depending on exact
    XPaths, this walks each <VehicleActivity> subtree and picks up any element
    whose local name is one we care about. This is markedly more robust across
    the ~400 operators publishing to BODS.
    """
    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError as exc:
        print(f"  ! XML parse error: {exc}")
        return []

    reverse = {v: k for k, v in _SIMPLE_FIELDS.items()}
    records: list[dict] = []

    for activity in root.iter():
        if _local_name(activity.tag) != "VehicleActivity":
            continue

        row = {col: "" for col in COLUMNS}
        row["poll_time_utc"] = poll_time

        for node in activity.iter():
            name = _local_name(node.tag)
            column = reverse.get(name)
            if column is None:
                continue
            # Only take the first occurrence; nested repeats are rare and the
            # outermost value is the authoritative one.
            if row[column] == "" and node.text:
                row[column] = node.text.strip()

        # A record with no position is useless downstream.
        if row["latitude"] and row["longitude"]:
            records.append(row)

    return records


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

def hour_file(outdir: Path, region: str) -> Path:
    """Return the path of the gzip CSV for the current UTC hour."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H")
    return outdir / f"avl_{region}_{stamp}.csv.gz"


def append_rows(path: Path, rows: list[dict]) -> None:
    """Append rows to an hourly gzip CSV, writing a header if the file is new."""
    is_new = not path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "at", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS)
        if is_new:
            writer.writeheader()
        writer.writerows(rows)


# --------------------------------------------------------------------------
# Main polling loop
# --------------------------------------------------------------------------

def poll_once(session: requests.Session, api_key: str, bbox: str,
              timeout: int = 45) -> bytes | None:
    """Make a single request to the BODS datafeed endpoint."""
    params = {"boundingBox": bbox, "api_key": api_key}
    try:
        response = session.get(BODS_DATAFEED_URL, params=params, timeout=timeout)
    except requests.RequestException as exc:
        print(f"  ! Request failed: {exc}")
        return None

    if response.status_code == 200:
        return response.content

    if response.status_code in (401, 403):
        sys.exit(f"ERROR: BODS rejected the API key (HTTP {response.status_code}). "
                 "Check the key in your .env file.")

    print(f"  ! HTTP {response.status_code} from BODS")
    return None


def handle_signal(signum, frame):  # noqa: ARG001
    global _STOP
    _STOP = True
    print("\nStop requested - finishing current cycle and shutting down cleanly...")


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect live BODS bus location data.")
    parser.add_argument("--bbox", default=DEFAULT_BBOX,
                        help="minLon,minLat,maxLon,maxLat (default: Greater Manchester)")
    parser.add_argument("--region", default="gm",
                        help="Short label used in output filenames")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL,
                        help="Seconds between polls (default 30)")
    parser.add_argument("--outdir", type=Path, default=DEFAULT_OUTDIR,
                        help="Directory for the hourly CSV files")
    parser.add_argument("--once", action="store_true",
                        help="Poll a single time and exit (use this to test setup)")
    args = parser.parse_args()

    api_key = load_api_key()
    args.outdir.mkdir(parents=True, exist_ok=True)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    session = requests.Session()
    session.headers.update({"User-Agent": "ST5011CEM-student-project/1.0"})

    print("=" * 68)
    print("BODS live vehicle location collector")
    print("=" * 68)
    print(f"  Bounding box : {args.bbox}")
    print(f"  Interval     : {args.interval}s")
    print(f"  Output       : {args.outdir.resolve()}")
    print(f"  Started      : {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    print("  Press Ctrl+C to stop cleanly.\n")

    total_rows = 0
    polls = 0
    consecutive_failures = 0

    while not _STOP:
        cycle_start = time.monotonic()
        poll_time = datetime.now(timezone.utc).isoformat(timespec="seconds")

        payload = poll_once(session, api_key, args.bbox)

        if payload is None:
            consecutive_failures += 1
            # Exponential backoff, capped, so a network blip doesn't spam the API.
            backoff = min(args.interval * (2 ** min(consecutive_failures, 5)), 600)
            print(f"  Retrying in {backoff}s (failure #{consecutive_failures})")
            if args.once:
                sys.exit(1)
            time.sleep(backoff)
            continue

        consecutive_failures = 0
        rows = parse_siri_vm(payload, poll_time)

        if rows:
            append_rows(hour_file(args.outdir, args.region), rows)
            total_rows += len(rows)

        polls += 1
        print(f"[{poll_time}] poll {polls:>5} | "
              f"{len(rows):>5} vehicles | {total_rows:>9,} rows total")

        if args.once:
            print("\nSingle-poll test complete.")
            if rows:
                print("Sample record:")
                for key, value in list(rows[0].items()):
                    if value:
                        print(f"    {key:<32} {value}")
            break

        # Sleep for the remainder of the interval, staying responsive to Ctrl+C.
        elapsed = time.monotonic() - cycle_start
        remaining = max(0.0, args.interval - elapsed)
        slept = 0.0
        while slept < remaining and not _STOP:
            step = min(0.5, remaining - slept)
            time.sleep(step)
            slept += step

    print(f"\nCollector stopped. {polls:,} polls, {total_rows:,} rows written to "
          f"{args.outdir.resolve()}")


if __name__ == "__main__":
    main()
