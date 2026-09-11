from __future__ import annotations

import csv
import zipfile
from collections.abc import Iterator
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data/futures/um/daily/metrics/BTCUSDT"
OUTPUT = ROOT / "csv/futures/um/daily/metrics/BTCUSDT"
FIELDS = (
    "create_time",
    "symbol",
    "sum_open_interest",
    "sum_open_interest_value",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
)


def rows(archive_path: Path) -> Iterator[dict[str, str]]:
    with zipfile.ZipFile(archive_path) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(csv_names) != 1:
            raise ValueError(f"{archive_path} must contain exactly one CSV")
        with archive.open(csv_names[0]) as raw:
            lines = (line.decode("utf-8-sig") for line in raw)
            reader = csv.DictReader(lines)
            if tuple(reader.fieldnames or ()) != FIELDS:
                raise ValueError(f"unexpected columns in {archive_path}: {reader.fieldnames}")
            yield from reader


def main() -> int:
    archives = sorted(SOURCE.glob("BTCUSDT-metrics-*.zip"))
    if not archives:
        raise SystemExit(f"no archives found under {SOURCE}")
    OUTPUT.mkdir(parents=True, exist_ok=True)

    writers: dict[str, tuple[object, csv.DictWriter]] = {}
    counts: dict[str, int] = {}
    seen_timestamps: set[str] = set()
    try:
        for archive_path in archives:
            for row in rows(archive_path):
                timestamp = row["create_time"]
                if timestamp in seen_timestamps:
                    continue
                seen_timestamps.add(timestamp)
                year = timestamp[:4]
                if year not in writers:
                    handle = (OUTPUT / f"BTCUSDT-metrics-{year}.csv").open(
                        "w", encoding="utf-8", newline=""
                    )
                    writer = csv.DictWriter(handle, fieldnames=FIELDS)
                    writer.writeheader()
                    writers[year] = (handle, writer)
                    counts[year] = 0
                writers[year][1].writerow(row)
                counts[year] += 1
    finally:
        for handle, _writer in writers.values():
            handle.close()

    for year in sorted(counts):
        print(f"{year}: {counts[year]:,} rows")
    print(f"total: {sum(counts.values()):,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
