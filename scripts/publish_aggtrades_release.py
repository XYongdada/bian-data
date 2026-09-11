from __future__ import annotations

import argparse
import csv
import hashlib
import json
import lzma
import os
import re
import shutil
import subprocess
import tarfile
import time
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


HEADER = (
    "agg_trade_id,price,quantity,first_trade_id,last_trade_id,"
    "transact_time,is_buyer_maker\n"
).encode()
ARCHIVE_RE = re.compile(r"^BTCUSDT-aggTrades-(\d{4}-\d{2}(?:-\d{2})?)\.zip$")
REPO_ROOT = Path(__file__).resolve().parents[1]
LOCAL_GH = REPO_ROOT / ".tools/bin/gh.exe"


def gh_executable() -> str:
    executable = shutil.which("gh") or (str(LOCAL_GH) if LOCAL_GH.exists() else None)
    if executable is None:
        raise RuntimeError("GitHub CLI 'gh' is required; install it and run 'gh auth login'")
    return executable


@dataclass(frozen=True)
class Cursor:
    archive: int
    row: int


def parse_args() -> argparse.Namespace:
    repo = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Stream BTCUSDT aggTrades ZIPs into <=limit tar.xz shards and upload GitHub Releases."
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=repo.parent / "data/binance_public/usdm",
        help="Directory containing Binance BTCUSDT aggTrades ZIP archives.",
    )
    parser.add_argument("--repo", default="XYongdada/bian-data")
    parser.add_argument("--state-dir", type=Path, default=repo / ".publish-state")
    parser.add_argument("--raw-chunk-mib", type=int, default=256)
    parser.add_argument("--max-asset-mib", type=int, default=95)
    parser.add_argument("--xz-preset", type=int, default=6, choices=range(0, 10))
    parser.add_argument("--retries", type=int, default=8)
    parser.add_argument(
        "--proxy",
        help="HTTP/SOCKS proxy used only by this process, e.g. http://127.0.0.1:7890",
    )
    parser.add_argument("--compress-only", action="store_true")
    parser.add_argument("--keep-local", action="store_true")
    parser.add_argument("--max-shards", type=int, help="Stop cleanly after this many base chunks.")
    return parser.parse_args()


def archive_paths(root: Path) -> list[Path]:
    parsed: list[tuple[Path, str]] = []
    monthly: set[str] = set()
    for path in root.rglob("BTCUSDT-aggTrades-*.zip"):
        match = ARCHIVE_RE.match(path.name)
        if not match:
            continue
        label = match.group(1)
        parsed.append((path.resolve(), label))
        if len(label) == 7:
            monthly.add(label)
    selected = [
        (path, label)
        for path, label in parsed
        if not (len(label) == 10 and label[:7] in monthly)
    ]
    return [path for path, _label in sorted(selected, key=lambda item: item[1])]


def load_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"version": 1, "cursor": {"archive": 0, "row": 0}, "assets": {}}
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, state: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def timestamp_ms(line: bytes) -> int:
    try:
        return int(line.split(b",", 6)[5])
    except (IndexError, ValueError) as exc:
        raise ValueError(f"invalid aggTrade CSV row: {line[:160]!r}") from exc


def build_raw_chunk(
    archives: list[Path], cursor: Cursor, output: Path, limit: int
) -> tuple[Cursor, int, int, int] | None:
    output.parent.mkdir(parents=True, exist_ok=True)
    current_archive = cursor.archive
    skip_rows = cursor.row
    first_ms = last_ms = -1
    row_count = 0
    written = len(HEADER)
    with output.open("wb") as target:
        target.write(HEADER)
        while current_archive < len(archives):
            archive_path = archives[current_archive]
            with zipfile.ZipFile(archive_path) as archive:
                members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
                if len(members) != 1:
                    raise ValueError(f"{archive_path} must contain exactly one CSV")
                data_row = 0
                with archive.open(members[0]) as source:
                    for line in source:
                        stripped = line.strip()
                        if not stripped:
                            continue
                        if not stripped.split(b",", 1)[0].isdigit():
                            continue
                        if data_row < skip_rows:
                            data_row += 1
                            continue
                        normalized = stripped + b"\n"
                        stamp = timestamp_ms(normalized)
                        if first_ms < 0:
                            first_ms = stamp
                        last_ms = stamp
                        target.write(normalized)
                        written += len(normalized)
                        row_count += 1
                        data_row += 1
                        if written >= limit:
                            return Cursor(current_archive, data_row), first_ms, last_ms, row_count
            current_archive += 1
            skip_rows = 0
    if row_count == 0:
        output.unlink(missing_ok=True)
        return None
    return Cursor(current_archive, 0), first_ms, last_ms, row_count


def utc_label(stamp_ms: int) -> str:
    return datetime.fromtimestamp(stamp_ms / 1000, UTC).strftime("%Y%m%dT%H%M%SZ")


def compress_csv(csv_path: Path, output: Path, preset: int) -> None:
    temporary = output.with_suffix(output.suffix + ".part")
    temporary.unlink(missing_ok=True)
    with csv_path.open("rb") as source, lzma.open(temporary, "wb", preset=preset) as compressed:
        with tarfile.open(fileobj=compressed, mode="w|") as archive:
            info = tarfile.TarInfo(output.name.removesuffix(".tar.xz") + ".csv")
            info.size = csv_path.stat().st_size
            info.mtime = 0
            archive.addfile(info, source)
    os.replace(temporary, output)


def split_csv(path: Path) -> tuple[Path, Path]:
    total = path.stat().st_size
    left = path.with_name(path.stem + "-left.csv")
    right = path.with_name(path.stem + "-right.csv")
    with path.open("rb") as source, left.open("wb") as first, right.open("wb") as second:
        header = source.readline()
        first.write(header)
        second.write(header)
        destination = first
        for line in source:
            if destination is first and source.tell() >= total // 2:
                destination = second
            destination.write(line)
    return left, right


def csv_bounds(path: Path) -> tuple[int, int, int]:
    first = last = -1
    count = 0
    with path.open("rb") as handle:
        handle.readline()
        for line in handle:
            stamp = timestamp_ms(line)
            if first < 0:
                first = stamp
            last = stamp
            count += 1
    if count == 0:
        raise ValueError(f"empty CSV shard: {path}")
    return first, last, count


def make_assets(
    csv_path: Path, output_dir: Path, preset: int, max_bytes: int
) -> list[tuple[Path, int, int, int]]:
    first, last, count = csv_bounds(csv_path)
    name = f"BTCUSDT-aggTrades_{utc_label(first)}_{utc_label(last)}.tar.xz"
    output = output_dir / name
    compress_csv(csv_path, output, preset)
    if output.stat().st_size <= max_bytes:
        return [(output, first, last, count)]
    output.unlink()
    left, right = split_csv(csv_path)
    try:
        return make_assets(left, output_dir, preset, max_bytes) + make_assets(
            right, output_dir, preset, max_bytes
        )
    finally:
        left.unlink(missing_ok=True)
        right.unlink(missing_ok=True)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def run_gh(arguments: list[str], retries: int, capture: bool = False) -> str:
    executable = gh_executable()
    for attempt in range(1, retries + 1):
        result = subprocess.run(
            [executable, *arguments], text=True, capture_output=True, encoding="utf-8"
        )
        if result.returncode == 0:
            return result.stdout if capture else ""
        error = (result.stderr.strip() or result.stdout.strip() or "unknown GitHub error")
        print(f"GitHub error (attempt {attempt}/{retries}): {error}", flush=True)
        if attempt == retries:
            raise RuntimeError(error)
        delay = min(120, 5 * 2 ** (attempt - 1))
        print(f"GitHub operation failed; retry {attempt + 1}/{retries} in {delay}s", flush=True)
        time.sleep(delay)
    raise AssertionError("unreachable")


def ensure_release(repo: str, tag: str, retries: int) -> None:
    executable = gh_executable()
    endpoint = f"repos/{repo}/releases/tags/{tag}"
    for attempt in range(1, retries + 1):
        result = subprocess.run(
            [executable, "api", endpoint, "--silent"],
            text=True,
            capture_output=True,
            encoding="utf-8",
        )
        if result.returncode == 0:
            return
        error = result.stderr.strip() or result.stdout.strip()
        if "HTTP 404" in error:
            run_gh(
                ["release", "create", tag, "--repo", repo, "--title", tag, "--notes", "BTCUSDT aggTrades time-sharded tar.xz archives."],
                retries,
            )
            return
        print(f"GitHub release lookup error (attempt {attempt}/{retries}): {error}", flush=True)
        if attempt == retries:
            raise RuntimeError(error)
        time.sleep(min(120, 5 * 2 ** (attempt - 1)))


def remote_assets(repo: str, tag: str, retries: int) -> dict[str, int]:
    payload = run_gh(
        ["api", f"repos/{repo}/releases/tags/{tag}"], retries, capture=True
    )
    return {item["name"]: int(item["size"]) for item in json.loads(payload)["assets"]}


def upload_asset(path: Path, repo: str, tag: str, retries: int) -> None:
    ensure_release(repo, tag, retries)
    existing = remote_assets(repo, tag, retries)
    if path.name in existing:
        if existing[path.name] != path.stat().st_size:
            raise RuntimeError(f"remote asset name collision with different size: {path.name}")
        print(f"already uploaded: {path.name}", flush=True)
        return
    run_gh(["release", "upload", tag, str(path), "--repo", repo], retries)
    verified = remote_assets(repo, tag, retries)
    if verified.get(path.name) != path.stat().st_size:
        raise RuntimeError(f"remote size verification failed: {path.name}")


def main() -> int:
    args = parse_args()
    if args.raw_chunk_mib <= 0 or args.max_asset_mib <= 0 or args.retries <= 0:
        raise SystemExit("chunk sizes and retries must be positive")
    if args.proxy:
        os.environ["HTTP_PROXY"] = args.proxy
        os.environ["HTTPS_PROXY"] = args.proxy
        os.environ["ALL_PROXY"] = args.proxy
        print("proxy enabled for GitHub operations", flush=True)
    archives = archive_paths(args.source)
    if not archives:
        raise SystemExit(f"no BTCUSDT aggTrades archives under {args.source}")
    args.state_dir.mkdir(parents=True, exist_ok=True)
    state_path = args.state_dir / "state.json"
    state = load_state(state_path)
    cursor_data = state["cursor"]
    cursor = Cursor(int(cursor_data["archive"]), int(cursor_data["row"]))
    assets: dict[str, object] = state["assets"]
    raw_path = args.state_dir / "current.csv"
    output_dir = args.state_dir / "assets"
    output_dir.mkdir(exist_ok=True)
    completed_chunks = 0

    # A compress-only test or an interrupted upload can leave complete local
    # assets behind. Upload those first before advancing the producer.
    if not args.compress_only:
        for name, value in list(assets.items()):
            record = value
            if record.get("status") != "compressed":
                continue
            asset_path = output_dir / name
            if not asset_path.exists() or sha256(asset_path) != record["sha256"]:
                raise RuntimeError(f"pending asset is missing or corrupt: {asset_path}")
            first = int(record["start_ms"])
            tag = f"aggtrades-{datetime.fromtimestamp(first / 1000, UTC).year}"
            upload_asset(asset_path, args.repo, tag, args.retries)
            record["status"] = "verified"
            record["release"] = tag
            save_state(state_path, state)
            if not args.keep_local:
                asset_path.unlink()

    while cursor.archive < len(archives):
        if args.max_shards is not None and completed_chunks >= args.max_shards:
            break
        raw_path.unlink(missing_ok=True)
        built = build_raw_chunk(
            archives, cursor, raw_path, args.raw_chunk_mib * 1024 * 1024
        )
        if built is None:
            break
        next_cursor, _first, _last, _count = built
        generated = make_assets(
            raw_path, output_dir, args.xz_preset, args.max_asset_mib * 1024 * 1024
        )
        for asset_path, first, last, count in generated:
            digest = sha256(asset_path)
            record = {
                "start_ms": first,
                "end_ms": last,
                "rows": count,
                "bytes": asset_path.stat().st_size,
                "sha256": digest,
                "status": "compressed",
            }
            assets[asset_path.name] = record
            save_state(state_path, state)
            if not args.compress_only:
                tag = f"aggtrades-{datetime.fromtimestamp(first / 1000, UTC).year}"
                upload_asset(asset_path, args.repo, tag, args.retries)
                record["status"] = "verified"
                record["release"] = tag
                save_state(state_path, state)
                if not args.keep_local:
                    asset_path.unlink()
        cursor = next_cursor
        state["cursor"] = {"archive": cursor.archive, "row": cursor.row}
        save_state(state_path, state)
        raw_path.unlink(missing_ok=True)
        completed_chunks += 1
        print(
            f"checkpoint: archive={cursor.archive}/{len(archives)} row={cursor.row} assets={len(assets)}",
            flush=True,
        )
    print("complete" if cursor.archive >= len(archives) else "stopped at checkpoint")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
