#!/usr/bin/env python3
"""
Batch-run Claude table extraction over all data-N result folders.

Calls Claude directly on each table PNG and writes flat TSV files that
create_local_manifest.py can consume immediately.

TSVs are written to:
  <base-dir>/data-N/<output-subdir>/page_N.tsv   (default: table_data/page_N.tsv)

Usage:
  # Process all datasets using config.yaml defaults:
  python3 run_claude_batch.py

  # Process specific datasets:
  python3 run_claude_batch.py --datasets data-1 data-2

  # Use a different config or output subdir:
  python3 run_claude_batch.py --config config.yaml --output-subdir table_data

  # Re-run even if TSVs already exist:
  python3 run_claude_batch.py --overwrite
"""

import argparse
import os
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(_SCRIPT_DIR))


def _find_datasets(base_dir: Path, names: list[str] | None) -> list[Path]:
    """Return sorted list of data-N dirs that have a tables/ subdir with PNGs."""
    if names:
        candidates = [base_dir / n for n in names]
    else:
        candidates = sorted(base_dir.glob("data-*"))

    result = []
    for d in candidates:
        if not d.is_dir():
            print(f"Warning: dataset dir not found, skipping: {d}", file=sys.stderr)
            continue
        tables_dir = d / "tables"
        if not tables_dir.is_dir():
            print(f"Warning: no tables/ in {d.name}, skipping.", file=sys.stderr)
            continue
        if not any(tables_dir.glob("*.png")):
            print(f"Warning: tables/ in {d.name} has no PNGs, skipping.", file=sys.stderr)
            continue
        result.append(d)
    return result


def main() -> None:
    default_config = _SCRIPT_DIR / "config.yaml"
    default_base   = _SCRIPT_DIR / "data" / "results"

    parser = argparse.ArgumentParser(
        description="Batch-run Claude extraction over all data-N/tables/ dirs"
    )
    parser.add_argument(
        "--base-dir", type=Path, default=default_base,
        help=f"Base results directory (default: {default_base})",
    )
    parser.add_argument(
        "--datasets", nargs="+", metavar="DATASET",
        help="Dataset names to process (e.g. data-1 data-2); omit for all",
    )
    parser.add_argument(
        "--config", type=Path, default=default_config,
        help=f"Path to config.yaml (default: {default_config})",
    )
    parser.add_argument(
        "--output-subdir", default="table_data", metavar="SUBDIR",
        help="Output subdirectory inside each data-N dir (default: table_data)",
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Re-extract even if the output TSV already exists",
    )
    args = parser.parse_args()

    if not args.config.exists():
        sys.exit(f"ERROR: config file not found: {args.config}")
    if not args.base_dir.is_dir():
        sys.exit(f"ERROR: base dir not found: {args.base_dir}")

    # Load config and override pipeline settings for this run
    from extract_tables import load_config, build_extractor, HEADER
    import time

    cfg = load_config(args.config)

    # Point results_dir to base_dir and set output_subdir so process_batch
    # writes to base_dir/data-N/<output_subdir>/page_N.tsv
    cfg["pipeline"]["results_dir"] = str(args.base_dir)
    cfg["pipeline"]["output_subdir"] = args.output_subdir
    cfg["pipeline"]["skip_existing"] = not args.overwrite
    if args.datasets:
        cfg["pipeline"]["datasets"] = args.datasets

    datasets = _find_datasets(args.base_dir, args.datasets)
    if not datasets:
        sys.exit("No eligible datasets found.")

    print(f"Config     : {args.config}")
    print(f"Base dir   : {args.base_dir}")
    print(f"Datasets   : {[d.name for d in datasets]}")
    print(f"Output sub : {args.output_subdir}/")
    print(f"Overwrite  : {args.overwrite}")
    print()

    label, do_extract = build_extractor(cfg)
    pcfg        = cfg["pipeline"]
    delay       = float(pcfg.get("delay_seconds", 0.3))
    max_retries = int(pcfg.get("max_retries", 3))
    backoff     = float(pcfg.get("retry_backoff", 2.0))

    total_succeeded = total_skipped = total_failed = 0

    for ds in datasets:
        tables_dir = ds / "tables"
        out_dir    = ds / args.output_subdir
        pngs       = sorted(tables_dir.glob("*.png"))

        print(f"===== {ds.name} ({len(pngs)} images) =====")

        succeeded = skipped = failed = 0
        for idx, png_path in enumerate(pngs, 1):
            out_path = out_dir / (png_path.stem + ".tsv")
            prefix   = f"  [{idx}/{len(pngs)}] {png_path.name}"

            if not args.overwrite and out_path.exists():
                print(f"{prefix} — skipped (exists)")
                skipped += 1
                continue

            out_path.parent.mkdir(parents=True, exist_ok=True)

            attempt, wait = 0, delay
            while attempt <= max_retries:
                try:
                    print(f"{prefix} — extracting ...", end="", flush=True)
                    tsv = do_extract(png_path)
                    out_path.write_text(tsv, encoding="utf-8")
                    print(" done")
                    succeeded += 1
                    break
                except Exception as exc:
                    err = str(exc)
                    is_rate_limit = "rate" in err.lower() or "429" in err
                    attempt += 1
                    if attempt > max_retries or not is_rate_limit:
                        print(f" ERROR: {exc}", file=sys.stderr)
                        failed += 1
                        break
                    print(f" rate limited, retry {attempt}/{max_retries} in {wait:.1f}s ...")
                    time.sleep(wait)
                    wait *= backoff

            if idx < len(pngs):
                time.sleep(delay)

        print(f"  → succeeded: {succeeded}, skipped: {skipped}, failed: {failed}\n")
        total_succeeded += succeeded
        total_skipped   += skipped
        total_failed    += failed

    print(f"All done — succeeded: {total_succeeded}, skipped: {total_skipped}, failed: {total_failed}")


if __name__ == "__main__":
    main()
