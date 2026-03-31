#!/usr/bin/env python3
"""
Run chandra OCR (typically --method vllm) over every result folder that contains
a `tables/` directory under ExtractImages/data/results/.

This matches the command that worked for a single image:
  chandra <tables_dir_or_file> <output_dir> --method vllm

We run it per `tables/` directory so chandra processes all images inside.

Usage:
  python run_chandra_batch.py
  python run_chandra_batch.py --base-dir /path/to/ExtractImages/data/results --method vllm
  python run_chandra_batch.py --output-subdir table_data --overwrite

Defaults:
  - base-dir: ./data/results (relative to this script)
  - method: vllm
  - output-subdir: table_data_vllm (so we don't overwrite existing hf runs)
  - skips a result if output-subdir already exists and is non-empty (unless --overwrite)
"""

import argparse
import os
import subprocess
import sys


def _is_nonempty_dir(path: str) -> bool:
    return os.path.isdir(path) and any(True for _ in os.scandir(path))


def _find_tables_dirs(base_dir: str):
    """Yield (result_dir, tables_dir) for each result_dir containing tables/*.png."""
    for entry in sorted(os.scandir(base_dir), key=lambda e: e.name):
        if not entry.is_dir():
            continue
        result_dir = entry.path
        tables_dir = os.path.join(result_dir, "tables")
        if not os.path.isdir(tables_dir):
            continue
        # Check if it contains any pngs
        has_png = False
        for f in os.scandir(tables_dir):
            if f.is_file() and f.name.lower().endswith(".png"):
                has_png = True
                break
        if has_png:
            yield result_dir, tables_dir


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_base = os.path.join(script_dir, "data", "results")

    parser = argparse.ArgumentParser(description="Batch-run chandra OCR over all tables/ dirs in results")
    parser.add_argument("--base-dir", default=default_base, help=f"Base results dir (default: {default_base})")
    parser.add_argument("--method", default="vllm", choices=("vllm", "hf"), help="Chandra method (default: vllm)")
    parser.add_argument(
        "--output-subdir",
        default="table_data_vllm",
        help="Output subdirectory name inside each result dir (default: table_data_vllm)",
    )
    parser.add_argument("--overwrite", action="store_true", help="Re-run even if output dir is non-empty")
    parser.add_argument("--chandra-cmd", default="chandra", help="Chandra executable (default: chandra)")
    args = parser.parse_args()

    base_dir = os.path.abspath(args.base_dir)
    if not os.path.isdir(base_dir):
        print(f"Error: base dir not found: {base_dir}", file=sys.stderr)
        sys.exit(1)

    targets = list(_find_tables_dirs(base_dir))
    if not targets:
        print(f"No tables/ dirs found under {base_dir}")
        return

    print(f"Found {len(targets)} result dirs with tables/.")
    for result_dir, tables_dir in targets:
        out_dir = os.path.join(result_dir, args.output_subdir)
        if not args.overwrite and _is_nonempty_dir(out_dir):
            print(f"Skipping (output exists): {out_dir}")
            continue
        os.makedirs(out_dir, exist_ok=True)
        cmd = [args.chandra_cmd, tables_dir, out_dir, "--method", args.method]
        print(f"\n===== {os.path.basename(result_dir)} =====")
        print("Running:", " ".join(cmd))
        # Stream output so you can see progress/errors live
        p = subprocess.run(cmd)
        if p.returncode != 0:
            print(f"Chandra failed for {tables_dir} (exit={p.returncode}). Continuing.", file=sys.stderr)

    print("\nDone.")


if __name__ == "__main__":
    main()

