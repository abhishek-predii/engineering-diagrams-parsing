#!/usr/bin/env python3
"""
Combine per-PDF dataset_manifest_local.tsv files into a single global manifest.

Each per-PDF manifest has columns: figure_path, table_path, tsv_path, figure
Paths may be absolute (from any machine/user). This script makes them relative
to the base results directory and adds a pdf_id column.

A --threshold filter drops figures that have more than N tsv_path rows
(i.e. more than N tables mapped to them), keeping only focused figure→table pairs.

Usage:
    python create_global_manifest.py <results_dir> [--output <path>] [--threshold N]

Output:
    <results_dir>/dataset_manifest_global.tsv  (default)
"""

import argparse
import os
import sys

import pandas as pd


def relativize(path_str: str, pdf_id: str) -> str:
    """
    Convert an absolute path (from any machine) to a path relative to base.

    Strategy: find the pdf_id directory name in the path and take everything
    from there onward, so cross-machine absolute paths are handled correctly.
    """
    # Normalize separators
    p = path_str.replace("\\", "/")
    needle = f"/{pdf_id}/"
    idx = p.find(needle)
    if idx != -1:
        # Absolute path from any machine — strip everything before pdf_id
        return pdf_id + "/" + p[idx + len(needle):]
    # Already relative — prepend pdf_id/ if not already there
    if not p.startswith(pdf_id + "/"):
        return pdf_id + "/" + p
    return p


def main():
    parser = argparse.ArgumentParser(
        description="Combine per-PDF dataset_manifest_vllm.tsv files into a global manifest"
    )
    parser.add_argument(
        "base",
        help="Results directory containing data-* subdirectories",
    )
    parser.add_argument(
        "--output", "-o",
        help="Output TSV path (default: <base>/dataset_manifest_global_vllm.tsv)",
    )
    parser.add_argument(
        "--manifest-name",
        default="dataset_manifest_local.tsv",
        help="Name of the per-PDF manifest file to look for (default: dataset_manifest_local.tsv)",
    )
    parser.add_argument(
        "--threshold", "-t",
        type=int,
        default=5,
        help="Max number of tsv_path rows per figure to include (default: 5). "
             "Figures with more than this many mapped TSVs are dropped.",
    )
    args = parser.parse_args()

    base = os.path.abspath(args.base)
    if not os.path.isdir(base):
        print(f"Error: results directory not found: {base}", file=sys.stderr)
        sys.exit(1)

    out_path = args.output or os.path.join(base, "dataset_manifest_global.tsv")

    manifests = []
    skipped = []

    for name in sorted(os.listdir(base)):
        d = os.path.join(base, name)
        if not os.path.isdir(d):
            continue
        mf = os.path.join(d, args.manifest_name)
        if not os.path.isfile(mf):
            skipped.append(name)
            continue

        df = pd.read_csv(mf, sep="\t", dtype=str)

        # Relativize path columns regardless of which machine generated them
        for col in ("figure_path", "table_path", "tsv_path"):
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda p: relativize(p, name) if isinstance(p, str) else p
                )

        df["pdf_id"] = name
        manifests.append(df)
        print(f"  {name}: {len(df)} rows")

    if not manifests:
        print("Error: no manifest files found.", file=sys.stderr)
        sys.exit(1)

    if skipped:
        print(f"\nSkipped (no {args.manifest_name}): {', '.join(skipped)}")

    global_df = pd.concat(manifests, ignore_index=True)

    # Reorder columns: put pdf_id right after figure
    cols = [c for c in global_df.columns if c != "pdf_id"]
    fig_idx = cols.index("figure") + 1 if "figure" in cols else len(cols)
    cols.insert(fig_idx, "pdf_id")
    global_df = global_df[cols]

    # Apply threshold: drop figures that have more than --threshold csv rows
    before = len(global_df)
    csv_counts = global_df.groupby("figure_path")["tsv_path"].transform("count")
    global_df = global_df[csv_counts <= args.threshold].reset_index(drop=True)
    dropped_rows = before - len(global_df)
    dropped_figs = global_df["figure_path"].nunique()

    global_df.to_csv(out_path, sep="\t", index=False)
    print(f"\nThreshold  : csv_count <= {args.threshold}")
    print(f"  Dropped rows (figures above threshold) : {dropped_rows:,}")
    print(f"  Unique figures retained : {dropped_figs:,}")
    print(f"\nWrote {out_path}")
    print(f"  Total rows : {len(global_df)}")
    print(f"  PDFs included: {global_df['pdf_id'].unique().tolist()}")


if __name__ == "__main__":
    main()
