#!/usr/bin/env python3
"""
Convert chandra OCR HTML output to CSV for dataset creation.
- Writes one CSV per page (table_data/page_N/page_N.csv) with normalized headers
  so all CSVs use the same header style: column_0, column_1, ...
- Optional --merged: writes a dataset manifest for "upload figure image, compare to table":
  columns figure_path, csv_path, figure. Each row = one (figure image, ground-truth table CSV).

Usage:
  python html_tables_to_csv.py <table_data_dir> [--merged <manifest.csv>]
  python html_tables_to_csv.py data/results/mar15/table_data --merged data/results/mar15/dataset_manifest.csv

Uses tab as separator (TSV) so commas in table descriptions don't break parsing.
Requires: pip install pandas
"""

import argparse

# Tab separator: descriptions and other fields often contain commas
CSV_SEP = "\t"
import json
import os
import sys

try:
    import pandas as pd
except ImportError:
    print("Install pandas: pip install pandas", file=sys.stderr)
    sys.exit(1)


def normalize_headers(df):
    """Flatten MultiIndex or tuple columns to column_0, column_1, ... so all CSVs have same header style."""
    n = len(df.columns)
    df = df.copy()
    df.columns = [f"column_{i}" for i in range(n)]
    return df


def html_to_csv(html_path, csv_path=None):
    """Read first table from HTML, normalize headers, save as CSV. Returns DataFrame or None."""
    if csv_path is None:
        csv_path = html_path.replace(".html", ".csv")
    tables = pd.read_html(html_path)
    if not tables:
        return None
    df = tables[0]
    df = normalize_headers(df)
    df.to_csv(csv_path, index=False, encoding="utf-8", sep=CSV_SEP)
    return df


def build_dataset_manifest(parent_dir, pairing, existing_table_pages):
    """
    Build list of (figure_path, csv_path, figure_key) from pairing.
    existing_table_pages: set of ints (table page numbers we have CSVs for).
    Paths are absolute or relative to parent_dir.
    """
    figures_dir = os.path.join(parent_dir, "figures")
    table_data_dir = os.path.join(parent_dir, "table_data")
    rows = []
    for fig_key, info in pairing.items():
        figure_pages = info.get("figure_pages") or []
        table_pages = info.get("table_pages") or []
        if not figure_pages or not table_pages:
            continue
        figure_path = os.path.join(figures_dir, f"page_{figure_pages[0]}.png")
        for p in table_pages:
            if p not in existing_table_pages:
                continue
            csv_path = os.path.join(table_data_dir, f"page_{p}", f"page_{p}.csv")
            rows.append({"figure_path": figure_path, "csv_path": csv_path, "figure": fig_key})
    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Convert chandra table HTML to CSV with normalized headers; optional manifest (csv_path, figure)"
    )
    parser.add_argument("table_data_dir", help="Path to table_data/ (chandra output)")
    parser.add_argument(
        "--merged", "-m",
        metavar="MANIFEST.csv",
        help="Write dataset manifest: figure_path, csv_path, figure (for upload-image vs table evaluation)",
    )
    args = parser.parse_args()
    root = os.path.abspath(args.table_data_dir)
    if not os.path.isdir(root):
        print(f"Error: not a directory: {root}", file=sys.stderr)
        sys.exit(1)

    # Convert each page_N.html -> page_N.csv with normalized headers
    collected = []
    for name in sorted(os.listdir(root)):
        sub = os.path.join(root, name)
        if not os.path.isdir(sub):
            continue
        html_name = f"{name}.html"
        html_path = os.path.join(sub, html_name)
        if not os.path.isfile(html_path):
            continue
        csv_path = os.path.join(sub, f"{name}.csv")
        try:
            df = html_to_csv(html_path, csv_path)
            if df is not None:
                collected.append((name, csv_path, df))
                print(f"  {name} -> {csv_path}")
        except Exception as e:
            print(f"  {name}: {e}", file=sys.stderr)

    # Optional: write dataset manifest (figure_path, csv_path, figure) for image-vs-table evaluation
    if args.merged and collected:
        parent = os.path.dirname(root)
        existing_table_pages = set()
        for name, _, _ in collected:
            try:
                existing_table_pages.add(int(name.replace("page_", "")))
            except ValueError:
                pass
        pairing_path = os.path.join(parent, "figures_tables_pages.json")
        if not os.path.isfile(pairing_path):
            print(f"Warning: {pairing_path} not found; cannot build dataset manifest.", file=sys.stderr)
            rows = []
        else:
            with open(pairing_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            rows = build_dataset_manifest(parent, data.get("pairing", {}), existing_table_pages)
        if rows:
            manifest_df = pd.DataFrame(rows)
            manifest_df.to_csv(args.merged, index=False, encoding="utf-8", sep=CSV_SEP)
            print(f"Dataset manifest -> {args.merged} ({len(manifest_df)} rows: figure_path, csv_path, figure)")
        else:
            print("No manifest rows (missing pairing or no table CSVs).", file=sys.stderr)

    print(f"Done: {len(collected)} tables converted (tab-separated; headers column_0, column_1, ...).")


if __name__ == "__main__":
    main()
