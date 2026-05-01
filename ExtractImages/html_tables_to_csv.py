#!/usr/bin/env python3
"""
Convert chandra OCR HTML output to CSV for dataset creation.
- Writes one CSV per page (table_data/page_N/page_N.csv).
- If the HTML has <thead>: retains semantic header names (flattens MultiIndex).
  Skips the table (no CSV written) if both an item_no column and a description
  column cannot be identified — those pages are automatically excluded from
  local and global manifests.
- If the HTML has no <thead>: falls back to normalized column_0, column_1, ...
- Optional --merged: writes a dataset manifest linking figure images to table CSVs.

Usage:
  python html_tables_to_csv.py <table_data_dir> [--merged <manifest.csv>]
  python html_tables_to_csv.py data/results/mar15/table_data --merged data/results/mar15/dataset_manifest.csv

Uses tab as separator (TSV) so commas in table descriptions don't break parsing.
Requires: pip install pandas
"""

import argparse
import re

# Tab separator: descriptions and other fields often contain commas
CSV_SEP = "\t"

# Patterns verified against all PDFs in the dataset.
# Every RPSTL parts table has a column matching each; non-parts tables
# (NSN indexes, abbreviation lists, etc.) lack one or both and are filtered out.
_ITEM_NO_RE = re.compile(r'\bitem\s*no\.?\b|\bitem\s*num', re.IGNORECASE)
_DESC_RE = re.compile(r'\bdescription\b|\bdesc\.?\b', re.IGNORECASE)

import json
import os
import sys

try:
    import pandas as pd
except ImportError:
    print("Install pandas: pip install pandas", file=sys.stderr)
    sys.exit(1)


def drop_all_nan_columns(df, nan_threshold=0.95):
    """Drop columns where >=nan_threshold fraction of values are NaN (handles data-5 NaN-interleaved OCR)."""
    return df.loc[:, df.isna().mean() < nan_threshold]


def drop_adjacent_duplicates(df, threshold=0.90):
    """
    Drop a column if it is >=threshold identical to the immediately preceding column.
    Handles data-2 style where every column is duplicated by the OCR (16-col → 8-col).
    NaN values in both columns at the same position count as identical.
    """
    cols = list(df.columns)
    to_drop = set()
    for i in range(1, len(cols)):
        prev = df[cols[i - 1]].fillna("__NaN__")
        curr = df[cols[i]].fillna("__NaN__")
        if (prev == curr).mean() >= threshold:
            to_drop.add(cols[i])
    return df.drop(columns=list(to_drop))


def normalize_headers(df):
    """Rename columns to column_0, column_1, ... (fallback when HTML has no <thead>)."""
    df = df.copy()
    df.columns = [f"column_{i}" for i in range(len(df.columns))]
    return df


def flatten_headers(df):
    """
    Collapse MultiIndex columns into single strings; strip pandas placeholder
    'Unnamed: N_level_M' entries produced by empty <th> cells.
    """
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        def _clean(v):
            s = str(v).strip()
            return "" if s.lower().startswith("unnamed:") or s == "nan" else s
        df.columns = [
            " ".join(filter(None, (_clean(v) for v in col)))
            for col in df.columns
        ]
    else:
        df.columns = [str(c) for c in df.columns]
    return df


def has_required_columns(df):
    """Return True if df has both an item_no column and a description column."""
    cols = [str(c) for c in df.columns]
    return (
        any(_ITEM_NO_RE.search(c) for c in cols)
        and any(_DESC_RE.search(c) for c in cols)
    )


def html_to_csv(html_path, csv_path=None):
    """
    Read first table from HTML, apply structural cleanup, then:
    - If <thead> present: flatten semantic headers and skip the table (return None,
      no CSV written) if item_no and description columns cannot be identified.
    - If no <thead>: fall back to column_0, column_1, ... normalization.
    Returns DataFrame on success, None if skipped.
    """
    if csv_path is None:
        csv_path = html_path.replace(".html", ".csv")

    with open(html_path, "r", encoding="utf-8", errors="replace") as f:
        has_thead = "<thead>" in f.read().lower()

    tables = pd.read_html(html_path)
    if not tables:
        return None
    df = tables[0]

    # Stage 1: structural cleanup (always applied)
    df = drop_all_nan_columns(df)
    df = drop_adjacent_duplicates(df)

    if has_thead:
        df = flatten_headers(df)
        if not has_required_columns(df):
            # Not a parts table — remove any stale CSV from a previous run
            if os.path.exists(csv_path):
                os.remove(csv_path)
            return None
    else:
        df = normalize_headers(df)

    df.to_csv(csv_path, index=False, encoding="utf-8", sep=CSV_SEP)
    return df


def build_dataset_manifest(parent_dir, pairing, existing_table_pages):
    """
    Build list of (figure_path, csv_path, figure_key) from pairing.
    existing_table_pages: set of ints (table page numbers we have CSVs for).
    Paths are absolute or relative to parent_dir.
    """
    rows = []
    for fig_key, info in pairing.items():
        figure_pages = info.get("figure_pages") or []
        table_pages = info.get("table_pages") or []
        if not figure_pages or not table_pages:
            continue
        figure_path = os.path.join("figures", f"page_{figure_pages[0]}.png")
        for p in table_pages:
            if p not in existing_table_pages:
                continue
            table_path = os.path.join("tables", f"page_{p}.png")
            csv_path = os.path.join("table_data", f"page_{p}", f"page_{p}.csv")
            rows.append({"figure_path": figure_path, "table_path": table_path, "csv_path": csv_path, "figure": fig_key})
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
            print(f"Dataset manifest -> {args.merged} ({len(manifest_df)} rows: figure_path, table_path, csv_path, figure)")
        else:
            print("No manifest rows (missing pairing or no table CSVs).", file=sys.stderr)

    print(f"Done: {len(collected)} tables converted (tab-separated).")


if __name__ == "__main__":
    main()
