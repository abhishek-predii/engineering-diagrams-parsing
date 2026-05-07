#!/usr/bin/env python3
"""
Build dataset_manifest_local.tsv from pre-processed flat TSV files.

Input layout (enginuity_source_data format):
  <result_dir>/
    table_data/          <- pass this as argument
      page_N.tsv         <- flat, one file per page
    figures/
    tables/
    figures_tables_pages.json

Steps performed:
  1. Read each page_N.tsv
  2. Structural cleanup: drop NaN-heavy columns (>=95%) and adjacent duplicates (>=90%)
  3. Validate: must have item_no AND description columns, and must not be empty
  4. Build manifest rows from figures_tables_pages.json pairing
  5. Write dataset_manifest_local.tsv to the parent of table_data/

Usage:
  python create_local_manifest.py <table_data_dir>
  python create_local_manifest.py /opt/predii/isha/enginuity_source_data/results/data-13/table_data
"""

import argparse
import json
import os
import re
import sys

try:
    import pandas as pd
except ImportError:
    print("Install pandas: pip install pandas", file=sys.stderr)
    sys.exit(1)

CSV_SEP = "\t"

# Matches both space-separated ("ITEM NO.") and underscore-separated ("item_no") forms
_ITEM_NO_RE = re.compile(r'\bitem[\s_]*no\.?\b|\bitem[\s_]*num', re.IGNORECASE)
_DESC_RE = re.compile(r'\bdescription\b|\bdesc\.?\b', re.IGNORECASE)


def drop_all_nan_columns(df, nan_threshold=0.95):
    """Drop columns where >=nan_threshold fraction of values are NaN."""
    return df.loc[:, df.isna().mean() < nan_threshold]


def drop_adjacent_duplicates(df, threshold=0.90):
    """
    Drop a column if it is >=threshold identical to the immediately preceding column.
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


def has_required_columns(df):
    """Return True if df has both an item_no column and a description column."""
    cols = [str(c) for c in df.columns]
    return (
        any(_ITEM_NO_RE.search(c) for c in cols)
        and any(_DESC_RE.search(c) for c in cols)
    )


def load_and_validate_tsv(tsv_path):
    """
    Read TSV, apply structural cleanup, validate required columns and non-empty.
    Returns cleaned DataFrame on success, None if invalid.
    """
    try:
        df = pd.read_csv(tsv_path, sep="\t", dtype=str)
    except Exception as e:
        print(f"  Warning: could not read {tsv_path}: {e}", file=sys.stderr)
        return None

    df = drop_all_nan_columns(df)
    df = drop_adjacent_duplicates(df)

    if not has_required_columns(df):
        return None
    if len(df) == 0:
        return None

    return df


def build_manifest_rows(pairing, existing_table_pages):
    """
    Build list of manifest row dicts from pairing and validated page set.
    Paths are relative to the result directory (parent of table_data/).
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
            tsv_path = os.path.join("table_data", f"page_{p}.tsv")
            rows.append({
                "figure_path": figure_path,
                "table_path": table_path,
                "tsv_path": tsv_path,
                "figure": fig_key,
            })
    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Build dataset_manifest_local.tsv from flat TSV files in table_data/"
    )
    parser.add_argument("table_data_dir", help="Path to table_data/ directory")
    args = parser.parse_args()

    table_data_dir = os.path.abspath(args.table_data_dir)
    if not os.path.isdir(table_data_dir):
        print(f"Error: not a directory: {table_data_dir}", file=sys.stderr)
        sys.exit(1)

    result_dir = os.path.dirname(table_data_dir)

    # Step 1-3: scan and validate each page_N.tsv
    existing_table_pages = set()
    for fname in sorted(os.listdir(table_data_dir)):
        if not (fname.startswith("page_") and fname.endswith(".tsv")):
            continue
        try:
            page_num = int(fname[len("page_"):-len(".tsv")])
        except ValueError:
            continue
        tsv_path = os.path.join(table_data_dir, fname)
        df = load_and_validate_tsv(tsv_path)
        if df is not None:
            existing_table_pages.add(page_num)
            print(f"  {fname} -> valid ({len(df)} rows)")
        else:
            print(f"  {fname} -> skipped (missing required columns or empty)")

    print(f"\nValid TSV pages: {len(existing_table_pages)}")

    # Step 4: load pairing
    pairing_path = os.path.join(result_dir, "figures_tables_pages.json")
    if not os.path.isfile(pairing_path):
        print(f"Error: {pairing_path} not found; cannot build manifest.", file=sys.stderr)
        sys.exit(1)

    with open(pairing_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    pairing = data.get("pairing", {})

    # Step 5: build and write manifest
    rows = build_manifest_rows(pairing, existing_table_pages)
    if not rows:
        print("No manifest rows (no valid figure-table pairs found).", file=sys.stderr)
        sys.exit(1)

    manifest_path = os.path.join(result_dir, "dataset_manifest_local.tsv")
    manifest_df = pd.DataFrame(rows)
    manifest_df.to_csv(manifest_path, index=False, sep=CSV_SEP, encoding="utf-8")
    print(f"\nManifest -> {manifest_path} ({len(manifest_df)} rows)")
    print(f"Unique figures: {manifest_df['figure'].nunique()}")


if __name__ == "__main__":
    main()
