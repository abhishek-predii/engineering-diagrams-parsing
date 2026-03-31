#!/usr/bin/env python3
"""
Extract figure and table page numbers from Unstructured metadata JSON, and pair
each figure with its following tables (by document order).

Usage:
  python extract_figures_tables_pages.py <metadata.json> [--output <out.json>]

If --output is omitted, writes to <metadata_dir>/figures_tables_pages.json.

Uses only stdlib (json, re, os) so you can run it when you already have
images_tables_metadata.json without needing the unstructured package.
"""

import argparse
import json
import os
import re
import sys

FIGURE_TEXT_RE = re.compile(r"^Figure\s+(\d+)\s*[.\s]", re.IGNORECASE)


def extract_figures_tables_pages(metadata_path, output_path=None):
    """
    Parse metadata JSON. Each figure caption is a separate figure (multi-sheet:
    "Sheet 1 of 2" and "Sheet 2 of 2" are two distinct figures). Tables that
    follow the entire "Figure N" block are assigned to every sheet of that figure.
    """
    with open(metadata_path, "r", encoding="utf-8") as f:
        elements = json.load(f)

    figures = []
    tables = []
    current_figure_num = None
    current_group = []
    tables_since_group_start = []
    pairing = {}
    pair_key_counter = {}

    def flush_group():
        nonlocal current_group, tables_since_group_start
        if not current_group or current_figure_num is None:
            return
        for i, (pnum, caption) in enumerate(current_group):
            key = str(current_figure_num) if len(current_group) == 1 else f"{current_figure_num}_{i + 1}"
            pairing[key] = {
                "figure_pages": [pnum] if pnum is not None else [],
                "figure_texts": [caption],
                "table_pages": list(tables_since_group_start),
            }
        current_group = []
        tables_since_group_start = []

    for idx, el in enumerate(elements):
        meta = el.get("metadata") or {}
        page_number = meta.get("page_number")
        elem_type = el.get("type")
        text = (el.get("text") or "").strip()

        match = FIGURE_TEXT_RE.match(text)
        if match:
            fig_num = int(match.group(1))
            sheet_index = (pair_key_counter.get(fig_num, 0)) + 1
            pair_key_counter[fig_num] = sheet_index
            figures.append({
                "figure_number": fig_num,
                "sheet_index": sheet_index,
                "text": text,
                "page_number": page_number,
                "element_index": idx,
            })
            if current_figure_num is not None and fig_num != current_figure_num:
                flush_group()
            current_figure_num = fig_num
            current_group.append((page_number, text))
            continue

        if elem_type == "Table":
            tables.append({
                "page_number": page_number,
                "element_id": el.get("element_id"),
                "element_index": idx,
            })
            if current_figure_num is not None and page_number is not None:
                if page_number not in tables_since_group_start:
                    tables_since_group_start.append(page_number)

    flush_group()

    result = {
        "figures": figures,
        "tables": tables,
        "pairing": pairing,
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, default=float)
        print(f"Wrote figures/tables pages and pairing to {output_path}")

    return result


def main():
    parser = argparse.ArgumentParser(description="Extract figure/table pages and pairing from metadata JSON")
    parser.add_argument("metadata_json", help="Path to images_tables_metadata.json from extract_metadata()")
    parser.add_argument("--output", "-o", help="Output JSON path (default: same dir as metadata, figures_tables_pages.json)")
    args = parser.parse_args()

    metadata_path = os.path.abspath(args.metadata_json)
    if not os.path.isfile(metadata_path):
        print(f"Error: file not found: {metadata_path}", file=sys.stderr)
        sys.exit(1)

    if args.output:
        output_path = os.path.abspath(args.output)
    else:
        output_path = os.path.join(os.path.dirname(metadata_path), "figures_tables_pages.json")

    result = extract_figures_tables_pages(metadata_path, output_path=output_path)

    print(f"Figures: {len(result['figures'])}")
    print(f"Tables:  {len(result['tables'])}")
    print(f"Pairing: {len(result['pairing'])} figures with associated table pages")
    for fig_key, info in sorted(result["pairing"].items(), key=lambda x: int(x[0])):
        print(f"  Figure {fig_key}: figure_pages={info['figure_pages']}, table_pages={info['table_pages']}")


if __name__ == "__main__":
    main()
