#!/usr/bin/env python3
"""
Render only missing figure/table page PNGs for datasets that already have partial renders.
Reads figures_tables_pages.json, compares against existing PNGs, renders only what's missing.
"""

import json
import os
import sys

def render_missing(pdf_path, result_dir, dpi=200):
    from pdf2image import convert_from_path

    json_path = os.path.join(result_dir, "figures_tables_pages.json")
    with open(json_path) as f:
        data = json.load(f)

    expected_figs = sorted({f["page_number"] for f in data["figures"] if f.get("page_number")})
    expected_tables = sorted({t["page_number"] for t in data["tables"] if t.get("page_number")})

    figures_dir = os.path.join(result_dir, "figures")
    tables_dir = os.path.join(result_dir, "tables")
    os.makedirs(figures_dir, exist_ok=True)
    os.makedirs(tables_dir, exist_ok=True)

    actual_figs = set(int(f.replace("page_","").replace(".png",""))
                      for f in os.listdir(figures_dir) if f.endswith(".png"))
    actual_tables = set(int(f.replace("page_","").replace(".png",""))
                        for f in os.listdir(tables_dir) if f.endswith(".png"))

    missing_figs = sorted(set(expected_figs) - actual_figs)
    missing_tables = sorted(set(expected_tables) - actual_tables)

    extra_figs = actual_figs - set(expected_figs)
    extra_tables = actual_tables - set(expected_tables)

    dataset = os.path.basename(result_dir)
    print(f"\n=== {dataset} ===")
    print(f"  Figures: {len(expected_figs)} expected, {len(actual_figs)} exist, {len(missing_figs)} to render, {len(extra_figs)} extra")
    print(f"  Tables:  {len(expected_tables)} expected, {len(actual_tables)} exist, {len(missing_tables)} to render, {len(extra_tables)} extra")

    if extra_figs:
        print(f"  Removing {len(extra_figs)} extra figure PNGs...")
        for p in extra_figs:
            os.remove(os.path.join(figures_dir, f"page_{p}.png"))
    if extra_tables:
        print(f"  Removing {len(extra_tables)} extra table PNGs...")
        for p in extra_tables:
            os.remove(os.path.join(tables_dir, f"page_{p}.png"))
            tsv_path = os.path.join(result_dir, "table_data", f"page_{p}.tsv")
            if os.path.exists(tsv_path):
                os.remove(tsv_path)

    for i, page in enumerate(missing_figs, 1):
        images = convert_from_path(pdf_path, first_page=page, last_page=page, dpi=dpi)
        if images:
            images[0].save(os.path.join(figures_dir, f"page_{page}.png"), "PNG")
        print(f"  figures/page_{page}.png ({i}/{len(missing_figs)})")

    new_table_pages = []
    for i, page in enumerate(missing_tables, 1):
        images = convert_from_path(pdf_path, first_page=page, last_page=page, dpi=dpi)
        if images:
            images[0].save(os.path.join(tables_dir, f"page_{page}.png"), "PNG")
            new_table_pages.append(page)
        print(f"  tables/page_{page}.png ({i}/{len(missing_tables)})")

    print(f"  Done: rendered {len(missing_figs)} figures, {len(missing_tables)} tables")
    return new_table_pages


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--datasets", nargs="+", required=True)
    parser.add_argument("--results-dir", default="data/results")
    parser.add_argument("--pdfs-dir", default="../pdfs")
    parser.add_argument("--dpi", type=int, default=200)
    args = parser.parse_args()

    for ds in args.datasets:
        pdf = os.path.join(args.pdfs_dir, f"{ds}.pdf")
        result_dir = os.path.join(args.results_dir, ds)
        if not os.path.isfile(pdf):
            print(f"ERROR: PDF not found: {pdf}", file=sys.stderr)
            continue
        render_missing(pdf, result_dir, args.dpi)
