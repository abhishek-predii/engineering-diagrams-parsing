"""
Extract figure/table page images from PDF and run chandra OCR.
No dependency on unstructured; only pdf2image, json, os, subprocess.
"""

import json
import os
import subprocess
import sys


def extract_figure_table_images(pdf_path, result_dir, dpi=200):
    """
    Using figures_tables_pages.json in result_dir, extract PDF pages as PNGs:
    - Figure pages -> result_dir/figures/page_N.png
    - Table pages  -> result_dir/tables/page_N.png
    Creates figures/ and tables/ dirs. Requires pdf2image.
    """
    from pdf2image import convert_from_path

    json_path = os.path.join(result_dir, "figures_tables_pages.json")
    if not os.path.isfile(json_path):
        raise FileNotFoundError(f"Need {json_path} (run pipeline step 2 first)")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    figure_pages = sorted({f["page_number"] for f in data["figures"] if f.get("page_number") is not None})
    table_pages = sorted({t["page_number"] for t in data["tables"] if t.get("page_number") is not None})
    figures_dir = os.path.join(result_dir, "figures")
    tables_dir = os.path.join(result_dir, "tables")
    os.makedirs(figures_dir, exist_ok=True)
    os.makedirs(tables_dir, exist_ok=True)
    pdf_path = os.path.abspath(pdf_path)
    if not os.path.isfile(pdf_path):
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    for page in figure_pages:
        images = convert_from_path(pdf_path, first_page=page, last_page=page, dpi=dpi)
        if images:
            out = os.path.join(figures_dir, f"page_{page}.png")
            images[0].save(out, "PNG")
            print(f"  figures/page_{page}.png")
    for page in table_pages:
        images = convert_from_path(pdf_path, first_page=page, last_page=page, dpi=dpi)
        if images:
            out = os.path.join(tables_dir, f"page_{page}.png")
            images[0].save(out, "PNG")
            print(f"  tables/page_{page}.png")
    print(f"Figures: {len(figure_pages)} pages -> {figures_dir}")
    print(f"Tables:  {len(table_pages)} pages -> {tables_dir}")
    return figures_dir, tables_dir


def run_chandra_ocr(tables_dir, output_dir, method="hf", chandra_cmd=None):
    """
    Run chandra OCR on table images. Writes HTML (and md/metadata) per image to output_dir.
    For each table image (e.g. page_5.png), chandra creates output_dir/page_5/page_5.html (and .md, _metadata.json).
    You can then convert these HTML tables to CSV for your dataset.
    Requires chandra CLI (e.g. conda activate ocr or pip install chandra-ocr).
    """
    tables_dir = os.path.abspath(tables_dir)
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    cmd = chandra_cmd or "chandra"
    args = [cmd, tables_dir, output_dir, "--method", method]
    print(f"Running: {' '.join(args)}")
    out = subprocess.run(args, capture_output=True, text=True)
    if out.returncode != 0:
        print(out.stderr or out.stdout, file=sys.stderr)
        raise RuntimeError(f"chandra exited with code {out.returncode}")
    print(f"Chandra OCR output -> {output_dir}")
    return output_dir


def _iter_page_htmls(table_data_dir):
    """Yield paths to page_N.html under table_data_dir."""
    if not os.path.isdir(table_data_dir):
        return
    for name in os.listdir(table_data_dir):
        sub = os.path.join(table_data_dir, name)
        if os.path.isdir(sub):
            html = os.path.join(sub, f"{name}.html")
            if os.path.isfile(html):
                yield html
