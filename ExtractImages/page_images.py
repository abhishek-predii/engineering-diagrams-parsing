"""
Extract figure/table page images from PDF and run Claude table extraction.
No dependency on unstructured; only pdf2image, json, os, subprocess.
"""

import json
import os
import subprocess
import sys
from pathlib import Path


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


def run_claude_ocr(tables_dir, output_dir, config_path=None):
    """
    Run Claude API extraction on table PNG images.

    For each page_N.png in tables_dir, sends the image to Claude and writes
    a flat page_N.tsv directly to output_dir — no intermediate HTML step.

    config_path: path to config.yaml (default: config.yaml next to this file).
    Requires: anthropic, pyyaml (pip install anthropic pyyaml).
    Set ANTHROPIC_API_KEY (or provider-specific key) before calling.
    """
    import time

    _dir = os.path.dirname(os.path.abspath(__file__))
    if _dir not in sys.path:
        sys.path.insert(0, _dir)

    from extract_tables import load_config, build_extractor

    if config_path is None:
        config_path = os.path.join(_dir, "config.yaml")

    cfg = load_config(Path(config_path))
    pcfg        = cfg.get("pipeline", {})
    delay       = float(pcfg.get("delay_seconds", 0.3))
    max_retries = int(pcfg.get("max_retries", 3))
    backoff     = float(pcfg.get("retry_backoff", 2.0))

    _, do_extract = build_extractor(cfg)

    tables_dir = os.path.abspath(tables_dir)
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    pngs = sorted(f for f in os.listdir(tables_dir) if f.lower().endswith(".png"))
    print(f"Claude extraction: {len(pngs)} images -> {output_dir}")

    for idx, png_name in enumerate(pngs, 1):
        png_path = Path(os.path.join(tables_dir, png_name))
        stem     = png_path.stem
        out_path = Path(os.path.join(output_dir, f"{stem}.tsv"))

        attempt, wait = 0, delay
        while attempt <= max_retries:
            try:
                print(f"  [{idx}/{len(pngs)}] {png_name} ...", end="", flush=True)
                tsv = do_extract(png_path)
                out_path.write_text(tsv, encoding="utf-8")
                print(" done")
                break
            except Exception as exc:
                err = str(exc)
                is_rate_limit = "rate" in err.lower() or "429" in err
                attempt += 1
                if attempt > max_retries or not is_rate_limit:
                    print(f" ERROR: {exc}", file=sys.stderr)
                    break
                print(f" rate limited, retry {attempt}/{max_retries} in {wait:.1f}s ...")
                time.sleep(wait)
                wait *= backoff

        if idx < len(pngs):
            time.sleep(delay)

    print(f"Claude extraction output -> {output_dir}")
    return output_dir
