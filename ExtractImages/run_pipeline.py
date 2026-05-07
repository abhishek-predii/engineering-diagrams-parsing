#!/usr/bin/env python3
"""
Run the full data-creation pipeline for an engineering diagram PDF:
  1. Create output directory
  2. Extract metadata from PDF (Unstructured) -> images_tables_metadata.json
  3. Extract figure/table page numbers and pairing -> figures_tables_pages.json
  4. (optional) Extract page images -> figures/ and tables/ PNGs
  5. (optional) Run Claude extraction on tables/ -> table_data/ (flat TSV per table image)

Usage:
  # Full pipeline (requires conda env with unstructured, e.g. conda activate enginuity):
  python run_pipeline.py <path/to/manual.pdf> [--output-dir <dir>] [--extract-pages] [--run-claude]

  # From existing metadata only (no unstructured needed):
  python run_pipeline.py --from-metadata <path/to/images_tables_metadata.json> [--pdf <path>] [--extract-pages] [--run-claude]

  # With --extract-pages, PDF pages are rendered to result_dir/figures/ and result_dir/tables/.
  # With --run-claude, Claude API runs on tables/ and writes flat TSVs to result_dir/table_data/.
  # --extract-pages requires pdf2image. Requires ANTHROPIC_API_KEY (or configured provider key).
  # TSV files are written as table_data/page_N.tsv, ready for create_local_manifest.py.
"""

import argparse
import os
import sys

from PIL import ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True  # tolerate PDFs with partially-embedded images

# Run from ExtractImages or repo root
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)


def _pair_key_order(k):
    parts = k.split("_")
    fig = int(parts[0])
    sheet = int(parts[1]) if len(parts) > 1 else 0
    return (fig, sheet)


def _all_page_images_exist(result, result_dir):
    """
    Return True if all figure and table pages from pairing already have PNGs
    in figures/ and tables/ under result_dir.
    """
    figures_dir = os.path.join(result_dir, "figures")
    tables_dir = os.path.join(result_dir, "tables")
    needed_figures = {f.get("page_number") for f in result.get("figures", []) if f.get("page_number") is not None}
    needed_tables = {t.get("page_number") for t in result.get("tables", []) if t.get("page_number") is not None}

    # If dirs don't exist yet, we definitely need to extract
    if needed_figures and not os.path.isdir(figures_dir):
        return False
    if needed_tables and not os.path.isdir(tables_dir):
        return False

    for p in needed_figures:
        png = os.path.join(figures_dir, f"page_{p}.png")
        if not os.path.isfile(png):
            return False
    for p in needed_tables:
        png = os.path.join(tables_dir, f"page_{p}.png")
        if not os.path.isfile(png):
            return False
    return True


def run_from_metadata(metadata_path, output_path=None):
    """Run only step 2 (figure/table pages + pairing). No unstructured dependency."""
    from extract_figures_tables_pages import extract_figures_tables_pages
    if output_path is None:
        output_path = os.path.join(os.path.dirname(metadata_path), "figures_tables_pages.json")
    return extract_figures_tables_pages(metadata_path, output_path=output_path)


def main():
    parser = argparse.ArgumentParser(
        description="Run pipeline: extract metadata from PDF and figure/table pages + pairing"
    )
    parser.add_argument(
        "pdf_path",
        nargs="?",
        help="Path to the PDF file (e.g. .../data-7-2.pdf). Omit if using --from-metadata.",
    )
    parser.add_argument(
        "--from-metadata",
        metavar="JSON",
        help="Use existing images_tables_metadata.json (skips PDF extraction; no unstructured needed)",
    )
    parser.add_argument(
        "--output-dir", "-o",
        help="Output directory for all results. Default: data/results/<pdf_stem>/ under ExtractImages",
    )
    parser.add_argument(
        "--extract-pages",
        action="store_true",
        help="Extract figure and table pages as PNGs into figures/ and tables/ (requires PDF; use --pdf with --from-metadata)",
    )
    parser.add_argument(
        "--run-claude",
        action="store_true",
        help="Run Claude extraction on tables/ -> table_data/ (flat TSV output). Requires ANTHROPIC_API_KEY.",
    )
    parser.add_argument(
        "--claude-config",
        default=None,
        metavar="YAML",
        help="Path to config.yaml for Claude extraction (default: config.yaml next to this script)",
    )
    parser.add_argument(
        "--pdf",
        help="PDF path (required for --extract-pages when using --from-metadata)",
    )
    args = parser.parse_args()

    # Mode: from existing metadata only
    if args.from_metadata:
        metadata_path = os.path.abspath(args.from_metadata)
        if not os.path.isfile(metadata_path):
            print(f"Error: metadata file not found: {metadata_path}", file=sys.stderr)
            sys.exit(1)
        result_dir = os.path.dirname(metadata_path)
        os.makedirs(result_dir, exist_ok=True)
        print(f"Using metadata: {metadata_path}")
        print("Step: Extracting figure/table page numbers and pairing...")
        result = run_from_metadata(metadata_path)
        print("\nDone.")
        print(f"  Figures: {len(result['figures'])}")
        print(f"  Tables:  {len(result['tables'])}")
        print(f"  Pairing: {len(result['pairing'])} figure(s) with table pages")
        for key in sorted(result["pairing"].keys(), key=_pair_key_order):
            info = result["pairing"][key]
            print(f"    {key}: figure_pages={info['figure_pages']}, table_pages={info['table_pages']}")
        result_dir = os.path.dirname(metadata_path)
        pdf_path = os.path.abspath(args.pdf) if args.pdf else None
        if args.extract_pages:
            if not pdf_path or not os.path.isfile(pdf_path):
                print("Error: --extract-pages requires a valid --pdf path when using --from-metadata.", file=sys.stderr)
                sys.exit(1)
            if _all_page_images_exist(result, result_dir):
                print("\nStep: Skipped page image extraction (all required figures/tables already present).")
            else:
                print("\nStep: Extracting figure/table page images...")
                from page_images import extract_figure_table_images
                extract_figure_table_images(pdf_path, result_dir)
        if args.run_claude:
            tables_dir = os.path.join(result_dir, "tables")
            table_data_dir = os.path.join(result_dir, "table_data")
            if not os.path.isdir(tables_dir):
                print("Error: tables/ not found. Run with --extract-pages first.", file=sys.stderr)
                sys.exit(1)
            print("\nStep: Running Claude extraction on tables/...")
            from page_images import run_claude_ocr
            run_claude_ocr(tables_dir, table_data_dir, config_path=args.claude_config)
        return

    # Mode: full pipeline from PDF
    if not args.pdf_path:
        parser.error("Either provide pdf_path or use --from-metadata")
    pdf_path = os.path.abspath(args.pdf_path)
    if not os.path.isfile(pdf_path):
        print(f"Error: PDF not found: {pdf_path}", file=sys.stderr)
        sys.exit(1)

    result_dir = args.output_dir
    if result_dir:
        result_dir = os.path.abspath(result_dir)
    else:
        base = os.path.join(_SCRIPT_DIR, "data", "results")
        stem = os.path.splitext(os.path.basename(pdf_path))[0]
        result_dir = os.path.join(base, stem)

    os.makedirs(result_dir, exist_ok=True)
    print(f"Output directory: {result_dir}")

    metadata_path = os.path.join(result_dir, "images_tables_metadata.json")
    skip_pdf_extract = os.path.isfile(metadata_path)

    if skip_pdf_extract:
        print("\nStep 1: Skipped (existing images_tables_metadata.json found).")
    else:
        # Step 1: extract metadata from PDF
        print("\nStep 1: Extracting metadata from PDF (Unstructured hi_res)...")
        try:
            from unstructuredio import UnstructuredPdf, extract_figures_tables_pages
        except ImportError:
            print("Error: 'unstructured' package not found. Activate your conda env (e.g. conda activate vllm) or run with --from-metadata using existing images_tables_metadata.json.", file=sys.stderr)
            sys.exit(1)
        pdf = UnstructuredPdf(pdf_path, result_dir=result_dir)
        metadata_path = pdf.extract_metadata()
        if not metadata_path or not os.path.isfile(metadata_path):
            print("Error: metadata extraction did not produce images_tables_metadata.json", file=sys.stderr)
            sys.exit(1)

    if not os.path.isfile(metadata_path):
        print("Error: metadata file not found.", file=sys.stderr)
        sys.exit(1)
    try:
        from unstructuredio import extract_figures_tables_pages
    except ImportError:
        from extract_figures_tables_pages import extract_figures_tables_pages

    # Step 2: extract figure/table pages and pairing
    print("\nStep 2: Extracting figure/table page numbers and pairing...")
    figures_tables_path = os.path.join(result_dir, "figures_tables_pages.json")
    result = extract_figures_tables_pages(metadata_path, output_path=figures_tables_path)

    # Step 3 (optional): extract page images to figures/ and tables/
    if args.extract_pages:
        if _all_page_images_exist(result, result_dir):
            print("\nStep 3: Skipped page image extraction (all required figures/tables already present).")
        else:
            print("\nStep 3: Extracting figure/table page images...")
            from page_images import extract_figure_table_images
            extract_figure_table_images(pdf_path, result_dir)

    # Step 4 (optional): Claude extraction on tables/
    if args.run_claude:
        tables_dir = os.path.join(result_dir, "tables")
        table_data_dir = os.path.join(result_dir, "table_data")
        if not os.path.isdir(tables_dir):
            print("Error: tables/ not found. Run with --extract-pages first.", file=sys.stderr)
            sys.exit(1)
        print("\nStep 4: Running Claude extraction on tables/...")
        from page_images import run_claude_ocr
        run_claude_ocr(tables_dir, table_data_dir, config_path=args.claude_config)

    print("\nDone.")
    print(f"  Figures: {len(result['figures'])}")
    print(f"  Tables:  {len(result['tables'])}")
    print(f"  Pairing: {len(result['pairing'])} figure(s) with table pages")
    for key in sorted(result["pairing"].keys(), key=_pair_key_order):
        info = result["pairing"][key]
        print(f"    {key}: figure_pages={info['figure_pages']}, table_pages={info['table_pages']}")


if __name__ == "__main__":
    main()
