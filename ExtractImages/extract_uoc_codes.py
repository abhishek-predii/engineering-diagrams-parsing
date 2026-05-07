#!/usr/bin/env python3
"""
Extract Usable On Code (UOC) model mappings from Army RPSTL PDFs.

Scans the Introduction/Special Information section of each PDF (pages 10–70)
for the Code → Model table and saves it as uoc_model_codes.tsv alongside the
page TSV files for that dataset.

Output path: data/results/<pdf_id>/table_data/uoc_model_codes.tsv

Usage:
    python extract_uoc_codes.py [--config config.yaml] \
                                [--pdfs-dir data/pdfs] \
                                [--results-dir data/results] \
                                [--output-subdir table_data] \
                                [--datasets data-1 data-2] \
                                [--no-skip]

Requires: pdftoppm (poppler-utils)
    apt install poppler-utils   OR   brew install poppler
"""

import argparse
import base64
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import yaml

SCAN_PAGE_START = 10
SCAN_PAGE_END   = 70

SYSTEM_PROMPT = """\
You are extracting data from scanned US military technical manuals (Repair Parts and Special Tools Lists).
You will be shown individual pages. Your only job is to detect and extract the "Usable on Code" (UOC) \
model mapping table when it appears.
"""

DETECTION_PROMPT = """\
Examine this page carefully.

Does this page contain a "USABLE ON CODE" or "Usable on Code" section that lists codes \
and the vehicle models they apply to? This section is typically titled "Special Information" \
or "Usable on Code" and maps short alphanumeric codes (2-5 characters, e.g. MTH, AVY, H11, \
A13, HVY) to vehicle model designations (e.g. M1070, M998, M1097, M1043 W/W).

The table may have as few as one entry or as many as 30+ entries across multiple columns.
It often appears under a heading like "Identification of the usable on codes used in this \
publication are:" followed by Code / Used On columns.

- If YES this page has such a section: extract EVERY code-to-model pair as TSV with header:
    code\tmodel
  Include ALL pairs even if arranged in multiple side-by-side column groups on the page.
  Include kit combination entries (e.g. HPM → M1097 W/W and L119 Prime Mover Kit).
  Output ONLY the raw TSV — no markdown, no explanations.

- If this page does NOT contain a "Usable on Code" mapping section, output exactly:
    NONE
"""


def _b64(path: Path) -> str:
    return base64.standard_b64encode(path.read_bytes()).decode()


def _strip_fences(text: str) -> str:
    lines = text.strip().splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines).strip()


def build_client(cfg: dict):
    """Return (label, call_fn) where call_fn takes a PNG path and returns extracted text."""
    provider = cfg.get("provider", "anthropic")

    if provider == "anthropic":
        import anthropic
        pcfg   = cfg.get("anthropic", {})
        model  = pcfg.get("model", "claude-sonnet-4-5-20251001")
        maxtok = int(pcfg.get("max_tokens", 2048))
        client = anthropic.Anthropic()

        def call(png: Path) -> str:
            resp = client.messages.create(
                model=model,
                max_tokens=maxtok,
                system=[{"type": "text", "text": SYSTEM_PROMPT,
                          "cache_control": {"type": "ephemeral"}}],
                messages=[{"role": "user", "content": [
                    {"type": "image", "source": {
                        "type": "base64", "media_type": "image/png",
                        "data": _b64(png)}},
                    {"type": "text", "text": DETECTION_PROMPT},
                ]}],
            )
            return _strip_fences(resp.content[0].text)

        return f"anthropic/{model}", call

    if provider == "claude_azure":
        import anthropic
        pcfg       = cfg["claude_azure"]
        deployment = pcfg["deployment"]
        maxtok     = int(pcfg.get("max_tokens", 2048))
        api_key    = os.environ.get(pcfg["api_key_env"], "")
        if not api_key:
            sys.exit(f"ERROR: env var '{pcfg['api_key_env']}' not set")
        client = anthropic.Anthropic(api_key=api_key, base_url=pcfg["endpoint"])

        def call(png: Path) -> str:
            resp = client.messages.create(
                model=deployment,
                max_tokens=maxtok,
                system=[{"type": "text", "text": SYSTEM_PROMPT,
                          "cache_control": {"type": "ephemeral"}}],
                messages=[{"role": "user", "content": [
                    {"type": "image", "source": {
                        "type": "base64", "media_type": "image/png",
                        "data": _b64(png)}},
                    {"type": "text", "text": DETECTION_PROMPT},
                ]}],
            )
            return _strip_fences(resp.content[0].text)

        return f"claude_azure/{deployment}", call

    if provider == "azure_openai":
        from openai import OpenAI
        pcfg       = cfg["azure_openai"]
        deployment = pcfg["deployment"]
        maxtok     = int(pcfg.get("max_tokens", 2048))
        api_key    = os.environ.get(pcfg["api_key_env"], "")
        if not api_key:
            sys.exit(f"ERROR: env var '{pcfg['api_key_env']}' not set")
        client = OpenAI(api_key=api_key, base_url=pcfg["endpoint"])

        def call(png: Path) -> str:
            resp = client.chat.completions.create(
                model=deployment,
                max_tokens=maxtok,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": [
                        {"type": "image_url", "image_url": {
                            "url": f"data:image/png;base64,{_b64(png)}",
                            "detail": "high"}},
                        {"type": "text", "text": DETECTION_PROMPT},
                    ]},
                ],
            )
            return _strip_fences(resp.choices[0].message.content)

        return f"azure_openai/{deployment}", call

    sys.exit(f"ERROR: unknown provider '{provider}'")


def render_page(pdf_path: Path, page_num: int, dpi: int, out_png: Path) -> bool:
    """Render a single PDF page (1-based) to out_png using pdftoppm. Returns True on success."""
    result = subprocess.run(
        ["pdftoppm", "-r", str(dpi), "-f", str(page_num), "-l", str(page_num),
         "-png", "-singlefile", str(pdf_path), str(out_png.with_suffix(""))],
        capture_output=True,
    )
    return result.returncode == 0 and out_png.exists()


def process_pdf(pdf_path: Path, out_dir: Path, call_fn, skip_existing: bool,
                delay: float) -> str:
    """Scan the PDF introduction pages for the UOC mapping table and save as TSV."""
    out_tsv = out_dir / "uoc_model_codes.tsv"

    if skip_existing and out_tsv.exists():
        return "skipped"

    out_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_png = Path(tmpdir) / "page.png"

        for page_num in range(SCAN_PAGE_START, SCAN_PAGE_END + 1):
            if not render_page(pdf_path, page_num, dpi=150, out_png=tmp_png):
                # PDF has fewer pages than scan window — stop scanning
                break

            result = call_fn(tmp_png)

            if result.upper() == "NONE" or not result:
                time.sleep(delay)
                continue

            # Got a candidate UOC table — validate it looks like TSV with a header
            lines = [l for l in result.splitlines() if l.strip()]
            if len(lines) >= 2 and "\t" in lines[0]:
                out_tsv.write_text(result + "\n", encoding="utf-8")
                return f"found on page {page_num} ({len(lines) - 1} codes)"

            # Model returned something unexpected — keep scanning
            time.sleep(delay)

    return "not_found"


def load_config(config_path: Path) -> dict:
    with open(config_path) as fh:
        return yaml.safe_load(fh)


def main() -> None:
    _script_dir = Path(__file__).parent
    default_config      = _script_dir / "config.yaml"
    default_pdfs_dir    = _script_dir / "data" / "pdfs"
    default_results_dir = _script_dir / "data" / "results"

    parser = argparse.ArgumentParser(
        description="Extract UOC model code mappings from RPSTL PDFs"
    )
    parser.add_argument(
        "--config", type=Path, default=default_config,
        help=f"Path to config.yaml (default: {default_config})",
    )
    parser.add_argument(
        "--pdfs-dir", type=Path, default=default_pdfs_dir,
        help=f"Directory containing data-N.pdf files (default: {default_pdfs_dir})",
    )
    parser.add_argument(
        "--results-dir", type=Path, default=default_results_dir,
        help=f"Base results directory containing data-N subdirs (default: {default_results_dir})",
    )
    parser.add_argument(
        "--output-subdir", default="table_data", metavar="SUBDIR",
        help="Subdirectory inside each data-N result dir to write uoc_model_codes.tsv (default: table_data)",
    )
    parser.add_argument(
        "--datasets", nargs="+", metavar="DATASET",
        help="Only process these datasets, e.g. data-1 data-2 (default: all data-*.pdf)",
    )
    parser.add_argument(
        "--skip-existing", action="store_true", default=True,
        help="Skip PDFs that already have uoc_model_codes.tsv (default: on)",
    )
    parser.add_argument(
        "--no-skip", dest="skip_existing", action="store_false",
        help="Re-extract even if uoc_model_codes.tsv already exists",
    )
    args = parser.parse_args()

    if not args.config.exists():
        sys.exit(f"ERROR: config file not found: {args.config}")

    cfg   = load_config(args.config)
    delay = float(cfg.get("pipeline", {}).get("delay_seconds", 0.3))

    label, call_fn = build_client(cfg)

    pdfs = sorted(args.pdfs_dir.glob("data-*.pdf"))
    if args.datasets:
        names = set(args.datasets)
        pdfs  = [p for p in pdfs if p.stem in names]

    if not pdfs:
        sys.exit(f"No data-*.pdf files found in {args.pdfs_dir}")

    print(f"Provider    : {label}")
    print(f"PDFs dir    : {args.pdfs_dir}")
    print(f"Results dir : {args.results_dir}")
    print(f"Output sub  : {args.output_subdir}/uoc_model_codes.tsv")
    print(f"PDFs        : {[p.name for p in pdfs]}")
    print(f"Scan window : pages {SCAN_PAGE_START}–{SCAN_PAGE_END}")
    print()

    counts = {"found": 0, "skipped": 0, "not_found": 0, "error": 0}

    for pdf_path in pdfs:
        out_dir = args.results_dir / pdf_path.stem / args.output_subdir
        print(f"[{pdf_path.name}] scanning ...", end="", flush=True)

        try:
            status = process_pdf(pdf_path, out_dir, call_fn, args.skip_existing, delay)
        except Exception as exc:
            print(f" ERROR: {exc}")
            counts["error"] += 1
            continue

        print(f" {status}")
        key = "found" if status.startswith("found") else status
        counts[key] = counts.get(key, 0) + 1

    print()
    print(f"Done — found: {counts['found']}, skipped: {counts['skipped']}, "
          f"not_found: {counts['not_found']}, errors: {counts['error']}")


if __name__ == "__main__":
    main()
