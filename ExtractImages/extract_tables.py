#!/usr/bin/env python3
"""
Claude Table Extraction Pipeline
Sends table PNG images to a vision model and returns clean TSV output.

Supports three providers:
  anthropic      — native Anthropic API (uses ANTHROPIC_API_KEY)
  azure_openai   — Azure AI Foundry: GPT-4o (uses SUBSCRIPTION_KEY)
  claude_azure   — Azure AI Foundry: Claude models (uses SUBSCRIPTION_KEY)

Output path behaviour (controlled by config.yaml pipeline section):
  - If `output_subdir` is set (e.g. "table_data"), TSVs are written to:
      results_dir/<dataset>/<output_subdir>/page_N.tsv
    This is the default for pipeline use — TSVs land directly inside each
    dataset's result folder, ready for create_local_manifest.py.
  - If `output_subdir` is NOT set, TSVs are written to:
      output_dir/<dataset>/page_N.tsv
    Use this for standalone runs where you want output in a separate directory.

Usage:
    python extract_tables.py [--config config.yaml] [--datasets data-7 data-8] [--output-dir DIR]
"""

import argparse
import base64
import os
import sys
import time
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a precision table extraction engine. Extract five columns from parts-list tables in technical manuals.

Output format:
- Five columns only: item_no, part_no, description, uoc, quantity
- Single flat header row: item_no\tpart_no\tdescription\tuoc\tquantity
- ONE row per item_no — do NOT output separate rows for sub-components
- The "item_no" column is labelled as "ITEM NO", "ITEM NUMBER", "(b)", or similar
- The "part_no" column is labelled as "PART NUMBER", "PART NO", "P/N", "NSN", "STOCK NO", "SMR", or similar — it contains alphanumeric part/stock identifiers
- The "description" column is labelled as "DESCRIPTION", "NOMENCLATURE", or similar
- The "uoc" column contains Usable On Code values found on lines starting with "UOC:" within the description cell
- The "quantity" column is labelled as "QTY", "Q", "QUANTITY", or similar — it is usually the last column

Sub-component vs. wrapped text rules (CRITICAL):
- Some kit items list sub-components beneath the main entry. In the image, EACH sub-component line has its own figure-item reference tag in an adjacent column (format: "N) fig-item", e.g., "2) 208-14", "6) 208-16"). When you see multiple lines each paired with their own figure-item tag, those are sub-component entries — skip ALL of them, including their description text.
- The main item's description is only the text on its primary line (or lines that are true wraps of the same phrase). Do NOT include sub-component names (e.g., NUT, SCREW, WASHER, BEARING, PIN, SPACER listed as separate components beneath a kit) in the description.
- Wrapped text rule: a line is a continuation only if it reads as the same phrase continuing (e.g., a truncated word or spec that carries over). If it reads as a new, distinct part name → it is a sub-component line — skip it.
- Rule of thumb: "PARTS KIT,HINGE TAI" + "HINGE,BUTT" is wrapped text → join. "PARTS KIT,HINGE TAIL" followed by "NUT,SELF-LOCKING" is a sub-component → skip "NUT,SELF-LOCKING".

Part number extraction rules:
- Tables typically have several identifier columns: SMR code (e.g., PAHZZ, XAHZZ, PAFZZ — letter codes indicating source/maintenance), NSN (13-digit National Stock Number), CAGEC (5-character Commercial and Government Entity code), and PART NUMBER (the actual manufacturer part identifier)
- Extract ONLY the PART NUMBER column value into part_no — do NOT include the SMR code, NSN, or CAGEC
- If no part number is present, output an empty string

Description extraction rules:
- Collect only lines that are part of the item name/description (wrapped text), joined with a space
- Dot leaders (sequences of 3 or more dots like "............") are visual fill characters — strip them entirely
- Text that appears AFTER dot leaders on the same line, or on the next wrapped line, is still part of the description IF it reads as part of the part name or spec — include it
- Example: "O-RING 16 X 3MM PART OF KIT P/N 1............417 010 008" → "O-RING 16 X 3MM PART OF KIT P/N 1 417 010 008"
- EXCLUDE any lines starting with "UOC:" from the description — move their values to the uoc column instead
- Strip leading dots/bullets at the very start of a description
- Digits must be digits: zero (0) is never the letter O — read carefully

UOC extraction rules:
- Lines starting with "UOC:" inside a description cell contain Usable On Code values
- Extract the code(s) that follow "UOC:" and place them in the uoc column
- If multiple UOC lines exist for one item, join all codes with a space
- Figure-item reference tags (e.g., "2) 208-14") are NOT UOC codes — do not place them in the uoc column
- If no UOC line is present, output an empty string

Quantity extraction rules:
- The quantity is a numeric value (e.g. 1, 2, 4) found in the last column (QTY/Q)
- If the quantity cell is blank or not present for an item, output an empty string for that field

Non-parts-list page rule (CRITICAL):
- If the image does NOT contain a standard parts list table (e.g., it is a List of Effective Pages, NSN/Part Number Index, alphabetical components listing, cross-reference table, figure, or any other non-parts-list content), output ONLY the header line and nothing else:
  item_no\tpart_no\tdescription\tuoc\tquantity
- A standard parts list has columns for ITEM NO, PART NUMBER, DESCRIPTION, and QTY. If these are absent, output only the header.

Output ONLY raw TSV — no markdown fences, no explanations, no section labels
"""

EXTRACTION_PROMPT = """\
Extract the item_no, part_no, description, uoc, and quantity columns from the parts list table in this image.

Rules:
1. Single header line: item_no\tpart_no\tdescription\tuoc\tquantity
2. If this image is NOT a standard parts list table (e.g., it is an index, cross-reference, List of Effective Pages, alphabetical listing, or figure), output ONLY the header line and nothing else — no explanations
3. ONE row per item_no — do not output separate rows for sub-components
4. Sub-components: each sub-component line in the image has its OWN figure-item reference tag (e.g., "2) 208-14") in an adjacent column. When you see several lines each paired with their own figure-item tag beneath a main entry, they are sub-components — skip those lines AND their description text entirely
5. Wrapped text: include a line in the description only if it is a true continuation of the same phrase (truncated or carried-over text). If the line reads as a new, distinct part name, it is a sub-component — skip it
6. Dot leaders (........) are fill characters — strip them, keep real text before AND after them
7. Lines starting with "UOC:" are Usable On Codes — extract code(s) into the uoc column, not the description
8. Figure-item tags like "2) 208-14" are NOT UOC codes — leave uoc empty if only figure references are present
9. part_no: extract ONLY the PART NUMBER column value — do NOT include the SMR code (PAHZZ, XAHZZ, etc.), NSN (13-digit number), or CAGEC (5-char code) in part_no
10. The quantity column (QTY/Q) is the last column — extract its numeric value; leave empty if blank
11. Digits only — 0 is zero, not letter O
12. No markdown, no extra columns, no blank lines between rows

Output TSV now:"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _b64(path: Path) -> str:
    return base64.standard_b64encode(path.read_bytes()).decode()


HEADER = "item_no\tpart_no\tdescription\tuoc\tquantity"


def _strip_fences(text: str) -> str:
    """Remove markdown code fences that models sometimes add despite instructions."""
    lines = text.strip().splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines)


def _enforce_tsv(text: str) -> str:
    """Sanitize model output into clean TSV:
    - Drop prose lines (no tabs)
    - Drop data rows where both item_no and part_no are empty
    - Strip any 'UOC:' prefix left in the uoc column
    - Guarantee the header is always the first line
    """
    import re
    lines = text.strip().splitlines()
    tsv_lines = [l for l in lines if "\t" in l]
    if not tsv_lines:
        return HEADER

    if tsv_lines[0].strip() != HEADER:
        tsv_lines.insert(0, HEADER)

    result = [tsv_lines[0]]
    for line in tsv_lines[1:]:
        cols = line.split("\t")
        item_no = cols[0].strip() if len(cols) > 0 else ""
        part_no = cols[1].strip() if len(cols) > 1 else ""
        if not item_no and not part_no:
            continue
        if len(cols) > 3:
            cols[3] = re.sub(r'(?i)^UOC:\s*', '', cols[3].strip())
        result.append("\t".join(cols))

    return "\n".join(result)


def _fix_numeric_o(text: str) -> str:
    """Replace letter O with digit 0 only when between digits or dots."""
    import re
    return re.sub(r'(?<=[0-9.])O(?=[0-9.])', '0', text)


# ---------------------------------------------------------------------------
# Extraction functions (one per provider)
# ---------------------------------------------------------------------------

def extract_anthropic(client, image_path: Path, model: str, max_tokens: int) -> str:
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": [
            {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": _b64(image_path)}},
            {"type": "text", "text": EXTRACTION_PROMPT},
        ]}],
    )
    return _enforce_tsv(_fix_numeric_o(_strip_fences(response.content[0].text)))


def extract_claude_azure(client, image_path: Path, model: str, max_tokens: int) -> str:
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": [
            {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": _b64(image_path)}},
            {"type": "text", "text": EXTRACTION_PROMPT},
        ]}],
    )
    return _enforce_tsv(_fix_numeric_o(_strip_fences(response.content[0].text)))


def extract_azure_openai(client, image_path: Path, deployment: str, max_tokens: int) -> str:
    response = client.chat.completions.create(
        model=deployment,
        max_tokens=max_tokens,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{_b64(image_path)}", "detail": "high"}},
                {"type": "text", "text": EXTRACTION_PROMPT},
            ]},
        ],
    )
    return _enforce_tsv(_fix_numeric_o(_strip_fences(response.choices[0].message.content)))


# ---------------------------------------------------------------------------
# Client / extractor factory
# ---------------------------------------------------------------------------

def _get_key(env_var: str) -> str:
    key = os.environ.get(env_var, "")
    if not key:
        sys.exit(f"ERROR: environment variable '{env_var}' is not set")
    return key


def build_extractor(cfg: dict):
    """Return (label, extractor_fn) based on config provider."""
    provider = cfg.get("provider", "anthropic")

    if provider == "anthropic":
        import anthropic
        pcfg   = cfg.get("anthropic", {})
        model  = pcfg.get("model", "claude-sonnet-4-5-20251001")
        maxtok = int(pcfg.get("max_tokens", 8192))
        client = anthropic.Anthropic()
        return f"anthropic/{model}", lambda p: extract_anthropic(client, p, model, maxtok)

    if provider == "claude_azure":
        import anthropic
        pcfg       = cfg["claude_azure"]
        deployment = pcfg["deployment"]
        maxtok     = int(pcfg.get("max_tokens", 8192))
        client = anthropic.Anthropic(
            api_key=_get_key(pcfg["api_key_env"]),
            base_url=pcfg["endpoint"],
        )
        return f"claude_azure/{deployment}", lambda p: extract_claude_azure(client, p, deployment, maxtok)

    if provider == "azure_openai":
        from openai import OpenAI
        pcfg       = cfg["azure_openai"]
        deployment = pcfg["deployment"]
        maxtok     = int(pcfg.get("max_tokens", 8192))
        client = OpenAI(api_key=_get_key(pcfg["api_key_env"]), base_url=pcfg["endpoint"])
        return f"azure_openai/{deployment}", lambda p: extract_azure_openai(client, p, deployment, maxtok)

    sys.exit(f"ERROR: unknown provider '{provider}'. Choose: anthropic | claude_azure | azure_openai")


# ---------------------------------------------------------------------------
# Dataset resolution
# ---------------------------------------------------------------------------

def resolve_datasets(results_dir: Path, datasets_cfg) -> list[Path]:
    if datasets_cfg == "*" or datasets_cfg is None:
        return sorted(results_dir.glob("data-*"))
    return [results_dir / name for name in datasets_cfg]


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------

def process_batch(cfg: dict, dataset_overrides: list[str] | None = None, output_override: str | None = None) -> None:
    pcfg        = cfg["pipeline"]
    results_dir = Path(pcfg["results_dir"])
    tables_sub  = pcfg.get("tables_subdir", "tables")
    skip        = pcfg.get("skip_existing", True)
    delay       = float(pcfg.get("delay_seconds", 0.3))
    max_retries = int(pcfg.get("max_retries", 3))
    backoff     = float(pcfg.get("retry_backoff", 2.0))

    # Output path strategy:
    # - output_subdir set → results_dir/<dataset>/<output_subdir>/page_N.tsv  (pipeline default)
    # - output_subdir not set → output_dir/<dataset>/page_N.tsv               (standalone)
    output_subdir = pcfg.get("output_subdir", "")
    if output_override:
        # CLI --output-dir always overrides to standalone mode
        _out_base = Path(output_override)
        def _out_path(ds: Path, png: Path) -> Path:
            return _out_base / ds.name / (png.stem + ".tsv")
        output_label = str(_out_base)
    elif output_subdir:
        def _out_path(ds: Path, png: Path) -> Path:
            return results_dir / ds.name / output_subdir / (png.stem + ".tsv")
        output_label = f"<results_dir>/<dataset>/{output_subdir}/"
    else:
        _out_base = Path(pcfg["output_dir"])
        def _out_path(ds: Path, png: Path) -> Path:
            return _out_base / ds.name / (png.stem + ".tsv")
        output_label = str(_out_base)

    label, do_extract = build_extractor(cfg)

    datasets_cfg  = dataset_overrides if dataset_overrides else pcfg.get("datasets", "*")
    dataset_paths = resolve_datasets(results_dir, datasets_cfg)

    missing = [d for d in dataset_paths if not d.exists()]
    if missing:
        sys.exit(f"ERROR: dataset directories not found: {[str(m) for m in missing]}")

    work: list[tuple[Path, Path]] = []
    for ds in dataset_paths:
        for png in sorted((ds / tables_sub).glob("*.png")):
            work.append((png, _out_path(ds, png)))

    if not work:
        sys.exit(f"No PNG files found across: {[d.name for d in dataset_paths]}")

    print(f"Provider   : {label}")
    print(f"Results dir: {results_dir}")
    print(f"Datasets   : {[d.name for d in dataset_paths]}")
    print(f"Output     : {output_label}")
    print(f"Total PNGs : {len(work)}")
    print()

    succeeded = skipped = failed = 0

    for idx, (png_path, out_path) in enumerate(work, 1):
        label_img = f"{png_path.parent.parent.name}/{png_path.name}"
        prefix    = f"[{idx}/{len(work)}] {label_img}"

        if skip and out_path.exists():
            print(f"{prefix} — skipped (exists)")
            skipped += 1
            continue

        out_path.parent.mkdir(parents=True, exist_ok=True)

        attempt, wait = 0, delay
        while attempt <= max_retries:
            try:
                print(f"{prefix} — extracting ...", end="", flush=True)
                tsv = do_extract(png_path)
                out_path.write_text(tsv, encoding="utf-8")
                print(f" saved → {out_path.name}")
                succeeded += 1
                break
            except Exception as exc:
                err = str(exc)
                is_rate_limit = "rate" in err.lower() or "429" in err
                attempt += 1
                if attempt > max_retries or not is_rate_limit:
                    print(f" ERROR: {exc}", file=sys.stderr)
                    failed += 1
                    break
                print(f" rate limited, retry {attempt}/{max_retries} in {wait:.1f}s ...")
                time.sleep(wait)
                wait *= backoff

        if idx < len(work):
            time.sleep(delay)

    print()
    print(f"Done — succeeded: {succeeded}, skipped: {skipped}, failed: {failed}")


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def load_config(config_path: Path) -> dict:
    with open(config_path) as fh:
        return yaml.safe_load(fh)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract parts-list tables from PNG images using a vision model (Claude or GPT-4o)"
    )
    parser.add_argument("--config", type=Path, default=Path(__file__).parent / "config.yaml",
                        help="Path to config.yaml (default: config.yaml next to this script)")
    parser.add_argument("--datasets", nargs="+", metavar="DATASET",
                        help="Dataset subdirectory names to process (e.g. data-1 data-2); omit or use '*' for all")
    parser.add_argument("--output-dir", metavar="DIR",
                        help="Override output directory (standalone mode: writes to DIR/<dataset>/page_N.tsv)")
    args = parser.parse_args()

    if not args.config.exists():
        sys.exit(f"ERROR: config file not found: {args.config}")

    cfg = load_config(args.config)
    datasets = args.datasets
    if datasets and len(datasets) == 1 and datasets[0] == "*":
        datasets = None

    process_batch(cfg, datasets, args.output_dir)


if __name__ == "__main__":
    main()
