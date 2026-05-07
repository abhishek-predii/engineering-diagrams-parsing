## ExtractImages dataset generation process

This folder contains the scripts that take a PDF engineering manual and produce a dataset of:

- **Figure images** (rendered PNGs)
- **Table images** (rendered PNGs)
- **Normalized table TSVs** (tab-separated, one per table page, produced by Claude)
- **Dataset manifests** that link `figure_path -> tsv_path`

The generated artifacts are stored under `ExtractImages/data/`.

---

## 1. Inputs and outputs (high level)

You start with:

- `ExtractImages/data/pdfs/<manual>.pdf`

You generate (per manual):

- `ExtractImages/data/results/<pdf_id>/images_tables_metadata.json`
- `ExtractImages/data/results/<pdf_id>/figures_tables_pages.json`
- `ExtractImages/data/results/<pdf_id>/figures/page_<N>.png`
- `ExtractImages/data/results/<pdf_id>/tables/page_<N>.png`
- `ExtractImages/data/results/<pdf_id>/table_data/page_<N>.tsv`
- `ExtractImages/data/results/<pdf_id>/dataset_manifest_local.tsv`

And optionally one global manifest:

- `ExtractImages/data/results/dataset_manifest_global.tsv`

---

## 2. Step-by-step pipeline

### Step 1–2: Unstructured metadata → figure/table page mapping

Script: `run_pipeline.py`

This step runs Unstructured on the PDF to produce `images_tables_metadata.json`, then parses it into:

- `figures_tables_pages.json` (figure captions + table pages + pairing)

```bash
cd ExtractImages
python3 run_pipeline.py <path/to/manual.pdf> \
  --output-dir <path to store outputs> \
  --extract-pages
```

Notes:

- If `images_tables_metadata.json` already exists in the target `--output-dir`, Step 1 is skipped.
- To skip Unstructured entirely and use an existing metadata file:

```bash
python3 run_pipeline.py \
  --from-metadata <path/to/images_tables_metadata.json> \
  --pdf <path/to/manual.pdf> \
  --extract-pages
```

### Step 3: Render required pages to PNG (figures/ and tables/)

Enabled by `--extract-pages`.

The script renders only the page numbers that appear in `figures_tables_pages.json`, skipping rendering if all required PNGs already exist.

Outputs:

- `data/results/<pdf_id>/figures/page_<N>.png`
- `data/results/<pdf_id>/tables/page_<N>.png`

### Step 4: Run Claude extraction over table images → TSVs

Claude is sent each table PNG and returns a clean TSV with five columns:
`item_no`, `part_no`, `description`, `uoc`, `quantity`.

TSVs are written directly to `table_data/page_<N>.tsv` — no intermediate HTML step.

**Prerequisites:**

```bash
pip install anthropic pyyaml
export ANTHROPIC_API_KEY=sk-...
```

Or configure an Azure provider in `config.yaml` and set `SUBSCRIPTION_KEY` instead.

**Option A — single PDF (via run_pipeline.py):**

```bash
python3 run_pipeline.py <path/to/manual.pdf> \
  --output-dir data/results/<pdf_id> \
  --extract-pages \
  --run-claude
```

To use a non-default config file:

```bash
python3 run_pipeline.py ... --run-claude --claude-config /path/to/config.yaml
```

**Option B — bulk processing across all datasets (recommended):**

```bash
python3 run_claude_batch.py
```

With options:

```bash
python3 run_claude_batch.py \
  --base-dir data/results \
  --datasets data-1 data-2 \
  --output-subdir table_data \
  --overwrite          # re-run images that already have a TSV
```

**Option C — extract_tables.py directly (standalone, output to a separate dir):**

```bash
python3 extract_tables.py --datasets data-7 data-8
python3 extract_tables.py --datasets "*"               # all datasets
python3 extract_tables.py --output-dir /tmp/tsv_out    # separate output dir
```

**Configuring the model/provider (`config.yaml`):**

```yaml
# Switch between providers by changing this line:
provider: anthropic          # native Anthropic API  (ANTHROPIC_API_KEY)
# provider: claude_azure     # Azure AI Foundry Claude (SUBSCRIPTION_KEY)
# provider: azure_openai     # Azure AI Foundry GPT-4o (SUBSCRIPTION_KEY)

anthropic:
  model: claude-opus-4-5
  max_tokens: 8192

pipeline:
  results_dir: ./data/results
  output_subdir: table_data   # TSVs land at results_dir/<dataset>/table_data/page_N.tsv
  skip_existing: true
```

### Step 4b: Extract UOC model code mappings from PDF

Each RPSTL PDF contains a "Usable on Code" section in its introduction (pages 10–70) that maps short alphanumeric codes (e.g. `MTH`, `H11`) to vehicle model designations (e.g. `M998`, `M1043 W/W`). This step scans those pages and writes a `uoc_model_codes.tsv` alongside the page TSVs.

**Prerequisites:**

```bash
apt install poppler-utils   # provides pdftoppm
# OR
brew install poppler
```

**Run for all PDFs:**

```bash
python3 extract_uoc_codes.py
```

**With options:**

```bash
python3 extract_uoc_codes.py \
  --pdfs-dir data/pdfs \
  --results-dir data/results \
  --output-subdir table_data \
  --datasets data-1 data-2 \
  --no-skip            # re-extract even if uoc_model_codes.tsv already exists
```

**Or via run_pipeline.py (single PDF):**

```bash
python3 run_pipeline.py <path/to/manual.pdf> \
  --output-dir data/results/<pdf_id> \
  --run-uoc
```

Output written to: `data/results/<pdf_id>/table_data/uoc_model_codes.tsv`

| Column | Description |
|--------|-------------|
| `code` | 2–5 char alphanumeric UOC (e.g. `BVY`, `H11`) |
| `model` | Vehicle model designation (e.g. `M1070`, `M998`) |

Non-UOC pages return `NONE` and are skipped. The scan halts on the first valid detection per PDF (each manual has exactly one UOC section).

---

### Step 5: Build per-PDF local manifest from TSVs

This step validates the flat `table_data/page_<N>.tsv` files and links them to figure images via `figures_tables_pages.json`.

Each TSV must have an `item_no` and a `description` column to be included; non-parts-list pages (index pages, etc.) are automatically skipped.

```bash
python3 create_local_manifest.py data/results/<pdf_id>/table_data
```

Output — `data/results/<pdf_id>/dataset_manifest_local.tsv`:

| column | description |
|--------|-------------|
| `figure_path` | relative path to the figure PNG |
| `table_path` | relative path to the table PNG |
| `tsv_path` | relative path to the table TSV (`table_data/page_<N>.tsv`) |
| `figure` | figure key from `figures_tables_pages.json` |

---

## 3. Create a global manifest

Once per-PDF `dataset_manifest_local.tsv` files exist, combine them into a single TSV:

```bash
python3 create_global_manifest.py <results_dir>
```

Options:

- `--output` / `-o` — output TSV path (default: `<results_dir>/dataset_manifest_global.tsv`)
- `--threshold` / `-t` — max TSV rows per figure to include (default: `5`)
- `--manifest-name` — per-PDF manifest filename (default: `dataset_manifest_local.tsv`)

Example:

```bash
python3 create_global_manifest.py data/results --threshold 5
```

---

## 4. What to use for "upload figure image, extract components, compare to table"

Use the global (or per-PDF) dataset manifest:

- Load the figure image from `figure_path`
- Load ground-truth components from `tsv_path`
- Compare model output to the TSV rows/columns for that table page

---

## 5. Key scripts summary

| Script | Role |
|--------|------|
| `run_pipeline.py` | Full pipeline: Unstructured → mapping → page rendering → Claude extraction |
| `extract_figures_tables_pages.py` | Parse `images_tables_metadata.json` → `figures_tables_pages.json` |
| `page_images.py` | Render PDF pages to PNG; `run_claude_ocr()` for single-directory Claude extraction |
| `extract_tables.py` | **Core Claude extraction** — sends table PNGs to vision model, writes TSVs |
| `config.yaml` | Provider config (Anthropic / Azure Claude / Azure GPT-4o) and pipeline settings |
| `run_claude_batch.py` | **Bulk Claude extraction** over all `data-N/tables/` directories |
| `extract_uoc_codes.py` | **UOC mapping extraction** — scans PDF intro pages, writes `uoc_model_codes.tsv` |
| `create_local_manifest.py` | Validate TSVs in `table_data/` + build per-PDF `dataset_manifest_local.tsv` |
| `create_global_manifest.py` | Combine per-PDF local manifests into `dataset_manifest_global.tsv` |

---

## 6. Legacy scripts (kept for reference)

These scripts were part of the old Chandra OCR pipeline and are no longer needed:

| Script | Description |
|--------|-------------|
| `run_chandra_batch.py` | Batch-ran Chandra OCR CLI → HTML files per table image |
| `html_tables_to_csv.py` | Converted Chandra HTML output → normalized CSV/TSV |

`--run-chandra` still works in `run_pipeline.py` but prints a deprecation warning.
