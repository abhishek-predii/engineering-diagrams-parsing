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

### Step 5: Post-processing, validation, and manifest construction

Each TSV undergoes structural cleaning: near-empty columns are dropped and adjacent duplicate columns are merged. A TSV is accepted only if it retains both `item_no` and `description` columns with at least one data row, excluding non-parts-list pages. Figures linked to more than `--threshold` table pages are dropped (default: 5).

**Build per-PDF local manifest:**

```bash
python3 create_local_manifest.py data/results/<pdf_id>/table_data
python3 create_local_manifest.py data/results/<pdf_id>/table_data --threshold 3
```

Output — `data/results/<pdf_id>/dataset_manifest_local.tsv`:

| column | description |
|--------|-------------|
| `figure_path` | relative path to the figure PNG |
| `table_path` | relative path to the table PNG |
| `tsv_path` | relative path to the table TSV (`table_data/page_<N>.tsv`) |
| `figure` | figure key from `figures_tables_pages.json` |

**Merge into a global manifest:**

```bash
python3 create_global_manifest.py data/results
```

Options:

- `--output` / `-o` — output TSV path (default: `<results_dir>/dataset_manifest_global.tsv`)
- `--manifest-name` — per-PDF manifest filename (default: `dataset_manifest_local.tsv`)

---

## 3. Key scripts summary

| Script | Role |
|--------|------|
| `run_pipeline.py` | Full pipeline: Unstructured → mapping → page rendering → Claude extraction |
| `extract_figures_tables_pages.py` | Parse `images_tables_metadata.json` → `figures_tables_pages.json` |
| `page_images.py` | Render PDF pages to PNG; `run_claude_ocr()` for single-directory Claude extraction |
| `extract_tables.py` | **Core Claude extraction** — sends table PNGs to vision model, writes TSVs |
| `config.yaml` | Provider config (Anthropic / Azure Claude / Azure GPT-4o) and pipeline settings |
| `run_claude_batch.py` | **Bulk Claude extraction** over all `data-N/tables/` directories |
| `create_local_manifest.py` | Validate TSVs in `table_data/` + build per-PDF `dataset_manifest_local.tsv` |
| `create_global_manifest.py` | Combine per-PDF local manifests into `dataset_manifest_global.tsv` |

