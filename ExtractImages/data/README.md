## `ExtractImages/data/`

This folder holds **inputs** (PDFs) and **generated outputs** (metadata, page images, OCR, and dataset manifests) produced by the scripts in `ExtractImages/`.

### Directory layout

- **`pdfs/`**: Input PDFs to process.
  - Example: `pdfs/data-2.pdf`

- **`results/`**: Outputs for each processed PDF (one subfolder per PDF/run).
  - Example: `results/data-2/`, `results/data-8/`, `results/mar15/`

### Per-PDF result folder (`results/<pdf_id>/`)

Each `results/<pdf_id>/` folder can contain:

- **`images_tables_metadata.json`**
  - Raw Unstructured element metadata from `partition_pdf(...)`.

- **`figures_tables_pages.json`**
  - Extracted **figure captions** (by `"Figure N."` prefix), **table elements** (by `type == "Table"`), and a **pairing** mapping figures → table page numbers.

- **`figures/`**
  - Rendered figure pages as PNGs: `figures/page_<N>.png`

- **`tables/`**
  - Rendered table pages as PNGs: `tables/page_<N>.png`

- **`table_data/`**
  - Chandra OCR outputs for tables (HTML/MD/metadata per page).
  - Layout: `table_data/page_<N>/page_<N>.html` (and `page_<N>.md`, `page_<N>_metadata.json`)

- **`table_data_vllm/`**
  - Same as `table_data/`, but produced with `chandra --method vllm` (kept separate from `hf` runs).

- **`dataset_manifest_vllm.tsv`**
  - Per-PDF dataset manifest (tab-separated) for evaluation.
  - Columns: `figure_path`, `csv_path`, `figure`

### Global manifests (`results/`)

`results/` may also contain global manifests aggregated across result folders:

- **`dataset_manifest_global_vllm.tsv`**
  - Global manifest (tab-separated) with absolute paths (as generated).

- **`dataset_manifest_global_vllm_rel.tsv`**
  - Global manifest (tab-separated) with paths made relative to `ExtractImages/data/results/`.
  - Columns: `figure_path`, `csv_path`, `figure`, `pdf_id`

### How outputs are produced

- **Pipeline (PDF → metadata → pairing → page images)**:
  - `ExtractImages/run_pipeline.py`

- **Chandra OCR (batch over `tables/`)**:
  - `ExtractImages/run_chandra_batch.py` (writes `table_data_vllm/` by default)

- **HTML → CSV + dataset manifests**:
  - `ExtractImages/html_tables_to_csv.py`
  - Produces **tab-separated** `.csv` files (TSV) with normalized headers: `column_0`, `column_1`, ...

### Notes

- This directory can be **large** (images/OCR outputs/manifests). It’s typically treated as generated data and is commonly excluded from git history.

