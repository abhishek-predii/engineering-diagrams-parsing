## ExtractImages dataset generation process

This folder contains the scripts that take a PDF engineering manual and produce a dataset of:

- **Figure images** (rendered PNGs)
- **Table images** (rendered PNGs)
- **Chandra OCR output** for the tables (HTML/MD)
- **Normalized table CSVs** (consistent headers, tab-separated)
- **Dataset manifests** that link `figure_path -> csv_path`

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
- `ExtractImages/data/results/<pdf_id>/table_data/page_<N>/page_<N>.html` (and `.md`)
- `ExtractImages/data/results/<pdf_id>/table_data/page_<N>/page_<N>.csv`
- `ExtractImages/data/results/<pdf_id>/dataset_manifest_vllm.tsv`

And optionally one global manifest:

- `ExtractImages/data/results/dataset_manifest_global_vllm.tsv`
- `ExtractImages/data/results/dataset_manifest_global_vllm_rel.tsv`

See `ExtractImages/data/README.md` for the detailed directory layout.

---

## 2. Step-by-step pipeline

NOTE: 

Make sure chandra is running on a port using: 
```
vllm serve datalab-to/chandra --served-model-name chandra --max-model-len 16384 --port 8000
```

To run steps 1-4:
```
python3 run_pipeline.py <path to PDF> --output-dir march30 --extract-pages --run-chandra --chandra-method vllm
```

### Step 1–2: Unstructured metadata -> figure/table page mapping

Script: `run_pipeline.py`

This step runs Unstructured on the PDF to produce `images_tables_metadata.json`, then parses it into:

- `figures_tables_pages.json` (figure captions + table pages + pairing)

Command (full run, including later steps if flags are used):

```bash
cd ExtractImages
python3 run_pipeline.py <path to PDF> \
  --output-dir <path to store outputs at> \
  --extract-pages
```

Notes:

- If `images_tables_metadata.json` already exists in the target `--output-dir`, Step 1 is skipped.
- If you want to skip Unstructured entirely, use `--from-metadata`:

```bash
python3 run_pipeline.py \
  --from-metadata <path to metadata JSON file> \
  --pdf <path to PDF> \
  --extract-pages
```

### Step 3: Render required pages to PNG (figures/ and tables/)

Enabled by `--extract-pages`.

The script renders only the page numbers that appear in `figures_tables_pages.json`, and will **skip** rendering if all required PNGs already exist.

Outputs:

- `data/results/<pdf_id>/figures/page_<N>.png`
- `data/results/<pdf_id>/tables/page_<N>.png`

### Step 4: Run Chandra OCR over tables

You can run Chandra per PDF with `run_pipeline.py --run-chandra --chandra-method vllm`, but for bulk processing `run_chandra_batch.py` is recommended.

Batch runner:

```bash
cd /home/prahitha.movva03/engineering-diagrams-parsing/ExtractImages

python3 run_chandra_batch.py \
  --base-dir /home/prahitha.movva03/engineering-diagrams-parsing/ExtractImages/data/results \
  --method vllm \
  --output-subdir table_data
```

Behavior:

- It finds each `data/results/<pdf_id>/tables/` directory that contains PNGs.
- It runs:
  - `chandra <tables_dir> <result_dir>/<output-subdir> --method <method>`
- Default output subdir is `table_data` to avoid overwriting previous runs.

Practical note (from your experience):

- Some environments/methods can fail on certain pages due to CUDA/CUBLAS issues.
- When it worked for you, it was with `chandra ... --method vllm` on a single image.
- For robustness, start with smaller batches and/or validate the first few pages before launching a large run.

### Step 5: Convert Chandra HTML -> normalized CSV + manifest

Convert HTML tables to per-page CSV:

```bash
python3 html_tables_to_csv.py data/results/<pdf_id>/table_data
```

Create per-PDF dataset manifest linking figure images to table CSVs:

```bash
python3 html_tables_to_csv.py data/results/<pdf_id>/table_data \
  --merged data/results/<pdf_id>/dataset_manifest_vllm.tsv
```

Manifest columns:

- `figure_path`
- `csv_path`
- `figure` (figure key from `figures_tables_pages.json`)

The CSVs are written with **consistent normalized headers** (`column_0`, `column_1`, …) and use **tab separation** so commas in descriptions don’t break parsing.

---

## 3. Create a global manifest

If you already generated per-PDF `dataset_manifest_vllm.tsv` files, concatenate them and add a `pdf_id`.

Run from `ExtractImages/`:

```bash
cd ExtractImages

python3 - << 'PY'
import os
import pandas as pd

base = "data/results"
manifests = []
for name in sorted(os.listdir(base)):
    d = os.path.join(base, name)
    if not os.path.isdir(d):
        continue
    mf = os.path.join(d, "dataset_manifest_vllm.tsv")
    if not os.path.isfile(mf):
        continue
    df = pd.read_csv(mf, sep="\t", dtype=str)
    df["pdf_id"] = name
    manifests.append(df)

global_df = pd.concat(manifests, ignore_index=True)
out = os.path.join(base, "dataset_manifest_global_vllm.tsv")
global_df.to_csv(out, sep="\t", index=False)
print("Wrote", out, "rows=", len(global_df))
PY
```

If you want a version with absolute prefixes removed, you can create a “rel” manifest as you already did.

---

## 4. What to use for “upload figure image, extract components, compare to table”

Use the global (or per-PDF) dataset manifest:

- Load the figure image from `figure_path`
- Load ground-truth components from `csv_path`
- Compare model output to the CSV rows/columns for that table page

If a table page is shared between multiple figures, you will see multiple rows that reference the same `csv_path` but different `figure` values.

---

## 5. Key scripts summary

- `run_pipeline.py`: Unstructured metadata -> mapping -> (optional) page rendering -> (optional) run chandra OCR
- `extract_figures_tables_pages.py`: parse `images_tables_metadata.json` -> `figures_tables_pages.json`
- `page_images.py`: render pages to PNG and call chandra
- `run_chandra_batch.py`: bulk chandra OCR over `tables/` folders
- `html_tables_to_csv.py`: HTML -> normalized CSV + dataset manifest
- `create_data7_dataset.py`: older dataset builder for the `merged_data.json` format (kept for reference)

