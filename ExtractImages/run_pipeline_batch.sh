#!/bin/bash
# Run the full pipeline (metadata + figure/table pages + extract page images) for multiple PDFs.
# Requires: conda env with unstructured and pdf2image (e.g. conda activate vllm).
# Network: Step 1 needs to download layout model from Hugging Face once; ensure HF is accessible.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PDFS=(
  /home/prahitha.movva03/data/pdfs/data-2.pdf
  /home/prahitha.movva03/data/pdfs/data-4.pdf
  /home/prahitha.movva03/data/pdfs/data-5.pdf
  /home/prahitha.movva03/data/pdfs/data-7.pdf
  /home/prahitha.movva03/data/pdfs/data-8.pdf
)

for pdf in "${PDFS[@]}"; do
  echo "===== $pdf ====="
  python3 run_pipeline.py "$pdf" --extract-pages || true
done

echo "Batch done."
