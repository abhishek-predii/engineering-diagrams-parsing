from unstructured.partition.pdf import partition_pdf
import json, os, re

# Match "Figure N." at start of text (e.g. "Figure 3. Turbine Axial Compressor (Sheet 1 of 2)")
FIGURE_TEXT_RE = re.compile(r"^Figure\s+(\d+)\s*[.\s]", re.IGNORECASE)


class UnstructuredPdf:
    def __init__(self, filename, result_dir=None):
        self.filename = os.path.abspath(filename)
        if result_dir is None:
            # Default: data/results/<pdf_stem> under this package
            base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            stem = os.path.splitext(os.path.basename(self.filename))[0]
            result_dir = os.path.join(base, "data", "results", stem)
        self.result_dir = os.path.abspath(result_dir)

    def extract_metadata(self, strategy="hi_res"):
        os.makedirs(self.result_dir, exist_ok=True)
        elements = partition_pdf(
            filename=self.filename,
            strategy=strategy,
            extract_images_in_pdf=True,
            extract_image_block_types=["Table"],
            extract_image_block_to_payload=True,
        )

        metadata_json = [el.to_dict() for el in elements]
        out_path = os.path.join(self.result_dir, "images_tables_metadata.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(metadata_json, f, indent=4)
        print(f"Wrote {out_path}")
        return out_path

    def extract_figures(self):
        fig_dir = os.path.join(self.result_dir, "figures")
        os.makedirs(fig_dir, exist_ok=True)
        try:
            elements = partition_pdf(
                filename=self.filename,
                strategy="hi_res",
                extract_images_in_pdf=True,
                extract_image_block_types=["Image"],
                extract_image_block_to_payload=False,
                extract_image_block_output_dir=fig_dir,
            )
            print(f"Figures written to {fig_dir}")
        except Exception as e:
            print(e)

    def extract_tables(self):
        os.makedirs(self.result_dir, exist_ok=True)
        try:
            elements = partition_pdf(
                filename=self.filename,
                strategy="hi_res",
                skip_infer_table_types=False,
                infer_table_structure=True,
            )
            tables_data = [el.to_dict() for el in elements]
            out_path = os.path.join(self.result_dir, "tables_data.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(tables_data, f, indent=4)
            print(f"Wrote {out_path}")
        except Exception as e:
            print(e)


def extract_figures_tables_pages(metadata_path, output_path=None):
    """
    Parse images_tables_metadata.json from extract_metadata() and extract:
    - Figures: elements whose text matches "Figure N. ..." (any type). Each caption
      is a separate figure (multi-sheet: "Sheet 1 of 2" and "Sheet 2 of 2" are two
      distinct figures, since they are different parts-explosion views).
    - Tables: elements with type "Table"; store page_number.
    - Pairing: by document order. Tables that follow an entire "Figure N" block
      (all its sheets) are assigned to every sheet of that figure. Keys are
      "1", "2_1", "2_2", "3_1", "3_2", ... (figure_number, or figure_number_sheetindex
      for multi-sheet).

    Returns dict with keys: figures, tables, pairing.
    If output_path is set, writes the result JSON there.
    """
    with open(metadata_path, "r", encoding="utf-8") as f:
        elements = json.load(f)

    figures = []  # list of {figure_number, sheet_index, text, page_number, element_index}
    tables = []   # list of {page_number, element_id, element_index}
    # Current "Figure N" group: list of (page_number, text) for each sheet seen so far
    current_figure_num = None
    current_group = []  # [(page_number, text), ...]
    tables_since_group_start = []  # table page numbers since we entered this figure number
    pairing = {}  # key -> {figure_pages: [page], figure_texts: [text], table_pages: [...]}
    pair_key_counter = {}  # per figure_number, count of sheets so far (for key 2_1, 2_2)

    def flush_group():
        """Assign accumulated tables to every sheet in current group; then clear group."""
        nonlocal current_group, tables_since_group_start
        if not current_group or current_figure_num is None:
            return
        for i, (pnum, caption) in enumerate(current_group):
            key = str(current_figure_num) if len(current_group) == 1 else f"{current_figure_num}_{i + 1}"
            pairing[key] = {
                "figure_pages": [pnum] if pnum is not None else [],
                "figure_texts": [caption],
                "table_pages": list(tables_since_group_start),
            }
        current_group = []
        tables_since_group_start = []

    for idx, el in enumerate(elements):
        meta = el.get("metadata") or {}
        page_number = meta.get("page_number")
        elem_type = el.get("type")
        text = (el.get("text") or "").strip()

        # Figure: any element whose text starts with "Figure N."
        match = FIGURE_TEXT_RE.match(text)
        if match:
            fig_num = int(match.group(1))
            # Each caption is its own figure (Sheet 1 of 2 and Sheet 2 of 2 are separate)
            sheet_index = (pair_key_counter.get(fig_num, 0)) + 1
            pair_key_counter[fig_num] = sheet_index
            figures.append({
                "figure_number": fig_num,
                "sheet_index": sheet_index,
                "text": text,
                "page_number": page_number,
                "element_index": idx,
            })
            if current_figure_num is not None and fig_num != current_figure_num:
                flush_group()
            current_figure_num = fig_num
            current_group.append((page_number, text))
            continue

        # Table
        if elem_type == "Table":
            tables.append({
                "page_number": page_number,
                "element_id": el.get("element_id"),
                "element_index": idx,
            })
            if current_figure_num is not None and page_number is not None:
                if page_number not in tables_since_group_start:
                    tables_since_group_start.append(page_number)

    flush_group()

    result = {
        "figures": figures,
        "tables": tables,
        "pairing": pairing,
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, default=float)
        print(f"Wrote figures/tables pages and pairing to {output_path}")

    return result


