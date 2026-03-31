# from unstructuredio import UnstructuredPdf, TableImages
# from unstructuredio import extract_figures_tables_pages

# tables = TableImages('/home/prahitha.movva03/output/data-8/pdf_metadata.json', '')
# tables._merge_image_caption_table('/home/prahitha.movva03/output/data-8/pdf_metadata.json')

from unstructuredio import UnstructuredPdf, TableImages

# step 1: get elements + metadata from PDF (writes images_tables_metadata.json)
# data = UnstructuredPdf('/home/prahitha.movva03/data/pdfs/data-7-2.pdf')
# data.extract_metadata()

# step 2: extract figure/table page numbers and pair figure -> tables (by document order)
# Run after extract_metadata(). Output: figures_tables_pages.json with:
#   - figures: list of {figure_number, text, page_number} for "Figure N. ..." elements
#   - tables: list of {page_number, element_id} for type "Table"
#   - pairing: { "1": {figure_pages: [...], table_pages: [...]}, ... } for extracting
#     PNGs and associating each figure with its table(s)
# result_dir = "/home/prahitha.movva03/engineering-diagrams-parsing/ExtractImages/data/results/mar13/data-7"
# extract_figures_tables_pages(f"{result_dir}/images_tables_metadata.json", output_path=f"{result_dir}/figures_tables_pages.json")
# Or: python extract_figures_tables_pages.py <path/to/images_tables_metadata.json>

# step 3 (optional): get PNG images of table pages
# tables = TableImages('.../tables_data.json', '/path/to/data-7-2.pdf')
# tables._get_tables(result_dir)

# tables = TableImages('/home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-4/tables_data.json', '')
# tables._merge_image_caption_table('/home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-4/tables_data.json')

# sbt "runMain org.allenai.pdffigures2.FigureExtractorBatchCli /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/pdfs/data-5.pdf -s /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-5/images.json -m /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-5/"
# sbt -J-Xmx64G "runMain org.allenai.pdffigures2.FigureExtractorBatchCli /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/pdfs/data-5-1-100.pdf -s /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-5/images.json -m /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/result_data/data-5/images/"
# sbt -J-Xmx64G "runMain org.allenai.pdffigures2.FigureExtractorBatchCli /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/pdfs/data-4-1-100.pdf -s /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-4/images.json -m /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-4/images/"
# python -m olmocr.pipeline /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-4/table_data/ --markdown --pdfs /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-4/tables/
# chandra /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-5/tables /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-5/table_data --method vllm
