from unstructured.partition.auto import partition
from unstructured.partition.pdf import partition_pdf
import json

elements = partition_pdf(
                filename="/home/prahitha.movva03/data/pdfs/data-8.pdf",
                strategy="hi_res",
                skip_infer_table_types=False,
                infer_table_structure=True
            )

# Convert elements to JSON
elements_json = [el.to_dict() for el in elements]
print(elements[0].text)
print(elements[0].metadata.text_as_html)
# # Save JSON metadata
with open("pdf_metadata.json", "w") as f:
    json.dump(elements_json, f, indent=4)

print("JSON metadata saved to pdf_metadata.json")