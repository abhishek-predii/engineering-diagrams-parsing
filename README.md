# Extracting Diagrams 
[PdftoFigures](https://github.com/allenai/pdffigures2)
- Pdfs that are collected have caption with figure number 
- This repo is used to get all the figures related to the particular pdf 
- Some of the figures are jbig2 format (with dotted lines), added dependencies as mentioned in documentation to extract those as well
- Refer to these commands used to replicate the process for other pdfs. 

```
sbt "runMain org.allenai.pdffigures2.FigureExtractorBatchCli /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/pdfs/data-5.pdf -s /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-5/images.json -m /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-5/"

sbt -J-Xmx64G "runMain org.allenai.pdffigures2.FigureExtractorBatchCli /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/pdfs/data-5-1-100.pdf -s /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-5/images.json -m /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/result_data/data-5/images/"

sbt -J-Xmx64G "runMain org.allenai.pdffigures2.FigureExtractorBatchCli /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/pdfs/data-4-1-100.pdf -s /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-4/images.json -m /home/nagaharshitamarupaka/engineering-diagrams-parsing/ExtractImages/data/results/data-4/images/"
```

# Extracting Table as png
[chandraocr](https://github.com/datalab-to/chandra) 
- Run the OCR model in vllm api server 
```
vllm serve datalab-to/chandra --model-name chandra --max-model-len 16384 --port 8000

chandra input.pdf ./output --method vllm
```

# Extracting metadata for tables and figures
[unstructured](https://github.com/Unstructured-IO/unstructured)
- Refer to the code mentioned in ExtractImages/unstructuredio.py for the methods used present in the documentation 

# HTML to CSV file 
To enhance the accuracy for tabular data, converted HTML files to csv using beautiful soup library 
[mdtable2csv](https://github.com/tomroy/mdtable2csv)

```
cd mdtable2csv
bash script.sh 
bash csv_merge.sh 
```


