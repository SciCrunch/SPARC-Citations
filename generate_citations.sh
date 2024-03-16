#!/bin/bash

current_date=$(date +"%Y%m%d")

echo "Starting Citation Generation - $current_date"
echo "    Log file: /home/ec2-user/SPARC-Citations/logs/run_$current_date.log"

python3 /home/ec2-user/SPARC-Citations/data_citations.py >> /home/ec2-user/SPARC-Citations/logs/run_$current_date.log 2>&1

python3 /home/ec2-user/SPARC-Citations/output_citations.py >> /home/ec2-user/SPARC-Citations/logs/run_$current_date.log 2>&1

/home/ec2-user/SPARC-Citations/pandoc-3.1/bin/pandoc --pdf-engine wkhtmltopdf /home/ec2-user/SPARC-Citations/citation_markdown.md -o /home/ec2-user/SPARC-Citations/citation_markdown.pdf 

python3 /home/ec2-user/SPARC-Citations/export_tsv.py