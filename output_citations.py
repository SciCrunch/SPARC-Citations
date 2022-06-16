import json
import requests

# Load citations data
citations_file = open('./dataset_data_citations.json', encoding='utf-8')
#citations_file = open('./debug-datasets.json')

citations = json.load(citations_file)

# Loop through and output Markdown
if "datasets" in citations:
    datasets = citations["datasets"]

    num_datasets = len(datasets)
    print("[INFO] Number of datasets to process: " + str(num_datasets))

    # Open file for writing citation markdown
    markdown_file = open("./citation_markdown.txt", "w", encoding='utf-8')
    markdown_file.write("# SPARC Dataset Citations \n \n")

    idx = 0
    while idx < num_datasets:
        dataset_record = datasets[idx]
        print("[INFO] Processing dataset: " + str(idx))

        citations = dataset_record["citations"]
        num_citations = len(citations)
        print("[INFO] Processing citations: " + str(num_citations))

        if num_citations > 0:
            name = dataset_record["name"]
            markdown_file.write("## " + name + " \n")

            doi = dataset_record["doi"]
            dataset_id = dataset_record["id"]
            dataset_version = dataset_record["version"]
            markdown_file.write(
                "**DOI:** " + str(doi) + " **Dataset ID:** " + str(dataset_id) + " **Dataset Version:** " + str(dataset_version) + " \n\n")

            dataset_citation = dataset_record["citation"]
            markdown_file.write("**Citation:** " + str(dataset_citation) + " \n \n")

            markdown_file.write("## Dataset Citations" + " \n")

            citation_idx = 0
            while citation_idx < num_citations:
                citation_type = citations[citation_idx]["type"]
                citation_curie = citations[citation_idx]["curie"]
                citation_text = citations[citation_idx]["citation"]

                markdown_file.write("    DOI: " + str(citation_curie) )

                if citation_type == 'Originating Publication':
                    markdown_file.write(" [Originating Publication] \n")
                elif citation_type == 'Protocol':
                    markdown_file.write(" [Protocol] \n")
                else:
                    markdown_file.write(" [Citation] \n")

                markdown_file.write("    Citation: " + str(citation_text) + " \n \n")

                citation_idx =citation_idx +1

        idx = idx + 1

    markdown_file.close()

else:
    print("[ERROR] No datasets")
    exit(0)

