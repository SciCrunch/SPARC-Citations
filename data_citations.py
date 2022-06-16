import json
import requests
from requests.adapters import HTTPAdapter, Retry
import configparser
import re
from elasticsearch import Elasticsearch
import pandas as pd
import logging

logging.basicConfig(filename="./run.log",
					format='%(asctime)s %(message)s',
					filemode='w')
logger=logging.getLogger()
logger.setLevel(logging.DEBUG)

config = configparser.ConfigParser()
config.read(r"./config.py")

run_env = config.get('env', "run_env")

#####################################################################
# Function to retrieve text from URL with retries
def getURL(url, headers="NONE"):

    result = "[ERROR]"
    url_session = requests.Session()

    retries = Retry(total=6,
                    backoff_factor=1,
                    status_forcelist=[403, 404, 413, 429, 500, 502, 503, 504])

    url_session.mount('http://', HTTPAdapter(max_retries=retries))
    url_session.mount('https://', HTTPAdapter(max_retries=retries))

    success = 1

    try:
        if headers == "NONE":
            url_result = url_session.get(url)
        else:
            url_result = url_session.get(url, headers=headers)

        if url_result.status_code == 410:
            print("[WARNING] Retrieval Status 410 - URL Unpublished:" + url)
        else:
            url_result.raise_for_status()

    except requests.exceptions.HTTPError as errh:
        print("[ERROR] Retrieving URL - HTTP Error:", errh)
        success = 0
    except requests.exceptions.ConnectionError as errc:
        print("[ERROR] Retrieving URL - Error Connecting:", errc)
        success = 0
    except requests.exceptions.Timeout as errt:
        print("[ERROR] Retrieving URL - Timeout Error:", errt)
        success = 0
    except requests.exceptions.RequestException as err:
        print("[ERROR] Retrieving URL - Something Else", err)
        success = 0

    url_session.close()

    if success == 1:
        result = url_result
    else:
        result = {}

    return result

#####################################################################
# Function to retrieve formatted citation from Crossref
def getCrossrefCitation(doi2format):
    base_url = config.get('crosscite', "base_url")
    crosscite_url = base_url + "/" + doi2format

    url_session = requests.Session()

    retries = Retry(total=8,
                    backoff_factor=1,
                    status_forcelist=[403, 404, 413, 429, 500, 502, 503, 504])

    url_session.mount('http://', HTTPAdapter(max_retries=retries))
    url_session.mount('https://', HTTPAdapter(max_retries=retries))

    success = 1

    try:
        crossref_citation = url_session.get(crosscite_url, headers={"Accept": "text/x-bibliography", 'style': 'ieee-transactions-on-medical-imaging'})
        crossref_citation.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print ("[ERROR] Retrieving Citation - HTTP Error:",errh)
        success = 0
    except requests.exceptions.ConnectionError as errc:
        print ("[ERROR] Retrieving Citation - Error Connecting:",errc)
        success = 0
    except requests.exceptions.Timeout as errt:
        print ("[ERROR] Retrieving Citation - Timeout Error:",errt)
        success = 0
    except requests.exceptions.RequestException as err:
        print ("[ERROR] Retrieving Citation - Something Else",err)
        success = 0

    url_session.close()

    if success == 1:
        result = crossref_citation.text
    else:
        result = "CITATION[" + doi2format + "]"

    return result

#####################################################################
#####################################################################

print("[INFO] Citation processing started")
logger.info("Citation processing started")

citation_data = dict()

print("[INFO] Get datasets")
logger.info("Get datasets")
#####################################################################
# Get total count for pennsieve dataset
use_cache = config.get('env', "use_cache")
if use_cache == 'true':
    print("[INFO] Using cache")
    logger.info("Using Cache")

    json_file = open('./datasets-cache.json')
    citation_data = json.load(json_file)

    num_datasets = len(citation_data["datasets"])

    print("[INFO] Number of SPARC datasets: " + str(num_datasets))
    logger.info("Number of SPARC datasets: " + str(num_datasets))

else:
    base_url = config.get('pennsieve', "base_url")

    pennsieve = getURL(base_url + '?orderBy=date&orderDirection=asc')
    pen_totalCount = pennsieve.json()['totalCount']

    if run_env == "test":
        pen_totalCount = int(config.get('env', "num_datasets"))
        limit = 5
    else:
        limit = 25

    print("[INFO] Total number of datasets to process: " + str(pen_totalCount))
    logger.info("Total number of datasets to process: " + str(pen_totalCount))

    # Read pensieve dataset
    pen_data = []

    for offset in range(0, pen_totalCount, limit):
        pennsieve_url = base_url + '?limit=' + str(limit) + '&offset=' + str(offset) + '&orderBy=date&orderDirection=asc'

        each_pennsieve = getURL(pennsieve_url)

        each_pen_data = each_pennsieve.json()
        pen_data.append(each_pen_data)

    # Get Dataset DOIs
    citation_data["datasets"] = []
    for api_return in pen_data:
        datasets = api_return["datasets"]

        for dataset in datasets:
            if dataset["organizationId"] == 367:
                if "doi" in dataset:
                    doi = str(dataset["doi"])

                    formatted_citation = getCrossrefCitation(doi)

                    # If not first version then add prior version DOIs
                    doi_versions = []
                    if dataset["version"] > 0:
                        num_versions = dataset["version"]
                        current_dataset = dataset["id"]

                        if dataset["version"] > 1:
                            print("[INFO] Multiple versions (" + str(num_versions) + ") found for dataset: " + str(current_dataset))
                            logger.info("Multiple versions (" + str(num_versions) + ") found for dataset: " + str(current_dataset))
                        else:
                            print("[INFO] Single version (" + str(num_versions) + ") found for dataset: " + str(current_dataset))
                            logger.info("Single version (" + str(num_versions) + ") found for dataset: " + str(current_dataset))

                        idx = 1
                        while idx <= num_versions:
                            pennsieve_url = base_url + '/' + str(current_dataset) + '/versions/' + str(idx)

                            pennsieve_version = getURL(pennsieve_url)
                            dataset_version = pennsieve_version.json()

                            if "doi" in dataset_version:
                                doi = str(dataset_version["doi"])
                                print("####### (" + str(idx) + ") " + doi)
                                logger.info("####### (" + str(idx) + ") " + doi)

                                version_element = {"doi": doi, "version": dataset_version["version"]}

                                doi_versions.append(version_element)

                            idx = idx + 1

                    doi_element = {"doi": doi, "type": "Dataset", "name": dataset["name"], "id": dataset["id"],
                                   "version": dataset["version"], "citation": formatted_citation, "versions": doi_versions}
                    citation_data["datasets"].append(doi_element)

    print("[INFO] Get datasets finished")
    logger.info("Get datasets finished")

    #####################################################################
    # Setup elements needed
    num_datasets = len(citation_data["datasets"])

    print("[INFO] Number of SPARC datasets: " + str(num_datasets))
    print("[INFO] Configuring data structures")
    logger.info("Number of SPARC datasets: " + str(num_datasets))
    logger.info("Configuring data structures")

    idx = 0
    while idx < num_datasets:
        citation_record = citation_data["datasets"][idx]

        citation_record["citations"] = []
        citation_data["datasets"][idx] = citation_record

        idx = idx + 1

    print("[INFO] Configuring data structures finished")
    logger.info("Configuring data structures finished")

    with open('./initial-datasets.json', 'w') as outfile:
        json.dump(citation_data, outfile, sort_keys=True, indent=4)

print("[INFO] Add SPARC citations")
logger.info("Add SPARC citations")

#####################################################################
# Add Internal Metadata (Algolia)
num_datasets = len(citation_data["datasets"])

run_service = config.get('env', "sparc")
if run_service == 'off':
    print("[INFO] Skipping SPARC citations")
    logger.info("Skipping SPARC citations")
else:
    # Get config parameters
    es_user = config.get('elastic', "username")
    es_pwd = config.get('elastic', "password")
    base_url = config.get('elastic', "base_url")
    es_index = config.get('elastic', "index")

    idx = 0
    while idx < num_datasets:
        citation_record = citation_data["datasets"][idx]
        print('D[' + str(idx) + ']', end='', flush=True)

        #####################################################################
        # CODE HERE TO RETRIEVE AND MANIPULATE DATA
        # Make sure to set update citations field via .append
        # citation data: { "curie": ##DOI##, "relationship": ##DataCite Relationship##,
        #                  "type": ##Originating Article, Protocol, etc##, "comment": ####}
        # citation_record["citations"].append(new_citation)

        # Get data from ES Algolia

        try:
            es = Elasticsearch(
                base_url,
                verify_certs=False,
                ssl_show_warn=False,
                basic_auth=(es_user, es_pwd),
                max_retries=10,
                request_timeout=30
            )
        except:
            print("[ERROR] Elasticsearch connection failed")
            logger.error("Elasticsearch connection failed")

        dataset_id = citation_record["id"]

        try:
            resp = es.get(index=es_index, id=dataset_id)
        except:
            print("[ERROR] Dataset not found: " + str(dataset_id))
            logger.error("Dataset not found: " + str(dataset_id))

        algolia_dataset = resp["_source"]

        print('P', end='', flush=True)

        # Extract Protocol DOIs
        if "protocols" in algolia_dataset:
            protocols = algolia_dataset["protocols"]["primary"]
            for protocol in protocols:
                print('.', end='', flush=True)
                if "curie" in protocol:
                    new_curie = re.sub('DOI:', 'doi:', protocol["curie"], flags=re.IGNORECASE)
                    new_curie = re.sub('URL:', 'uri:', new_curie, flags=re.IGNORECASE)
                    new_curie = re.sub('URI:', 'uri:', new_curie, flags=re.IGNORECASE)
                    new_rel = "IsDocumentedBy"
                    new_type = "Protocol"
                    new_comment = ""
                    new_source = "SPARC"

                    # If new citation not in citations then add
                    citation_set = citation_record["citations"]

                    append = 1
                    for orig_citation in citation_set:
                        if "curie" in orig_citation:
                            if orig_citation["curie"] == new_curie:
                                print('=', end='', flush=True)
                                append = 0

                    if append == 1:
                        # Create New Citation Record
                        formatted_citation = getCrossrefCitation(re.sub("doi:", "", new_curie))
                        new_citation = {"curie": new_curie, "relationship": new_rel, "type": new_type,
                                        "comment": new_comment, "citation": formatted_citation, "source": new_source}

                        citation_record["citations"].append(new_citation)

        print('O', end='', flush=True)

        # Extract publication DOIs
        if "publications" in algolia_dataset:
            publications = algolia_dataset["publications"]["originating"]
            for publication in publications:
                print('.', end='', flush=True)
                if "curie" in publication:
                    new_curie = re.sub('DOI:', 'doi:', publication["curie"], flags=re.IGNORECASE)
                    new_curie = re.sub('URL:', 'uri:', new_curie, flags=re.IGNORECASE)
                    new_curie = re.sub('URI:', 'uri:', new_curie, flags=re.IGNORECASE)
                    new_rel = "Describes"
                    new_type = "Originating Publication"
                    new_comment = ""
                    new_source = "SPARC"

                    # If new citation not in citations then add
                    citation_set = citation_record["citations"]

                    append = 1
                    for orig_citation in citation_set:
                        if "curie" in orig_citation:
                            if orig_citation["curie"] == new_curie:
                                print('=', end='', flush=True)
                                append = 0

                    if append == 1:
                        # Create New Citation Record
                        formatted_citation = getCrossrefCitation(re.sub("doi:", "", new_curie))
                        new_citation = {"curie": new_curie, "relationship": new_rel, "type": new_type,
                                        "comment": new_comment, "citation": formatted_citation, "source": new_source}

                        citation_record["citations"].append(new_citation)

        #####################################################################

        # Replace citation record
        citation_data["datasets"][idx] = citation_record

        idx = idx + 1
    print('|', flush=True)

    print("[INFO] Add SPARC citations finished")
    logger.info("Add SPARC citations finished")

    with open('./datasets-cache.json', 'w') as outfile:
        json.dump(citation_data, outfile, sort_keys=True, indent=4)

print("[INFO] Add OCI citations")
logger.info("Add OCI citations")
#####################################################################
# Add Metadata (Open Citations)
# https://opencitations.net

num_datasets = len(citation_data["datasets"])

run_service = config.get('env', "oci")
if run_service == 'off':
    print("[INFO] Skipping OCI citations")
    logger.info("Skipping OCI citations")
else:

    # Get config parameters
    base_url = config.get('oci', "base_url")

    idx = 0
    while idx < num_datasets:
        citation_record = citation_data["datasets"][idx]
        print('D[' + str(idx) + ']', end='', flush=True)

        if "versions" in citation_record:
            dataset_versions = citation_record["versions"]
            for version_record in dataset_versions:
                print('V', end='', flush=True)
                if "doi" in version_record:

                    #####################################################################
                    # CODE HERE TO RETRIEVE AND MANIPULATE DATA
                    # Make sure to set update citations field via .append
                    # citation data: { "curie": ##DOI##, "relationship": ##DataCite Relationship##,
                    #                  "type": ##Originating Article, Protocol, etc##, "comment": ####}
                    # citation_record["citations"].append(new_citation)

                    dataset_doi = version_record["doi"]

                    oci_url = base_url + "/" + dataset_doi

                    oci_citations = getURL(oci_url)
                    new_citations = oci_citations.json()

                    num_citations = len(new_citations)

                    if num_citations > 0:
                        for citation in new_citations:
                            print('.', end='', flush=True)
                            if "citing" in citation:
                                new_curie = re.sub('coci => ', 'doi:', citation["citing"], flags=re.IGNORECASE)
                                new_curie = re.sub('DOI:', 'doi:', new_curie, flags=re.IGNORECASE)
                                new_curie = re.sub('URL:', 'uri:', new_curie, flags=re.IGNORECASE)
                                new_curie = re.sub('URI:', 'uri:', new_curie, flags=re.IGNORECASE)
                                new_rel = "Cites"
                                new_type = "Work"
                                new_comment = ""
                                new_source = "OCI"

                                # If new citation not in citations then add
                                citation_set = citation_record["citations"]

                                append = 1
                                for orig_citation in citation_set:
                                    if "curie" in orig_citation:
                                        if orig_citation["curie"] == new_curie:
                                            print('=', end='', flush=True)
                                            append = 0

                                if append == 1:
                                    # Create New Citation Record
                                    formatted_citation = getCrossrefCitation(re.sub("doi:", "", new_curie))
                                    new_citation = {"curie": new_curie, "relationship": new_rel, "type": new_type,
                                                    "comment": new_comment, "citation": formatted_citation, "source": new_source}

                                    citation_record["citations"].append(new_citation)

        idx = idx + 1
    print('|', flush=True)

    print("[INFO] Add OCI citations finished")
    logger.info("Add OCI citations finished")

    with open('./datasets-cache.json', 'w') as outfile:
        json.dump(citation_data, outfile, sort_keys=True, indent=4)

print("[INFO] Add DataCite citations")
logger.info("Add DataCite citations")
#####################################################################
# Add Metadata (DataCite)
# https://opencitations.net

num_datasets = len(citation_data["datasets"])

run_service = config.get('env', "datacite")
if run_service == 'off':
    print("[INFO] Skipping Datacite citations")
    logger.info("Skipping Datacite citations")
else:

    # Get config parameters
    base_url = config.get('datacite', "base_url")

    idx = 0
    while idx < num_datasets:
        citation_record = citation_data["datasets"][idx]
        print('D[' + str(idx) + ']', end='', flush=True)

        if "versions" in citation_record:
            dataset_versions = citation_record["versions"]
            for version_record in dataset_versions:
                print('V', end='', flush=True)
                if "doi" in version_record:

                    #####################################################################
                    # CODE HERE TO RETRIEVE AND MANIPULATE DATA
                    # Make sure to set update citations field via .append
                    # citation data: { "curie": ##DOI##, "relationship": ##DataCite Relationship##,
                    #                  "type": ##Originating Article, Protocol, etc##, "comment": ####}
                    # citation_record["citations"].append(new_citation)

                    dataset_doi = version_record["doi"]

                    datacite_url = base_url + "/" + dataset_doi

                    datacite_citations = getURL(datacite_url)

                    if str(datacite_citations) == '{}':
                        datacite_record = dict()
                    else:
                        datacite_record = datacite_citations.json()

                    num_citations = 0
                    if "data" in datacite_record:
                        tmp_record = datacite_record["data"]
                        if "relationships" in tmp_record:
                            tmp_record = tmp_record["relationships"]
                            if "citations" in tmp_record:
                                tmp_record = tmp_record["citations"]
                                if "data" in tmp_record:
                                    tmp_record = tmp_record["data"]

                                    new_citations = tmp_record
                                    num_citations = len(new_citations)

                    if num_citations > 0:
                        for citation in new_citations:
                            print('.', end='', flush=True)
                            if "id" in citation:
                                if "type" in citation:
                                    if citation["type"] == "dois":
                                        new_curie = 'doi:' + citation["id"]
                                    else:
                                        new_curie = citation["id"]
                                        print("[WARNING] Unknown citation type found")
                                        logger.warning("Unknown citation type found")

                                new_rel = "Cites"
                                new_type = "Work"
                                new_comment = ""
                                new_source = "Datacite"

                                # If new citation not in citations then add
                                citation_set = citation_record["citations"]

                                append = 1
                                for orig_citation in citation_set:
                                    if "curie" in orig_citation:
                                        if orig_citation["curie"] == new_curie:
                                            print('=', end='', flush=True)
                                            append = 0

                                if append == 1:
                                    # Create New Citation Record
                                    formatted_citation = getCrossrefCitation(re.sub("doi:", "", new_curie))
                                    new_citation = {"curie": new_curie, "relationship": new_rel, "type": new_type,
                                                    "comment": new_comment, "citation": formatted_citation, "source": new_source}

                                    citation_record["citations"].append(new_citation)
        else:
            print("[ERROR] No version information found for dataset: " + str(citation_record["id"]))
            logger.error("No version information found for dataset: " + str(citation_record["id"]))

        idx = idx + 1
    print('|', flush=True)

    print("[INFO] Add DataCite citations finished")
    logger.info("Add DataCite citations finished")

    with open('./datasets-cache.json', 'w') as outfile:
        json.dump(citation_data, outfile, sort_keys=True, indent=4)

print("[INFO] Add Crossref citations")
logger.info("Add Crossref citations")
#####################################################################
# Add Metadata (Crossref)
# https://api.crossref.org/swagger-ui/index.html#/Works/get_works__doi_

num_datasets = len(citation_data["datasets"])

run_service = config.get('env', "crossref")
if run_service == 'off':
    print("[INFO] Skipping Crossref citations")
    logger.info("Skipping Crossref citations")
else:

    # Get config parameters
    base_url = config.get('crossref', "base_url")
    url_email = config.get('crossref', "email")

    idx = 0
    while idx < num_datasets:
        citation_record = citation_data["datasets"][idx]
        print('D[' + str(idx) + ']', end='', flush=True)

        if "versions" in citation_record:
            dataset_versions = citation_record["versions"]

            for version_record in dataset_versions:
                print('V', end='', flush=True)
                if "doi" in version_record:

                    #####################################################################
                    # CODE HERE TO RETRIEVE AND MANIPULATE DATA
                    # Make sure to set update citations field via .append
                    # citation data: { "curie": ##DOI##, "relationship": ##DataCite Relationship##,
                    #                  "type": ##Originating Article, Protocol, etc##, "comment": ####}
                    # citation_record["citations"].append(new_citation)

                    dataset_doi = version_record["doi"]

                    crossref_url = base_url + "?mailto=" + url_email + "&rows=1000&relation-type=references&obj-id=" + dataset_doi

                    crossref_citations = getURL(crossref_url)
                    new_record = crossref_citations.json()

                    num_citations = 0
                    if "message" in new_record:
                        tmp_record = new_record["message"]
                        if "events" in tmp_record:
                            tmp_record = tmp_record["events"]

                            new_citations = tmp_record
                            num_citations = len(new_citations)
                    else:
                        print("[ERROR] Bad data from API")
                        logger.error("Bad data from API")

                    if num_citations > 0:
                        for citation in new_citations:
                            print('.', end='', flush=True)
                            if "subj_id" in citation:
                                new_curie = citation["subj_id"]
                                new_curie = re.sub("https://doi.org/","doi:",new_curie)

                                new_rel = "Cites"
                                new_type = "Work"
                                new_comment = ""
                                new_source = "Crossref"

                                # If new citation not in citations then add
                                citation_set = citation_record["citations"]

                                append = 1
                                for orig_citation in citation_set:
                                    if "curie" in orig_citation:
                                        if orig_citation["curie"] == new_curie:
                                            print('=', end='', flush=True)
                                            append = 0

                                if append == 1:
                                    # Create New Citation Record
                                    formatted_citation = getCrossrefCitation(re.sub("doi:", "", new_curie))
                                    new_citation = {"curie": new_curie, "relationship": new_rel, "type": new_type,
                                                    "comment": new_comment, "citation": formatted_citation, "source": new_source}

                                    citation_record["citations"].append(new_citation)
        else:
            print("[ERROR]: No version information found for dataset: " + str(citation_record["id"]))
            logger.error("[ERROR] No version information found for dataset: " + str(citation_record["id"]))

        idx = idx + 1
    print('|', flush=True)

    print("[INFO] Add Crossref citations finished")
    logger.info("Add Crossref citations finished")

    with open('./datasets-cache.json', 'w') as outfile:
        json.dump(citation_data, outfile, sort_keys=True, indent=4)

print("[INFO] Add K-Core citations")
logger.info("Add K-Core citations")
#####################################################################
# Add Metadata (K-Core)
# Citation Analysis Google Doc

num_datasets = len(citation_data["datasets"])

run_service = config.get('env', "kcore")
if run_service == 'off':
    print("[INFO] Skipping K-Core citations")
    logger.info("Skipping K-Core citations")
else:

    # Get config parameters
    csv_url = config.get('k-core', "csv_url")

    csv_citations = pd.DataFrame()

    csv_file = getURL(csv_url)
    open('./temp-citations.csv', 'wb').write(csv_file.content)

    csv_citations = pd.read_csv('./temp-citations.csv')

    print("[INFO] Number of rows in citation CSV: " + str(csv_citations.shape[0]) )
    logger.info("Number of rows in citation CSV: " + str(csv_citations.shape[0]))

    idx = 0
    while idx < num_datasets:
        citation_record = citation_data["datasets"][idx]
        print('D[' + str(idx) + ']', end='', flush=True)

        if "versions" in citation_record:
            dataset_versions = citation_record["versions"]
            for version_record in dataset_versions:
                print('V', end='', flush=True)
                if "doi" in version_record:

                    #####################################################################
                    # CODE HERE TO RETRIEVE AND MANIPULATE DATA
                    # Make sure to set update citations field via .append
                    # citation data: { "curie": ##DOI##, "relationship": ##DataCite Relationship##,
                    #                  "type": ##Originating Article, Protocol, etc##, "comment": ####}
                    # citation_record["citations"].append(new_citation)

                    dataset_doi = version_record["doi"]

                    csv_rows = csv_citations[csv_citations['Dataset_DOI'] == dataset_doi]

                    num_citations = csv_rows.shape[0]

                    # Add Primary Citations
                    if num_citations > 0:
                        for ind in csv_rows.index:

                            primary_doi = str(csv_rows['Primary_DOI'][ind])
                            primary_doi = primary_doi.strip()

                            new_curie = "doi:" + primary_doi

                            new_rel = "Describes"
                            new_type = "Originating Publication"
                            new_comment = ""
                            new_source = "K-Core"

                            # If new citation not in citations then add
                            citation_set = citation_record["citations"]

                            append = 1

                            if len(primary_doi) < 1 or primary_doi == "nan":
                                append = 0
                            else:
                                print('.', end='', flush=True)

                            for orig_citation in citation_set:
                                if "curie" in orig_citation:
                                    if orig_citation["curie"] == new_curie:
                                        print('=', end='', flush=True)
                                        append = 0

                            if append == 1:
                                # Create New Citation Record
                                formatted_citation = getCrossrefCitation(re.sub("doi:", "", new_curie))
                                new_citation = {"curie": new_curie, "relationship": new_rel, "type": new_type,
                                                "comment": new_comment, "citation": formatted_citation, "source": new_source}

                                citation_record["citations"].append(new_citation)

                    # Add Other Citations
                    if num_citations > 0:
                        for ind in csv_rows.index:

                            citation_doi = str(csv_rows['Citation_DOI'][ind])
                            citation_doi = citation_doi.strip()

                            new_curie = "doi:" + citation_doi

                            new_rel = "Cites"
                            new_type = "Work"
                            new_comment = ""
                            new_source = "K-Core"

                            # If new citation not in citations then add
                            citation_set = citation_record["citations"]

                            append = 1

                            if len(citation_doi) < 1 or citation_doi == "nan":
                                append = 0
                            else:
                                print('.', end='', flush=True)

                            for orig_citation in citation_set:
                                if "curie" in orig_citation:
                                    if orig_citation["curie"] == new_curie:
                                        print('=', end='', flush=True)
                                        append = 0

                            if append == 1:
                                # Create New Citation Record
                                formatted_citation = getCrossrefCitation(re.sub("doi:", "", new_curie))
                                new_citation = {"curie": new_curie, "relationship": new_rel, "type": new_type,
                                                "comment": new_comment, "citation": formatted_citation, "source": new_source}

                                citation_record["citations"].append(new_citation)
        else:
            print("[ERROR] No version information found for dataset: " + str(citation_record["id"]))
            logger.error("No version information found for dataset: " + str(citation_record["id"]))

        idx = idx + 1
    print('|', flush=True)

    print("[INFO] Add K-Core citations finished")
    logger.info("Add K-Core citations finished")

    with open('./datasets-cache.json', 'w') as outfile:
        json.dump(citation_data, outfile, sort_keys=True, indent=4)

#####################################################################
#####################################################################
print("[INFO] Finishing Up")
logger.info("Finishing Up")

with open('./dataset_data_citations.json', 'w') as outfile:
    json.dump(citation_data, outfile, indent=4)

print("[INFO] All citation processing has finished")
logger.info("All citation processing has finished")