import json
import requests
from requests.adapters import HTTPAdapter, Retry
import configparser
import re
from elasticsearch import Elasticsearch
from time import sleep
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
# Function to retrieve formatted citation from Crossref
def getCrossrefCitation(doi2format):
    base_url = config.get('crosscite', "base_url")
    email = config.get('crosscite', "email")

    crosscite_url = base_url + "/" + doi2format

    result = "CITATION[" + doi2format + "]"

    url_session = requests.Session()

    retries = Retry(total=10,
                    backoff_factor=1,
                    status_forcelist=[403, 413, 429, 500, 502, 503, 504])

    url_session.mount('http://', HTTPAdapter(max_retries=retries))
    url_session.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        crossref_citation = url_session.get(crosscite_url, headers={"Accept": "text/x-bibliography", 'style': 'ieee-transactions-on-medical-imaging', "mailto": email})
        crossref_citation.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print ("[ERROR] Retrieving Citation - HTTP Error:",errh)
    except requests.exceptions.ConnectionError as errc:
        print ("[ERROR] Retrieving Citation - Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        print ("[ERROR] Retrieving Citation - Timeout Error:",errt)
    except requests.exceptions.RequestException as err:
        print ("[ERROR] Retrieving Citation - Something Else",err)


    result = crossref_citation.text

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
base_url = config.get('pennsieve', "base_url")

pen_session = requests.Session()

retries = Retry(total=10,
                backoff_factor=1,
                status_forcelist=[403, 413, 429, 500, 502, 503, 504])

pen_session.mount('http://', HTTPAdapter(max_retries=retries))
pen_session.mount('https://', HTTPAdapter(max_retries=retries))

try:
    pennsieve = pen_session.get(base_url + '?orderBy=date&orderDirection=asc')
    pennsieve.raise_for_status()
except requests.exceptions.HTTPError as errh:
    print ("[ERROR] Retrieving Citation - HTTP Error:",errh)
except requests.exceptions.ConnectionError as errc:
    print ("[ERROR] Retrieving Citation - Error Connecting:",errc)
except requests.exceptions.Timeout as errt:
    print ("[ERROR] Retrieving Citation - Timeout Error:",errt)
except requests.exceptions.RequestException as err:
    print ("[ERROR] Retrieving Citation - Something Else",err)

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

    try:
        each_pennsieve = pen_session.get(pennsieve_url)
        each_pennsieve.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print("[ERROR] Retrieving Citation - HTTP Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("[ERROR] Retrieving Citation - Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("[ERROR] Retrieving Citation - Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("[ERROR] Retrieving Citation - Something Else", err)

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

                        try:
                            pennsieve_version = pen_session.get(pennsieve_url)
                            if pennsieve_version.status_code == 410:
                                print("[WARNING] Retrieving Dataset Version - Version Unpublished:" + pennsieve_url)
                            else:
                                pennsieve_version.raise_for_status()
                        except requests.exceptions.HTTPError as errh:
                            print("[ERROR] Retrieving Dataset Version - HTTP Error:", errh)
                        except requests.exceptions.ConnectionError as errc:
                            print("[ERROR] Retrieving Dataset Version - Error Connecting:", errc)
                        except requests.exceptions.Timeout as errt:
                            print("[ERROR] Retrieving Dataset Version - Timeout Error:", errt)
                        except requests.exceptions.RequestException as err:
                            print("[ERROR] Retrieving Dataset Version - Something Else", err)

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

with open('./debug-datasets.json', 'w') as outfile:
    json.dump(citation_data, outfile, sort_keys=True, indent=4)


print("[INFO] Add SPARC citations")
logger.info("Add SPARC citations")
