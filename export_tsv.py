import json
import pandas as pd

f = open('./dataset_data_citations.json')
data = json.load(f)
f.close()

citation_df = pd.DataFrame()

for dataset in data['datasets']:
    
    id = dataset['id']
    doi = dataset['doi']
    type = dataset['type']
    name = dataset['name']
    dataset_citation = dataset['citation']
    
    for citation in dataset['citations']:
        citation_new = pd.DataFrame.from_dict([{
                        'dataset_id': id,
                        'dataset_doi': doi,
                        'dataset_type': type,
                        'dataset_name': name,
                        
                        'citation_curie': citation['curie'],
                        'citation_relationship': citation['relationship'],
                        'citation_type': citation['type'],
                        'citation_comment': citation['comment'],
                        'citation_citation': citation['citation'],
                        'citation_source': citation['source']
                        }])
        
        citation_df = pd.concat([citation_df, citation_new], axis = 0)
        
citation_df.to_csv('dataset_data_citations.tsv', sep='\t', index=False, header=True)