import csv
import glob
import gzip
import json
import os
import time

SNAPSHOT_DIR = 'D:/openalex_data'
CSV_DIR = 'D:/openalex-documentation-scripts-main/outputfile'

csv_files = {
    'funders': {
        'funders': {
            'name': os.path.join(CSV_DIR, 'funders.csv.gz'),
            'columns': ['id', 'display_name', 'country_code', 'country_id',
                        'description', 'grants_count', 'works_count','homepage_url',
                        ]
        },
        'funders_id': {
            'name': os.path.join(CSV_DIR, 'funders_id.csv.gz'),
            'columns': ['funder_id', 'crossref_id', 'doi', 'ror', 'wikidata']
        }
    }
}

def flatten_funders():
    file_spec = csv_files['funders']
    with gzip.open(file_spec['funders']['name'], 'wt',
                encoding='utf-8') as funders_csv, \
        gzip.open(file_spec['funders_id']['name'], 'wt',
                    encoding='utf-8') as funders_id_csv:
        
        funders_writer = csv.DictWriter(funders_csv, fieldnames=file_spec['funders']['columns'], lineterminator='\n')
        funders_writer.writeheader()

        funders_id_writer = csv.DictWriter(funders_id_csv, fieldnames=file_spec['funders_id']['columns'], lineterminator='\n')
        funders_id_writer.writeheader()


        for jsonl_file_name in glob.glob(os.path.join(SNAPSHOT_DIR, 'funders', '*', '*.gz')):
            print(jsonl_file_name)
            with gzip.open(jsonl_file_name, 'r') as funders_jsonl:
                for funders_json in funders_jsonl:
                    if not funders_json.strip():
                        continue
                    funder = json.loads(funders_json)


                    if not (funder_id := funder.get('id')):
                        continue

                    # funders
                    funders_writer.writerow({
                        'id': funder_id,
                        'display_name': funder.get('display_name'),
                        'country_code': funder.get('country_code'),
                        'country_id': funder.get('country_id'),
                        'description': funder.get('description'),
                        'grants_count': funder.get('grants_count'),
                        'works_count': funder.get('works_count'),
                        'homepage_url': funder.get('homepage_url')
                    })

                    # funders_id
                    if funder_ids := funder.get('ids'):
                        funders_id_writer.writerow({
                            'funder_id': funder_id,
                            'crossref_id': funder_ids.get('crossref'),
                            'doi': funder_ids.get('doi'),
                            'ror': funder_ids.get('ror'),
                            'wikidata': funder_ids.get('wikidata')
                        })

if __name__ == '__main__':
    flatten_funders()