from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import os
import json
import glob
import csv
import gzip

SNAPSHOT_DIR = 'E:/openalex_data'
CSV_DIR = 'E:/openalex_csv'

csv_files = {
    'works': {
        'works': {
            'name': os.path.join(CSV_DIR, 'works.csv.gz'),
            'columns': [
                'id', 'doi', 'title', 'display_name', 'publication_year',
                'publication_date', 'type', 'cited_by_count',
                'is_retracted', 'is_paratext', 'cited_by_api_url',
                'abstract_inverted_index', 'language'
            ]
        },
        'primary_locations': {
            'name': os.path.join(CSV_DIR, 'works_primary_locations.csv.gz'),
            'columns': [
                'work_id', 'source_id', 'landing_page_url', 'pdf_url', 'is_oa',
                'version', 'license'
            ]
        },
        'locations': {
            'name': os.path.join(CSV_DIR, 'works_locations.csv.gz'),
            'columns': [
                'work_id', 'source_id', 'landing_page_url', 'pdf_url', 'is_oa',
                'version', 'license'
            ]
        },
        'best_oa_locations': {
            'name': os.path.join(CSV_DIR, 'works_best_oa_locations.csv.gz'),
            'columns': [
                'work_id', 'source_id', 'landing_page_url', 'pdf_url', 'is_oa',
                'version', 'license'
            ]
        },
        'authorships': {
            'name': os.path.join(CSV_DIR, 'works_authorships.csv.gz'),
            'columns': [
                'work_id', 'author_position', 'author_id', 'institution_id',
                'raw_affiliation_string'
            ]
        },
        'biblio': {
            'name': os.path.join(CSV_DIR, 'works_biblio.csv.gz'),
            'columns': [
                'work_id', 'volume', 'issue', 'first_page', 'last_page'
            ]
        },
        'topics': {
            'name': os.path.join(CSV_DIR, 'works_topics.csv.gz'),
            'columns': [
                'work_id', 'topic_id', 'score'
            ]
        },
        'concepts': {
            'name': os.path.join(CSV_DIR, 'works_concepts.csv.gz'),
            'columns': [
                'work_id', 'concept_id', 'score'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'works_ids.csv.gz'),
            'columns': [
                'work_id', 'openalex', 'doi', 'mag', 'pmid', 'pmcid'
            ]
        },
        'mesh': {
            'name': os.path.join(CSV_DIR, 'works_mesh.csv.gz'),
            'columns': [
                'work_id', 'descriptor_ui', 'descriptor_name', 'qualifier_ui',
                'qualifier_name', 'is_major_topic'
            ]
        },
        'open_access': {
            'name': os.path.join(CSV_DIR, 'works_open_access.csv.gz'),
            'columns': [
                'work_id', 'is_oa', 'oa_status', 'oa_url',
                'any_repository_has_fulltext'
            ]
        },
        'referenced_works': {
            'name': os.path.join(CSV_DIR, 'works_referenced_works.csv.gz'),
            'columns': [
                'work_id', 'referenced_work_id'
            ]
        },
        'related_works': {
            'name': os.path.join(CSV_DIR, 'works_related_works.csv.gz'),
            'columns': [
                'work_id', 'related_work_id'
            ]
        },
    },
}

def flatten_works():
    file_spec = csv_files['works']

    with gzip.open(file_spec['works']['name'], 'wt',
                   encoding='utf-8') as works_csv, \
            gzip.open(file_spec['primary_locations']['name'], 'wt',
                      encoding='utf-8') as primary_locations_csv, \
            gzip.open(file_spec['locations']['name'], 'wt',
                      encoding='utf-8') as locations, \
            gzip.open(file_spec['best_oa_locations']['name'], 'wt',
                      encoding='utf-8') as best_oa_locations, \
            gzip.open(file_spec['authorships']['name'], 'wt',
                      encoding='utf-8') as authorships_csv, \
            gzip.open(file_spec['biblio']['name'], 'wt',
                      encoding='utf-8') as biblio_csv, \
            gzip.open(file_spec['topics']['name'], 'wt',
                      encoding='utf-8') as topics_csv, \
            gzip.open(file_spec['concepts']['name'], 'wt',
                      encoding='utf-8') as concepts_csv, \
            gzip.open(file_spec['ids']['name'], 'wt',
                      encoding='utf-8') as ids_csv, \
            gzip.open(file_spec['mesh']['name'], 'wt',
                      encoding='utf-8') as mesh_csv, \
            gzip.open(file_spec['open_access']['name'], 'wt',
                      encoding='utf-8') as open_access_csv, \
            gzip.open(file_spec['referenced_works']['name'], 'wt',
                      encoding='utf-8') as referenced_works_csv, \
            gzip.open(file_spec['related_works']['name'], 'wt',
                      encoding='utf-8') as related_works_csv:

        works_writer = init_dict_writer(works_csv, file_spec['works'],
                                        extrasaction='ignore')
        primary_locations_writer = init_dict_writer(primary_locations_csv,
                                                    file_spec[
                                                        'primary_locations'])
        locations_writer = init_dict_writer(locations, file_spec['locations'])
        best_oa_locations_writer = init_dict_writer(best_oa_locations,
                                                    file_spec[
                                                        'best_oa_locations'])
        authorships_writer = init_dict_writer(authorships_csv,
                                              file_spec['authorships'])
        biblio_writer = init_dict_writer(biblio_csv, file_spec['biblio'])
        topics_writer = init_dict_writer(topics_csv, file_spec['topics'])
        concepts_writer = init_dict_writer(concepts_csv, file_spec['concepts'])
        ids_writer = init_dict_writer(ids_csv, file_spec['ids'],
                                      extrasaction='ignore')
        mesh_writer = init_dict_writer(mesh_csv, file_spec['mesh'])
        open_access_writer = init_dict_writer(open_access_csv,
                                              file_spec['open_access'])
        referenced_works_writer = init_dict_writer(referenced_works_csv,
                                                   file_spec[
                                                       'referenced_works'])
        related_works_writer = init_dict_writer(related_works_csv,
                                                file_spec['related_works'])

        for jsonl_file_name in glob.glob(
                os.path.join(SNAPSHOT_DIR, 'data', 'works', '*', '*.gz')):
            print(jsonl_file_name)
            with gzip.open(jsonl_file_name, 'r') as works_jsonl:
                for work_json in works_jsonl:
                    if not work_json.strip():
                        continue

                    work = json.loads(work_json)

                    if not (work_id := work.get('id')):
                        continue

                    # works
                    if (abstract := work.get(
                            'abstract_inverted_index')) is not None:
                        work['abstract_inverted_index'] = json.dumps(abstract,
                                                                     ensure_ascii=False)

                    works_writer.writerow(work)

                    # primary_locations
                    if primary_location := (work.get('primary_location') or {}):
                        if primary_location.get(
                                'source') and primary_location.get(
                                'source').get('id'):
                            primary_locations_writer.writerow({
                                'work_id': work_id,
                                'source_id': primary_location['source']['id'],
                                'landing_page_url': primary_location.get(
                                    'landing_page_url'),
                                'pdf_url': primary_location.get('pdf_url'),
                                'is_oa': primary_location.get('is_oa'),
                                'version': primary_location.get('version'),
                                'license': primary_location.get('license'),
                            })

                    # locations
                    if locations := work.get('locations'):
                        for location in locations:
                            if location.get('source') and location.get(
                                    'source').get('id'):
                                locations_writer.writerow({
                                    'work_id': work_id,
                                    'source_id': location['source']['id'],
                                    'landing_page_url': location.get(
                                        'landing_page_url'),
                                    'pdf_url': location.get('pdf_url'),
                                    'is_oa': location.get('is_oa'),
                                    'version': location.get('version'),
                                    'license': location.get('license'),
                                })

                    # best_oa_locations
                    if best_oa_location := (work.get('best_oa_location') or {}):
                        if best_oa_location.get(
                                'source') and best_oa_location.get(
                                'source').get('id'):
                            best_oa_locations_writer.writerow({
                                'work_id': work_id,
                                'source_id': best_oa_location['source']['id'],
                                'landing_page_url': best_oa_location.get(
                                    'landing_page_url'),
                                'pdf_url': best_oa_location.get('pdf_url'),
                                'is_oa': best_oa_location.get('is_oa'),
                                'version': best_oa_location.get('version'),
                                'license': best_oa_location.get('license'),
                            })

                    # authorships
                    if authorships := work.get('authorships'):
                        for authorship in authorships:
                            if author_id := authorship.get('author', {}).get(
                                    'id'):
                                institutions = authorship.get('institutions')
                                institution_ids = [i.get('id') for i in
                                                   institutions]
                                institution_ids = [i for i in institution_ids if
                                                   i]
                                institution_ids = institution_ids or [None]

                                for institution_id in institution_ids:
                                    authorships_writer.writerow({
                                        'work_id': work_id,
                                        'author_position': authorship.get(
                                            'author_position'),
                                        'author_id': author_id,
                                        'institution_id': institution_id,
                                        'raw_affiliation_string': authorship.get(
                                            'raw_affiliation_string'),
                                    })

                    # biblio
                    if biblio := work.get('biblio'):
                        biblio['work_id'] = work_id
                        biblio_writer.writerow(biblio)

                    # topics
                    for topic in work.get('topics', []):
                        if topic_id := topic.get('id'):
                            topics_writer.writerow({
                                'work_id': work_id,
                                'topic_id': topic_id,
                                'score': topic.get('score')
                            })

                    # concepts
                    for concept in work.get('concepts'):
                        if concept_id := concept.get('id'):
                            concepts_writer.writerow({
                                'work_id': work_id,
                                'concept_id': concept_id,
                                'score': concept.get('score'),
                            })

                    # ids
                    if ids := work.get('ids'):
                        ids['work_id'] = work_id
                        ids_writer.writerow(ids)

                    # mesh
                    for mesh in work.get('mesh'):
                        mesh['work_id'] = work_id
                        mesh_writer.writerow(mesh)

                    # open_access
                    if open_access := work.get('open_access'):
                        open_access['work_id'] = work_id
                        open_access_writer.writerow(open_access)

                    # referenced_works
                    for referenced_work in work.get('referenced_works'):
                        if referenced_work:
                            referenced_works_writer.writerow({
                                'work_id': work_id,
                                'referenced_work_id': referenced_work
                            })

                    # related_works
                    for related_work in work.get('related_works'):
                        if related_work:
                            related_works_writer.writerow({
                                'work_id': work_id,
                                'related_work_id': related_work
                            })


def init_dict_writer(csv_file, file_spec, **kwargs):
    writer = csv.DictWriter(
        csv_file, fieldnames=file_spec['columns'], **kwargs
    )
    writer.writeheader()
    return writer

def process_file(jsonl_file_name):
    file_spec = csv_files['works']
    results = []

    with gzip.open(jsonl_file_name, 'r') as works_jsonl:
        for work_json in works_jsonl:
            if not work_json.strip():
                continue

            work = json.loads(work_json)

            if not (work_id := work.get('id')):
                continue

            # works
            if (abstract := work.get('abstract_inverted_index')) is not None:
                work['abstract_inverted_index'] = json.dumps(abstract, ensure_ascii=False)

            results.append(('works', work))

            # primary_locations
            if primary_location := (work.get('primary_location') or {}):
                if primary_location.get('source') and primary_location.get('source').get('id'):
                    results.append(('primary_locations', {
                        'work_id': work_id,
                        'source_id': primary_location['source']['id'],
                        'landing_page_url': primary_location.get('landing_page_url'),
                        'pdf_url': primary_location.get('pdf_url'),
                        'is_oa': primary_location.get('is_oa'),
                        'version': primary_location.get('version'),
                        'license': primary_location.get('license'),
                    }))

            # locations
            if locations := work.get('locations'):
                for location in locations:
                    if location.get('source') and location.get('source').get('id'):
                        results.append(('locations', {
                            'work_id': work_id,
                            'source_id': location['source']['id'],
                            'landing_page_url': location.get('landing_page_url'),
                            'pdf_url': location.get('pdf_url'),
                            'is_oa': location.get('is_oa'),
                            'version': location.get('version'),
                            'license': location.get('license'),
                        }))

            # best_oa_locations
            if best_oa_location := (work.get('best_oa_location') or {}):
                if best_oa_location.get('source') and best_oa_location.get('source').get('id'):
                    results.append(('best_oa_locations', {
                        'work_id': work_id,
                        'source_id': best_oa_location['source']['id'],
                        'landing_page_url': best_oa_location.get('landing_page_url'),
                        'pdf_url': best_oa_location.get('pdf_url'),
                        'is_oa': best_oa_location.get('is_oa'),
                        'version': best_oa_location.get('version'),
                        'license': best_oa_location.get('license'),
                    }))

            # authorships
            if authorships := work.get('authorships'):
                for authorship in authorships:
                    if author_id := authorship.get('author', {}).get('id'):
                        institutions = authorship.get('institutions')
                        institution_ids = [i.get('id') for i in institutions]
                        institution_ids = [i for i in institution_ids if i]
                        institution_ids = institution_ids or [None]

                        for institution_id in institution_ids:
                            results.append(('authorships', {
                                'work_id': work_id,
                                'author_position': authorship.get('author_position'),
                                'author_id': author_id,
                                'institution_id': institution_id,
                                'raw_affiliation_string': authorship.get('raw_affiliation_string'),
                            }))

            # biblio
            if biblio := work.get('biblio'):
                biblio['work_id'] = work_id
                results.append(('biblio', biblio))

            # topics
            for topic in work.get('topics', []):
                if topic_id := topic.get('id'):
                    results.append(('topics', {
                        'work_id': work_id,
                        'topic_id': topic_id,
                        'score': topic.get('score')
                    }))

            # concepts
            for concept in work.get('concepts'):
                if concept_id := concept.get('id'):
                    results.append(('concepts', {
                        'work_id': work_id,
                        'concept_id': concept_id,
                        'score': concept.get('score'),
                    }))

            # ids
            if ids := work.get('ids'):
                ids['work_id'] = work_id
                results.append(('ids', ids))

            # mesh
            for mesh in work.get('mesh'):
                mesh['work_id'] = work_id
                results.append(('mesh', mesh))

            # open_access
            if open_access := work.get('open_access'):
                open_access['work_id'] = work_id
                results.append(('open_access', open_access))

            # referenced_works
            for referenced_work in work.get('referenced_works'):
                if referenced_work:
                    results.append(('referenced_works', {
                        'work_id': work_id,
                        'referenced_work_id': referenced_work
                    }))

            # related_works
            for related_work in work.get('related_works'):
                if related_work:
                    results.append(('related_works', {
                        'work_id': work_id,
                        'related_work_id': related_work
                    }))

    return jsonl_file_name, results

def custom_callback(future):
    jsonl_file_name, results = future.result()
    base_name = os.path.basename(jsonl_file_name).replace('.gz', '')
    output_dir = os.path.join(CSV_DIR, base_name)
    os.makedirs(output_dir, exist_ok=True)

    writers = {}
    for key, file_spec in csv_files['works'].items():
        file_path = os.path.join(output_dir, f"{key}.csv.gz")
        writers[key] = init_dict_writer(gzip.open(file_path, 'wt', encoding='utf-8'), file_spec)

    for result_type, result in results:
        writers[result_type].writerow(result)

    for writer in writers.values():
        writer.writer.writer.close()


def flatten_works():
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_file, jsonl_file_name) for jsonl_file_name in glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'works', '*', '*.gz'))]
        for future in as_completed(futures):
            future.add_done_callback(custom_callback)


if __name__ == '__main__':
    start = time.time()
    flatten_works()
    print(f"Time taken: {time.time() - start:.2f} seconds.")