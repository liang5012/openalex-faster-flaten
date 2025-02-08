--authors

\copy openalex.authors (id, orcid, display_name, display_name_alternatives, works_count, cited_by_count, last_known_institution, works_api_url, updated_date) from program 'gunzip -c E:/openalex_csv/authors.csv.gz' csv header
\copy openalex.authors_ids (author_id, openalex, orcid, scopus, twitter, wikipedia, mag) from program 'gzip -d -c E:/openalex_csv/authors_ids.csv.gz' csv header
\copy openalex.authors_counts_by_year (author_id, year, works_count, cited_by_count, oa_works_count) from program 'gzip -d -c E:/openalex_csv/authors_counts_by_year.csv.gz' csv header

-- topics

\copy openalex.topics (id, display_name, subfield_id, subfield_display_name, field_id, field_display_name, domain_id, domain_display_name, description, keywords, works_api_url, wikipedia_id, works_count, cited_by_count, updated_date) from program 'gzip -d -c E:/openalex_csv/topics.csv.gz' csv header

--concepts

\copy openalex.concepts (id, wikidata, display_name, level, description, works_count, cited_by_count, image_url, image_thumbnail_url, works_api_url, updated_date) from program 'gzip -d -c E:/openalex_csv/concepts.csv.gz' csv header
\copy openalex.concepts_ancestors (concept_id, ancestor_id) from program 'gzip -d -c E:/openalex_csv/concepts_ancestors.csv.gz' csv header
\copy openalex.concepts_counts_by_year (concept_id, year, works_count, cited_by_count, oa_works_count) from program 'gzip -d -c E:/openalex_csv/concepts_counts_by_year.csv.gz' csv header
\copy openalex.concepts_ids (concept_id, openalex, wikidata, wikipedia, umls_aui, umls_cui, mag) from program 'gzip -d -c E:/openalex_csv/concepts_ids.csv.gz' csv header
\copy openalex.concepts_related_concepts (concept_id, related_concept_id, score) from program 'gzip -d -c E:/openalex_csv/concepts_related_concepts.csv.gz' csv header

--institutions

\copy openalex.institutions (id, ror, display_name, country_code, type, homepage_url, image_url, image_thumbnail_url, display_name_acronyms, display_name_alternatives, works_count, cited_by_count, works_api_url, updated_date) from program 'gzip -d -c E:/openalex_csv/institutions.csv.gz' csv header
\copy openalex.institutions_ids (institution_id, openalex, ror, grid, wikipedia, wikidata, mag) from program 'gzip -d -c E:/openalex_csv/institutions_ids.csv.gz' csv header
\copy openalex.institutions_geo (institution_id, city, geonames_city_id, region, country_code, country, latitude, longitude) from program 'gzip -d -c E:/openalex_csv/institutions_geo.csv.gz' csv header
\copy openalex.institutions_associated_institutions (institution_id, associated_institution_id, relationship) from program 'gzip -d -c E:/openalex_csv/institutions_associated_institutions.csv.gz' csv header
\copy openalex.institutions_counts_by_year (institution_id, year, works_count, cited_by_count, oa_works_count) from program 'gzip -d -c E:/openalex_csv/institutions_counts_by_year.csv.gz' csv header

--publishers

\copy openalex.publishers (id, display_name, alternate_titles, country_codes, hierarchy_level, parent_publisher, works_count, cited_by_count, sources_api_url, updated_date) from program 'gzip -d -c E:/openalex_csv/publishers.csv.gz' csv header
\copy openalex.publishers_ids (publisher_id, openalex, ror, wikidata) from program 'gzip -d -c E:/openalex_csv/publishers_ids.csv.gz' csv header
\copy openalex.publishers_counts_by_year (publisher_id, year, works_count, cited_by_count, oa_works_count) from program 'gzip -d -c E:/openalex_csv/publishers_counts_by_year.csv.gz' csv header

--sources

\copy openalex.sources (id, issn_l, issn, display_name, publisher, works_count, cited_by_count, is_oa, is_in_doaj, homepage_url, works_api_url, updated_date) from program 'gzip -d -c E:/openalex_csv/sources.csv.gz' csv header
\copy openalex.sources_ids (source_id, openalex, issn_l, issn, mag, wikidata, fatcat) from program 'gzip -d -c E:/openalex_csv/sources_ids.csv.gz' csv header
\copy openalex.sources_counts_by_year (source_id, year, works_count, cited_by_count, oa_works_count) from program 'gzip -d -c E:/openalex_csv/sources_counts_by_year.csv.gz' csv header

--works

\copy openalex.works (id, doi, title, display_name, publication_year, publication_date, type, cited_by_count, is_retracted, is_paratext, cited_by_api_url, abstract_inverted_index, language) from program 'gzip -d -c E:/openalex_csv/works.csv.gz' csv header
\copy openalex.works_primary_locations (work_id, source_id, landing_page_url, pdf_url, is_oa, version, license) from program 'gzip -d -c E:/openalex_csv/works_primary_locations.csv.gz' csv header
\copy openalex.works_locations (work_id, source_id, landing_page_url, pdf_url, is_oa, version, license) from program 'gzip -d -c E:/openalex_csv/works_locations.csv.gz' csv header
\copy openalex.works_best_oa_locations (work_id, source_id, landing_page_url, pdf_url, is_oa, version, license) from program 'gzip -d -c E:/openalex_csv/works_best_oa_locations.csv.gz' csv header
\copy openalex.works_authorships (work_id, author_position, author_id, institution_id, raw_affiliation_string) from program 'gzip -d -c E:/openalex_csv/works_authorships.csv.gz' csv header
\copy openalex.works_biblio (work_id, volume, issue, first_page, last_page) from program 'gzip -d -c E:/openalex_csv/works_biblio.csv.gz' csv header
\copy openalex.works_topics (work_id, topic_id, score) from program 'gzip -d -c E:/openalex_csv/works_topics.csv.gz' csv header
\copy openalex.works_concepts (work_id, concept_id, score) from program 'gzip -d -c E:/openalex_csv/works_concepts.csv.gz' csv header
\copy openalex.works_ids (work_id, openalex, doi, mag, pmid, pmcid) from program 'gzip -d -c E:/openalex_csv/works_ids.csv.gz' csv header
\copy openalex.works_mesh (work_id, descriptor_ui, descriptor_name, qualifier_ui, qualifier_name, is_major_topic) from program 'gzip -d -c E:/openalex_csv/works_mesh.csv.gz' csv header
\copy openalex.works_open_access (work_id, is_oa, oa_status, oa_url, any_repository_has_fulltext) from program 'gzip -d -c E:/openalex_csv/works_open_access.csv.gz' csv header
\copy openalex.works_referenced_works (work_id, referenced_work_id) from program 'gzip -d -c E:/openalex_csv/works_referenced_works.csv.gz' csv header
\copy openalex.works_related_works (work_id, related_work_id) from program 'gzip -d -c E:/openalex_csv/works_related_works.csv.gz' csv header

    -- works more inforamtion
\copy openalex.works_grants (work_id, funder, funder_display_name, award_id) from program 'gzip -d -c E:/openalex_csv/works_grants.csv.gz' csv header
\copy openalex.works_counts_by_year (work_id, year, cited_by_count) from program 'gzip -d -c E:/openalex_csv/works_counts_by_year.csv.gz' csv header
\copy openalex.works_more_info (work_id, institutions_distinct_count, countries_distinct_count, authors_count, fwci, citation_normalized_percentile, top1_percentile, top10_percentile) from program 'gzip -d -c E:/openalex_csv/works_more_info.csv.gz' csv header