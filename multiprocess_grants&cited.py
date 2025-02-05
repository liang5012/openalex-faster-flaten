import csv
import gzip
import json
import os
import time
from multiprocessing import Process, Queue
from itertools import islice
import glob

# 全局路径配置
SNAPSHOT_DIR = 'D:/postgreSQL_project/test_prj/inputfile'
CSV_DIR = 'D:/postgreSQL_project/test_prj/output'

# CSV 文件配置
csv_files = {
    'works': {
        'grants': {
            'name': os.path.join(CSV_DIR, 'works_grants.csv.gz'),
            'columns': [
                'work_id', 'funder_id', 'display_name', 'award_id',
            ]
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'works_counts_by_year.csv.gz'),
            'columns': [
                'author_id', 'year', 'cited_by_count'
                ]
        }
    }
}

file_spec = csv_files['authors']

