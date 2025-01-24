import csv
import gzip
import json
import os
import time
from multiprocessing import Process, Queue
from itertools import islice
import glob

# 全局路径配置
SNAPSHOT_DIR = 'path/to/snapshot'
CSV_DIR = 'path/to/csv'

# CSV 文件配置
csv_files = {
    'authors': {
        'authors': {
            'name': os.path.join(CSV_DIR, 'authors.csv.gz'),
            'columns': [
                'id', 'orcid', 'display_name', 'display_name_alternatives',
                'works_count', 'cited_by_count',
                'last_known_institution', 'works_api_url', 'updated_date',
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'authors_ids.csv.gz'),
            'columns': [
                'author_id', 'openalex', 'orcid', 'scopus', 'twitter',
                'wikipedia', 'mag'
            ]
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'authors_counts_by_year.csv.gz'),
            'columns': [
                'author_id', 'year', 'works_count', 'cited_by_count',
                'oa_works_count'
            ]
        }
    }
}

file_spec = csv_files['authors']

def reader(file_paths, data_queues, coordinator_queue, chunk_size):
    print("Reader Started.")
    queue_index = 0  # 用于轮询分发数据

    for file_path in file_paths:
        print(f"Reading file: {file_path}")
        with gzip.open(file_path, "rt", encoding='utf-8', newline='') as infile:
            while True:
                data_chunk = list(islice(infile, chunk_size))
                if not data_chunk:
                    break  # 当前文件读取完成，继续读取下一个文件

                # 将数据块分发到对应的 data_queue
                data_queues[queue_index].put(data_chunk)

                # 更新 queue_index，轮询分发数据
                queue_index = (queue_index + 1) % len(data_queues)

    # 所有文件读取完成，向所有 data_queue 发送 DONE 信号
    for data_queue in data_queues:
        data_queue.put('DONE')
    coordinator_queue.put('READ_DONE')


def filter(data_queue, authors_queue, authors_ids_queue, counts_queue, coordinator_queue):
    print("filter Started.")
    while True:
        data_chunk = data_queue.get()
        if data_chunk == 'DONE':
            coordinator_queue.put('FILTER_DONE')
            break
        # 处理数据
        authors = []
        authors_ids = []
        counts = []
        for line in data_chunk:
            if not line.strip():
                continue
            author = json.loads(line)
            if not (author_id := author.get('id')):
                continue
            author['display_name_alternatives'] = json.dumps(author.get('display_name_alternatives'), ensure_ascii=False)
            author['last_known_institution'] = (author.get('last_known_institution') or {}).get('id')
            authors.append(author)
            if author_ids := author.get('ids'):
                author_ids['author_id'] = author_id
                authors_ids.append(author_ids)
            if counts_by_year := author.get('counts_by_year'):
                for count_by_year in counts_by_year:
                    count_by_year['author_id'] = author_id
                    counts.append(count_by_year)
        authors_queue.put(authors)
        authors_ids_queue.put(authors_ids)
        counts_queue.put(counts)


def write_to_gz(queue, file_path, columns, coordinator_queue):
    print(f"Writer Started for {file_path}.")
    with gzip.open(file_path, 'wt', encoding='utf-8', newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=columns, extrasaction='ignore', lineterminator='\n')
        writer.writeheader()  # 写入表头

        while True:
            data = queue.get()
            if data == 'DONE':
                coordinator_queue.put('WRITE_DONE')
                break
            writer.writerows(data)  # 写入数据

def coordinator(coordinator_queue, authors_queue, authors_ids_queue, counts_queue):
    print("Coordinator Started.")
    active_readers = 1  # 只有一个 reader 进程
    active_filters = 2  # 有两个 filter 进程
    active_writers = 3  # 有三个 writer 进程

    while True:
        queue_message = coordinator_queue.get()
        if queue_message == 'READ_DONE':
            active_readers -= 1
            if active_readers == 0:
                print("reader done")
        elif queue_message == 'FILTER_DONE':
            active_filters -= 1
            print(f"Filter done. Active filters: {active_filters}")
            if active_filters == 0:
                while not authors_queue.qsize() == 0:
                    continue
                authors_queue.put('DONE')
                authors_ids_queue.put('DONE')
                counts_queue.put('DONE')
                print("All filters done.")
        elif queue_message == 'WRITE_DONE':
            active_writers -= 1
            print(f"Writer done. Active writers: {active_writers}")
            if active_writers == 0:
                print("All writers done.")
                break

    with open('d:/openalex-documentation-scripts-main/outputfile/log.json', 'w', encoding="utf-8") as f:
        for queue in [authors_queue, authors_ids_queue, counts_queue]:
            while not queue.empty():
                content = queue.get()
                for i in content:
                    f.write(json.dumps(i, ensure_ascii=False) + '\n')
                    # print(i)
            

if __name__ == '__main__':
    # 创建多个 data_queue
    data_queue_1 = Queue()
    data_queue_2 = Queue()
    data_queues = [data_queue_1, data_queue_2]  # 将 data_queue 放入列表中

    # 创建其他队列
    authors_queue = Queue()
    authors_ids_queue = Queue()
    counts_queue = Queue()

    # 创建 coordinator_queue
    coordinator_queue = Queue()
    input_files = glob.glob(os.path.join(SNAPSHOT_DIR, '*.gz'))
    # 创建 reader 进程
    readers = Process(target=reader, args=(input_files, data_queues, coordinator_queue, 400))

    # 创建 filter 进程
    filters = [
        Process(target=filter, args=(data_queue_1, authors_queue, authors_ids_queue, counts_queue, coordinator_queue)),
        Process(target=filter, args=(data_queue_2, authors_queue, authors_ids_queue, counts_queue, coordinator_queue))
    ]

    # 创建 writer 进程
    writers = [
        Process(target=write_to_gz, args=(authors_queue, file_spec['authors']['name'], file_spec['authors']['columns'], coordinator_queue)),
        Process(target=write_to_gz, args=(authors_ids_queue, file_spec['ids']['name'], file_spec['ids']['columns'], coordinator_queue)),
        Process(target=write_to_gz, args=(counts_queue, file_spec['counts_by_year']['name'], file_spec['counts_by_year']['columns'], coordinator_queue))
    ]

    # 创建 coordinator 进程
    coordinator_p = Process(target=coordinator, args=(coordinator_queue,authors_queue, authors_ids_queue, counts_queue))
    start = time.time()
    # 启动 reader、filter 和 writer 进程
    
    for proc in filters:
        proc.start()
    for proc in writers:
        proc.start()
    coordinator_p.start()
    readers.start()


    # 等待 reader、filter 和 writer 进程结束
    readers.join()
    for proc in filters:
        proc.join()
    for proc in writers:
        proc.join()
    coordinator_p.join()

    end = time.time()
    print("All processes done, it took", end - start, "seconds.")
