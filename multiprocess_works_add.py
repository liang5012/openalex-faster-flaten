import csv
import gzip
import json
import os
import time
from multiprocessing import Process, Queue
from itertools import islice
import glob

# 全局路径配置
SNAPSHOT_DIR = 'D:/openalex_data'
CSV_DIR = 'D:/openalex-documentation-scripts-main/outputfile'


# works中可以考虑增加的信息:
# citation_normalized_percentile counts_by_year fwci grants institutions_distinct_count
# countries_distinct_count authors_count

# CSV 文件配置
csv_files = {
    'works': {
        'grants': {
            'name': os.path.join(CSV_DIR, 'works_grants.csv.gz'),
            'columns': [
                'work_id', 'funder', 'funder_display_name', 'award_id',
            ]
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'works_counts_by_year.csv.gz'),
            'columns': [
                'work_id', 'year', 'cited_by_count'
                ]
        },
        'more_info': {
            'name': os.path.join(CSV_DIR, 'works_more_info.csv.gz'),
            'columns': [
                'work_id', 'institutions_distinct_count',
                'countries_distinct_count', 'authors_count','fwci', 'citation_normalized_percentile',
                'top1_percentile', 'top10_percentile'
            ]
        }
    }
}

file_spec = csv_files['works']

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


def filter(data_queue, grants_queue, counts_queue, add_queue, coordinator_queue):
    print("filter Started.")
    while True:
        data_chunk = data_queue.get()
        if data_chunk == 'DONE':
            coordinator_queue.put('FILTER_DONE')
            break
        # 处理数据
        grants_list = []
        counts_list = []
        add_list = []
        for line in data_chunk:
            if not line.strip():
                continue
            work = json.loads(line)
            if not (work_id := work.get('id')):
                continue
            info_add = {}
            
            # grants
            if grants := work.get('grants'):  # 如果 grants 为空列表,则不执行
                for grant in grants:
                    grant['work_id'] = work_id
                    grants_list.append(grant)

            # counts_by_year
            if counts := work.get('counts_by_year'):
                for info in counts:
                    info['work_id'] = work_id
                    counts_list.append(info)
            # more_info
            info_add['work_id'] = work_id
            info_add['institutions_distinct_count'] = work.get('institutions_distinct_count')
            info_add['countries_distinct_count'] = work.get('countries_distinct_count')
            info_add['authors_count'] = work.get('authors_count')
            info_add['fwci'] = work.get('fwci')

            # 假设 info_add 是你用来存储提取数据的字典
            # 假设 info_add 是你用来存储提取数据的字典
            citation_info = work.get('citation_normalized_percentile')  # 获取 citation_normalized_percentile

            # 如果 citation_normalized_percentile 是 None，则将其视为空字典
            if citation_info is None:
                citation_info = {}

            # 提取字段，如果字段不存在则使用默认值 None
            info_add['citation_normalized_percentile'] = citation_info.get('value')
            info_add['top1_percentile'] = citation_info.get('is_in_top_1_percent')
            info_add['top10_percentile'] = citation_info.get('is_in_top_10_percent')

            add_list.append(info_add)

        grants_queue.put(grants_list)
        counts_queue.put(counts_list)
        add_queue.put(add_list)


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


def coordinator(coordinator_queue, grants_queue, counts_queue, add_queue):
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
                while not grants_queue.qsize() == 0:
                    continue
                grants_queue.put('DONE')
                counts_queue.put('DONE')
                add_queue.put('DONE')
                print("All filters done.")
        elif queue_message == 'WRITE_DONE':
            active_writers -= 1
            print(f"Writer done. Active writers: {active_writers}")
            if active_writers == 0:
                print("All writers done.")
                break

    with open('D:/openalex-documentation-scripts-main/outputfile/log.json', 'w', encoding="utf-8") as f:
        for queue in [grants_queue, counts_queue, add_queue]:
            while not queue.empty():
                content = queue.get()
                for i in content:
                    f.write(json.dumps(i, ensure_ascii=False) + '\n')
                    # print(i)


if __name__ == '__main__':
    # 创建多个 data_queue
    data_queue_1 = Queue(500000)
    data_queue_2 = Queue(500000)
    data_queues = [data_queue_1, data_queue_2]  # 将 data_queue 放入列表中

    # 创建其他队列
    grants_queue = Queue(500000)
    counts_queue = Queue(500000)
    add_queue = Queue(500000)

    # 创建 coordinator_queue
    coordinator_queue = Queue()
    # input_files = glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'authors', '*', '*.gz'))
    input_files = glob.glob(os.path.join(SNAPSHOT_DIR, '*.gz'))
    # 创建 reader 进程
    readers = Process(target=reader, args=(input_files, data_queues, coordinator_queue, 50))

    # 创建 filter 进程
    filters = [
        Process(target=filter, args=(data_queue_1, grants_queue, counts_queue, add_queue, coordinator_queue)),
        Process(target=filter, args=(data_queue_2, grants_queue, counts_queue, add_queue, coordinator_queue))
    ]

    # 创建 writer 进程
    writers = [
        Process(target=write_to_gz, args=(grants_queue, file_spec['grants']['name'], file_spec['grants']['columns'], coordinator_queue)),
        Process(target=write_to_gz, args=(counts_queue, file_spec['counts_by_year']['name'], file_spec['counts_by_year']['columns'], coordinator_queue)),
        Process(target=write_to_gz, args=(add_queue, file_spec['more_info']['name'], file_spec['more_info']['columns'], coordinator_queue))
    ]

    # 创建 coordinator 进程
    coordinator_p = Process(target=coordinator, args=(coordinator_queue,grants_queue, counts_queue, add_queue))
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
    print("All processes done, it took", (end - start)/60, "minutes.")