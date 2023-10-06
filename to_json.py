import os
from datetime import datetime
from multiprocessing import Pool, cpu_count
from collections import defaultdict
import mmap
import time
import json

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def process_lines(lines):
    local_phone_calls_dict = {}

    for line in lines:
        timestamp_str, phone_number = line.strip().split(': ')
        area_code = phone_number.split('(')[1][:3]
        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')

        if 0 <= timestamp.hour < 6:
            if area_code not in local_phone_calls_dict:
                local_phone_calls_dict[area_code] = {}
            if phone_number not in local_phone_calls_dict[area_code]:
                local_phone_calls_dict[area_code][phone_number] = []
            local_phone_calls_dict[area_code][phone_number].append(timestamp)

    return local_phone_calls_dict

def read_file(file_name):
    with open(file_name, "r") as f:
        mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        lines = []
        line = mmapped_file.readline()  # Reading the first line
        while line:
            lines.append(line.decode('utf-8'))  # Decode bytes to string
            line = mmapped_file.readline()  # Read the next line
    return lines

def load_phone_calls_dict(data_dir):
    phone_calls_dict = defaultdict(lambda: defaultdict(list))

    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.startswith('phone_calls') and f.endswith('.txt')]
    
    num_processes = cpu_count()
    
    with Pool(num_processes) as pool:
        all_lines_lists = pool.map(read_file, files)
        
    all_lines = [line for sublist in all_lines_lists for line in sublist]
    
    chunk_size = len(all_lines) // num_processes
    chunks = [all_lines[i:i + chunk_size] for i in range(0, len(all_lines), chunk_size)]
    
    with Pool(num_processes) as pool:
        results = pool.map(process_lines, chunks)

    for local_dict in results:
        for area_code, numbers in local_dict.items():
            for phone_number, timestamps in numbers.items():
                phone_calls_dict[area_code][phone_number].extend(timestamps)

    with open('phone_calls_dict.json', 'w') as file:
        json.dump(phone_calls_dict, file, indent=2, cls=DateTimeEncoder)

    plain_dict = {k: dict(v) for k, v in phone_calls_dict.items()}
    return plain_dict
    #return phone_calls_dict

def generate_phone_call_counts(phone_calls_dict):
    phone_call_counts = {}
    
    for area_code, numbers in phone_calls_dict.items():
        for phone_number, calls in numbers.items():
            phone_call_counts[phone_number] = len(calls)
            
    return phone_call_counts


def most_frequently_called(phone_call_counts, top_n):
    items = list(phone_call_counts.items())
    sorted_items = sorted(items, key=lambda x: (-x[1], x[0]))
    return sorted_items[:top_n]


def export_phone_call_counts(most_frequent_list, out_file_path):
    with open(out_file_path, 'w') as output_file:
        for phone_number, count in most_frequent_list:
            output_file.write(f"{phone_number}: {count}\n")

def export_redials_report(phone_calls_dict, report_dir):
    os.makedirs(report_dir, exist_ok=True)

    for area_code, ac_data in phone_calls_dict.items():
        report = []  

        for phone_number, call_data in sorted(ac_data.items()):
            sorted_timestamps = sorted(call_data)

            for i in range(len(sorted_timestamps) - 1):
                timestamp_1 = sorted_timestamps[i]
                timestamp_2 = sorted_timestamps[i + 1]

                time_delta = timestamp_2 - timestamp_1
                sec_diff = time_delta.total_seconds()

                if sec_diff < 600:
                    time_str_1 = timestamp_1.strftime("%Y-%m-%d %H:%M:%S")  
                    time_str_2 = timestamp_2.strftime("%H:%M:%S")
                    minutes, seconds = divmod(int(sec_diff), 60)
                    duration_str = f"{minutes:02}:{seconds:02}"
                    line = f"{phone_number}: {time_str_1} -> {time_str_2} ({duration_str})"
                    report.append(line)
        
        with open(os.path.join(report_dir, f"{area_code}.txt"), 'w') as file:
            if report:
                file.write('\n'.join(report)+'\n')

def main():
    start_time = time.time()
    data_dir = 'data' 
    #file = jload_phone_calls_dict(data_dir)
    phone_calls_dict = load_phone_calls_dict(data_dir)
    phone_call_counts = generate_phone_call_counts(phone_calls_dict)
    most_frequent_list = most_frequently_called(phone_call_counts, 10)
    export_phone_call_counts(most_frequent_list, 'phone_call_counts.txt')
    export_redials_report(phone_calls_dict, 'redials_report')
    stop_time = time.time()
    print(f"Execution time: {stop_time - start_time} seconds")

if __name__ == '__main__':
    main()