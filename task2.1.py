import os
from datetime import datetime
import time
import random
from collections import defaultdict, Counter

def create_dev_set(full_data_dir, dev_data_dir, ratio=10):
    os.makedirs(dev_data_dir, exist_ok=True)
    for file_name in sorted(os.listdir(full_data_dir)):
        with open(f'{full_data_dir}/{file_name}') as file_full,\
                open(f'{dev_data_dir}/{file_name}', 'w') as file_dev:
            for line in file_full:
                rand_num = random.randint(0, 100)
                if rand_num < ratio:
                    file_dev.write(line)

def load_phone_calls_dict(data_dir):
    phone_calls_dict = defaultdict(lambda: defaultdict(list))
    
    for file_name in [f for f in os.listdir(data_dir) if f.startswith('phone_calls') and f.endswith('.txt')]:
        with open(os.path.join(data_dir, file_name), 'r') as file:
            for line in file:
                timestamp_str, phone_number = line.strip().split(': ')
                area_code = phone_number.split('(')[1][:3]
                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                if 0 <= timestamp.hour < 6:
                    phone_calls_dict[area_code][phone_number].append(timestamp)
    return phone_calls_dict

def generate_phone_call_counts(phone_calls_dict):
    phone_call_counts = Counter({phone_number: len(calls) for _, numbers in phone_calls_dict.items() for phone_number, calls in numbers.items()})
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
    phone_calls_dict = load_phone_calls_dict(data_dir)
    phone_call_counts = generate_phone_call_counts(phone_calls_dict)
    most_frequent_list = most_frequently_called(phone_call_counts, 10)
    export_phone_call_counts(most_frequent_list, 'phone_call_counts.txt')
    export_redials_report(phone_calls_dict, 'redials_report')
    stop_time = time.time()
    print(f"Execution time: {stop_time - start_time} seconds")

if __name__ == '__main__':
    main()
