import json
import os
import time
from datetime import datetime
from multiprocessing import Pool, cpu_count

output_json_path = "output.json" 

start = time.time()
with open(output_json_path, 'r') as file:
    phone_calls_dict = json.load(file)
end = time.time()

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

def process_area_code(args):
    area_code, ac_data, report_dir = args
    report = []

    for phone_number, call_data in sorted(ac_data.items()):
        sorted_timestamps = [datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S") for ts in call_data]

        for i in range(len(sorted_timestamps) - 1):
            timestamp_1 = sorted_timestamps[i]
            timestamp_2 = sorted_timestamps[i + 1]

            time_delta = timestamp_2 - timestamp_1
            sec_diff = time_delta.total_seconds()

            if 0 <= sec_diff < 600:
                time_str_1 = timestamp_1.strftime("%Y-%m-%d %H:%M:%S")  
                time_str_2 = timestamp_2.strftime("%H:%M:%S")
                minutes, seconds = divmod(int(sec_diff), 60)
                duration_str = f"{minutes:02}:{seconds:02}"
                line = f"{phone_number}: {time_str_1} -> {time_str_2} ({duration_str})"
                report.append(line)
    
    with open(os.path.join(report_dir, f"{area_code}.txt"), 'w') as file:
        if report:
            file.write('\n'.join(report)+'\n')

def export_redials_report(phone_calls_dict, report_dir):
    os.makedirs(report_dir, exist_ok=True)
    pool = Pool(processes=cpu_count())  # Create a pool of processes
    pool.map(process_area_code, [(area_code, ac_data, report_dir) for area_code, ac_data in phone_calls_dict.items()])
    pool.close()
    pool.join()


def main():
    #start_time = time.time()
    #data_dir = 'data' 
    #file = jload_phone_calls_dict(data_dir))
    time_start = time.time()
    print(f'Loading data from {output_json_path} took {end - start} seconds')
    phone_call_counts = generate_phone_call_counts(phone_calls_dict)
    most_frequent_list = most_frequently_called(phone_call_counts, 10)
    export_phone_call_counts(most_frequent_list, 'phone_call_counts.txt')
    export_redials_report(phone_calls_dict, 'redials_report')
    stop_time = time.time()
    print(f"Execution time: {stop_time - time_start} seconds")

if __name__ == '__main__':
    main()