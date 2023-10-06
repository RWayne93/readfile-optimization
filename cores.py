import os
from datetime import datetime
from multiprocessing import Pool, cpu_count
from collections import defaultdict
import mmap
import time

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
    """
    Reads the contents of a file using memory mapping.

    Parameters:
        file_name (str): The name of the file to read.

    Returns:
        list: A list of strings representing the lines in the file.

    Raises:
        FileNotFoundError: If the specified file does not exist.

    Technical Details:
        The `mmap` module is used to memory map the file for reading. 
        Memory mapping is a technique that allows a file to be mapped into 
        memory so that it can be accessed like an array. This can be more 
        efficient than reading the file using traditional I/O operations 
        because it avoids the overhead of copying data between the file and memory.
        The `mmap.mmap` function is used to memory map the file for reading. 
        The `fileno` method of the file object is used to get the file descriptor, 
        which is passed as the first argument to `mmap.mmap`. The second argument 
        is the length of the memory map, which is set to 0 to map the entire file. 
        the `access` argument is set to `mmap.ACCESS_READ` to indicate that the 
        file should be mapped for reading.The lines of the file are read using the 
        `readline` method of the memory-mapped file object. Each line is decoded from bytes 
        to string using the UTF-8 encoding, which is a widely used character encoding 
        that can represent any character in the Unicode standard. The decoded lines are appended 
        to a list, which is returned as the result of the function.The function raises a `FileNotFoundError` 
        exception if the specified file does not exist.
    """
    with open(file_name, "r") as f:
        # Memory map the file for reading
        mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        lines = []
        line = mmapped_file.readline()  
        while line:
            # Decode the line from bytes to string using UTF-8 encoding
            lines.append(line.decode('utf-8'))  
            line = mmapped_file.readline()  
    return lines

def load_phone_calls_dict(data_dir):
    """
    Multiprocessing is a Python module that allows you to run multiple 
    processes in parallel, which can be useful for tasks that 
    can be split into smaller, independent parts. In this function, 
    we use multiprocessing to read the phone call data files and process 
    the data in parallel across multiple CPU cores. This can significantly 
    reduce the processing time for large amounts of data, as each process 
    can work on a separate chunk of data simultaneously. While this is great
    it also introduces its own overhead by spawning its own python interpreter
    with its own memory space. This leads to a trade between memory usage
    for faster processing time.
    """
    phone_calls_dict = defaultdict(lambda: defaultdict(list))
    #phone_calls_dict = {}

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
    
    #plain_dict = {k: dict(v) for k, v in phone_calls_dict.items()}
    #return plain_dict
    return phone_calls_dict

def generate_phone_call_counts(phone_calls_dict):
    phone_call_counts = {}
    
    for _, numbers in phone_calls_dict.items():
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
