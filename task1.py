from datetime import datetime

def filter_phone_calls(area_code, start_hour, end_hour, input_path, output_path):
    try:
        with open(input_path, 'r') as input_file, open(output_path, 'w') as output_file:
            for line in input_file:
                parts = line.strip().split(': ')
                print(parts)
                
                if len(parts) >= 2:
                    timestamp_str, phone_number = parts[0], parts[1]

                    # Extract the hour from the timestamp and parse it as an integer
                    try:
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                        call_hour = timestamp.hour

                        # Check if the area code and call hour match the criteria
                        if f'+1({area_code})' in phone_number and start_hour <= call_hour < end_hour:
                            # Write the line to the output file
                            output_file.write(line)
                    except ValueError:
                        print(f"Invalid timestamp format: {timestamp_str}")
    except FileNotFoundError:
        print(f"File not found: {input_path}")

if __name__ == '__main__':
    filter_phone_calls(
        area_code=412,
        start_hour=0,
        end_hour=6,
        input_path='data/phone_calls.txt', 
        output_path='data/phone_calls_filtered.txt'
    )