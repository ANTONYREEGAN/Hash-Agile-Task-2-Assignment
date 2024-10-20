import chardet

# Replace 'employee_sample_data.csv' with your actual file path
file_path = 'employee.csv'

with open(file_path, 'rb') as f:
    result = chardet.detect(f.read())
    print(f"Detected encoding: {result['encoding']}")
