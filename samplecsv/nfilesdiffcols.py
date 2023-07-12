import csv
import random
import string
import uuid

# Generate a unique ID string
def generate_unique_id(rownum:int) -> str:
    return str(uuid.uuid3(uuid.NAMESPACE_DNS,rownum))[:15]

# Generate the CSV file with specified column range
def generate_csv_file(filename, start_column, end_column, num_rows):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header row
        header = ['ID'] + [f'Column{i}' for i in range(start_column, end_column + 1)]
        writer.writerow(header)
        
        # Write data rows
        for i in range(num_rows):
            row = [generate_unique_id(i)]  # ID column
            
            for j in range(start_column, end_column + 1):
                if j % 2 == 0:
                    # 50% chance of integer value
                    row.append(random.randint(1, 100))
                else:
                    # 50% chance of 1 or 2
                    row.append(random.choice([1, 2]))
                    
            writer.writerow(row)

# Generate the CSV files with different column ranges
num_files = 3
num_columns_per_file = 100
num_rows_per_file = 150000

for file_index in range(num_files):
    start_column = file_index * num_columns_per_file + 2
    end_column = (file_index + 1) * num_columns_per_file + 1
    filename = f'data_{file_index+1}.csv'
    
    generate_csv_file(filename, start_column, end_column, num_rows_per_file)
    print(f"CSV file '{filename}' generated successfully.")
