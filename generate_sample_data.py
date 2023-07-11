import csv
import random
import string
import uuid

# Generate a unique ID string
def generate_unique_id(rownum:int) -> str:
    return str(uuid.uuid3(uuid.NAMESPACE_DNS,"row"+str(rownum)))[:15]

# Generate the CSV file
def generate_csv_file(filename, num_columns, num_rows):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header row
        header = ['ID'] + [f'Column{i}' for i in range(2, num_columns + 1)]
        writer.writerow(header)
        
        # Write data rows
        for i in range(num_rows):
            if i%10000 == 0: print(f"{i} rows added")
            row = [generate_unique_id(i)]  # ID column
            
            for j in range(2, num_columns + 1):
                if j % 2 == 0:
                    # 50% chance of integer value
                    row.append(random.randint(1, 100))
                else:
                    # 50% chance of 1 or 2
                    row.append(random.choice([1, 2]))
                    
            writer.writerow(row)

# Generate the CSV file with 100 columns and 150,000 rows
filename = 'data.csv'
num_columns = 100
num_rows = 5

generate_csv_file(filename, num_columns, num_rows)
print(f"CSV file '{filename}' generated successfully.")

