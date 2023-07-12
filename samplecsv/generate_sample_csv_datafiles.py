import csv
import random
import string
import uuid
import argparse
import os

# Generate a unique ID string
def generate_unique_id(rownum:int) -> str:
    return '"'+str(uuid.uuid3(uuid.NAMESPACE_DNS,"row"+str(rownum)))[:15]+'"'

# Generate the CSV file
def generate_csv_file(filename:str, num_columns:int, num_rows:int):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
        
        # Write header row
        header = ['PROJECT_PSEUDO_ID'] + [f'Column{i}' for i in range(2, num_columns + 1)]
        writer.writerow(header)
        
        # Write data rows
        for i in range(num_rows):
            if i%10000 == 0: print(f"{i} rows added")
            row = [generate_unique_id(i)]  # ID column
            
            for j in range(2, num_columns + 1):
                if j % 2 == 0:
                    # 50% chance of integer value
                    row.append('"'+str(random.randint(1, 100))+'"')
                else:
                    # 50% chance of 1 or 2
                    row.append('"'+str(random.choice([1, 2]))+'"')
                    
            writer.writerow(row)
        
        print(f"CSV file '{filename}' generated successfully.")


# Generate IDs file
def generate_ids_file(filename:str, num_rows:int):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
        
        # Write header row
        header = ['PROJECT_PSEUDO_ID']
        writer.writerow(header)
        
        # Write data rows
        for i in range(num_rows):
            if i%10000 == 0: print(f"{i} rows added")
            row = [generate_unique_id(i)]  # ID column                                            
            writer.writerow(row)
        
        print(f"IDs CSV file '{filename}' generated successfully.")



# Get the absolute path of the folder containing this script
# Set the output folder at './bigfiles' (currently in git's .gitignore)
script_folder = os.path.abspath(os.path.dirname(__file__))
output_folder = os.path.abspath(os.path.join(script_folder, 'bigfiles'))

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

num_columns = 200
num_rows = 150000

#generate_ids_file(os.path.abspath(os.path.join(output_folder, 'pseudo_ids.csv')), num_rows)
#generate_csv_file(os.path.abspath(os.path.join(output_folder, 'a1_data.csv')), num_columns, num_rows)
#generate_csv_file(os.path.abspath(os.path.join(output_folder, 'a2_data.csv')), num_columns, num_rows)
#generate_csv_file(os.path.abspath(os.path.join(output_folder, 'a3_data.csv')), num_columns, num_rows)
generate_csv_file(os.path.abspath(os.path.join(output_folder, 'a4_data.csv')), num_columns, num_rows)
generate_csv_file(os.path.abspath(os.path.join(output_folder, 'a5_data.csv')), num_columns, num_rows)
generate_csv_file(os.path.abspath(os.path.join(output_folder, 'a6_data.csv')), num_columns, num_rows)
generate_csv_file(os.path.abspath(os.path.join(output_folder, 'a7_data.csv')), num_columns, num_rows)
generate_csv_file(os.path.abspath(os.path.join(output_folder, 'a8_data.csv')), num_columns, num_rows)
generate_csv_file(os.path.abspath(os.path.join(output_folder, 'a9_data.csv')), num_columns, num_rows)
generate_csv_file(os.path.abspath(os.path.join(output_folder, 'a10_data.csv')), num_columns, num_rows)




