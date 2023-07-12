import pandas as pd
import time
import resource
import json
from typing import Set, Dict, List
import uuid
import argparse
import csv
import os

def load_val(data_frames:Dict[str,pd.core.frame.DataFrame],file:str,col:str,id:str)->int:
    return (data_frames[file].loc[id])[col]
    
def pseudo_ids()->List[str]:
    ids_list:List[str] = []
    for i in range(2,99):
        ids_list.append(str(uuid.uuid3(uuid.NAMESPACE_DNS,"row"+str(i)))[:15])
    return ids_list


def load_and_index_csv_datafiles(config_file_path:str) -> Dict[str,pd.core.frame.DataFrame]:

    data_frames:Dict[str,pd.core.frame.DataFrame] = {}

    #load transformation configuration file
    f = open(config_file_path)
    data = json.load(f)
    assessment_variables = data.keys();
    datafiles:Set[set] = set()

    #get the CSV files that are needed for the transformation
    for assessment_variable in assessment_variables:
        var_assessment_files = data[assessment_variable]
        for varversion in var_assessment_files:        
            datafiles.add(list(varversion.values())[0])

    #create an indexed dataframe for each datafile
    for file in datafiles:
        data_frames[file] = pd.read_csv(file);
        data_frames[file].set_index('PROJECT_PSEUDO_ID',inplace=True)
    
    return data_frames


    #start_load = time.time()
    #end_load = time.time()

    #print(f"Data indexed in {end_load-start_load} ms")


def generate_csd(id:str,config:dict,data_frames:Dict[str,pd.core.frame.DataFrame])->dict:
    assessment_variables = config.keys()
    
    output = {"PROJECT_PSEUDO_ID":{"1A":id}}
    for assessment_variable in assessment_variables:
        var_assessments = {}
        var_assessment_files = config[assessment_variable]
        for varversion in var_assessment_files:
            assessment_name = list(varversion.keys())[0]
            assessment_file = list(varversion.values())[0]                 
            var_assessments[assessment_name] = load_val(data_frames,assessment_file,assessment_variable,id)    
        output[assessment_variable]=var_assessments
    return output    


def load_ids(ids_file)->List[str]:
    content_list = List[str]

    with open(ids_file, 'r') as f:
        reader = csv.reader(f)
        next(reader) # skip header
        content_list = [row[0] for row in reader]
    
    return content_list


def main():
    # Create the command-line argument parser
    parser = argparse.ArgumentParser(description='Generate JSON output files based on a configuration.')

    # Add the command-line arguments
    parser.add_argument('ids_file', help='Path to the CSV file with a list of IDs.')
    parser.add_argument('config_file', help='Path to the JSON configuration file.')
    parser.add_argument('output_folder', help='Path to the output folder.')

    # Parse the command-line arguments
    args = parser.parse_args()

    if not os.path.isfile(args.ids_file):
        print(f"The specified file path '${args.ids_file}' does not exist.")
        return

    if not os.path.isfile(args.config_file):
        print(f"The specified file path '${args.config_file}' does not exist.")
        return


    #load rows identifiers and transformation configuration settings
    ids = load_ids(args.ids_file)
    data_frames = load_and_index_csv_datafiles(args.config_file)

    config_file = open(args.config_file)
    config_params = json.load(config_file)


    for id in ids:
        participant_data = generate_csd(id,config_params,data_frames)
        print(participant_data)

if __name__ == '__main__':
    main()