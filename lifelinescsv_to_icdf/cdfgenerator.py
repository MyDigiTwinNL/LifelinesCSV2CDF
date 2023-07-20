import pandas as pd
import time
import resource
import json
from typing import Set, Dict, List
import uuid
import argparse
import csv
import os
import sys
import psutil
import logging
from lifelinescsv_to_icdf.missing_participant_row_exception import MissingParticipantRowException

def load_val(data_frames:Dict[str,pd.core.frame.DataFrame],file:str,col:str,participant_id:str)->int:
    try:
        val = (data_frames[file].loc[participant_id])[col]
        return val;
    except KeyError as ke:
        raise MissingParticipantRowException(ke)    


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


    
    #print(f"Data indexed in {end_load-start_load} ms")


def generate_csd(participant_id:str,config:dict,data_frames:Dict[str,pd.core.frame.DataFrame])->dict:
    assessment_variables = config.keys()
    
    output = {"PROJECT_PSEUDO_ID":{"1A":participant_id}}
    for assessment_variable in assessment_variables:
        var_assessments = {}
        var_assessment_files = config[assessment_variable]
        for varversion in var_assessment_files:
            assessment_name = list(varversion.keys())[0]
            assessment_file = list(varversion.values())[0]                 
            # Return each value encapsulated in quotes

            try:
                var_value = str(load_val(data_frames,assessment_file,assessment_variable,participant_id));
                #skip missing values (start with '$' in lifelines)
                if var_value[0]!='$':
                    var_assessments[assessment_name] = str(load_val(data_frames,assessment_file,assessment_variable,participant_id))
                else:
                    logging.info(f'Skipping value: Missing value code ({var_value}) in assessment {varversion} of variable {assessment_variable}')
            except MissingParticipantRowException as mr:
                logging.info(f'Skipping value: Missing row for participant [{participant_id}] in file [{assessment_file}]  when looking for of variable {assessment_variable}')
                        
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
    parser = argparse.ArgumentParser(description='Transform Lifelines CSV files into CDF (cohort-data JSON format).')

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

    if not os.path.exists(args.output_folder):
        print(f"The specified output folder path '${args.output_folder}' does not exist.")
        return


    #load rows identifiers and transformation configuration settings
    
    load_start_time = time.time()
    ids = load_ids(args.ids_file)
    data_frames = load_and_index_csv_datafiles(args.config_file)
    load_end_time = time.time()

    print(f"{len(data_frames)} CSV files loaded and indexed in {load_end_time - load_start_time} seconds.")

    process = psutil.Process()
    memory_usage = process.memory_info().rss / 1024 ** 2

    print(f"Total memory usage: {memory_usage} MB")


    config_file = open(args.config_file)
    config_params = json.load(config_file)

    progress_count = 0;


    process_start_time = time.time()
    for id in ids:
        try:
            participant_data = generate_csd(id,config_params,data_frames)
            output_file = os.path.join(args.output_folder,id+".cdf.json")        
            with open(output_file, 'w') as json_file:
                json.dump(participant_data, json_file)
            progress_count += 1
            if progress_count%10000==0:
                process_end_time = time.time()
                print(f'{progress_count} files processed. Elapsed time: {process_end_time - process_start_time} sec ({progress_count/(process_end_time - process_start_time)} rows/s)')
        except Exception as e:
            process_end_time = time.time()
            print(f"An error occurred after processing {progress_count} rows: {str(e)}. Time elapsed: {process_end_time - process_start_time} sec.")               
            sys.exit(1)     

    process_end_time = time.time()
    print(f"{progress_count} files created on {args.output_folder} in {process_end_time - process_start_time} sec.")   

if __name__ == '__main__':
    main()