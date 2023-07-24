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
import traceback
from .missing_participant_row_exception import MissingParticipantRowException

def load_val(data_frames:Dict[str,pd.core.frame.DataFrame],file:str,col:str,participant_id:str)->int:
    try:
        val = (data_frames[file].loc[participant_id])[col]
        return val;
    except KeyError as ke:
        raise MissingParticipantRowException(ke)    


def load_and_index_csv_datafiles(config_file_path:str) -> Dict[str,pd.core.frame.DataFrame]:

    data_frames:Dict[str,pd.core.frame.DataFrame] = {}

    #load transformation configuration file
    f = open(config_file_path)
    data = json.load(f)
    assessment_variables = data.keys();

    #default set of columns that must be loaded (regardless the configuration)
    default_columns:Set[str] = {'project_pseudo_id'}

    #key: 'file name', value: list of variables to be read in such a file
    required_csv_columns:Dict[str,set] = {}

    #required_csv_columns = list(assessment_variables).copy();
    #required_csv_columns.append('project_pseudo_id');

    datafiles:Set[str] = set()


    #get the CSV files that are needed for the transformation
    for assessment_variable in assessment_variables:
        var_assessment_files = data[assessment_variable]

        #get the files, and their respective columns that need to be read
        for varversion in var_assessment_files:
            filename:str = list(varversion.values())[0]     
            
            datafiles.add(filename)

            if not filename in required_csv_columns:
                required_csv_columns[filename]=default_columns.copy()

            required_csv_columns[filename].add(assessment_variable)

    #create an indexed dataframe for each datafile
    for file in datafiles:
        
        #load only the needed columns
        print(f"Loading and indexing {file}. Columns:{required_csv_columns[file]}")        
        data_frames[file] = pd.read_csv(file,na_filter=False,dtype=str,usecols=required_csv_columns[file]);  
        print(str(data_frames[file]))
        data_frames[file].set_index('project_pseudo_id',inplace=True)
        process = psutil.Process()
        memory_usage = process.memory_info().rss / 1024 ** 2

        print(f"{file} loaded and indexed. Total memory usage: {memory_usage} MB")

    
    return data_frames
    
    #print(f"Data indexed in {end_load-start_load} ms")


def generate_csd(participant_id:str,config:dict,data_frames:Dict[str,pd.core.frame.DataFrame])->dict:
    assessment_variables = config.keys()
    
    output = {"project_pseudo_id":{"1a":participant_id}}
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
                if var_value!='' and var_value[0]!='$':
                    var_assessments[assessment_name] = str(load_val(data_frames,assessment_file,assessment_variable,participant_id))
                else:
                    logging.info(f'Skipping value: Missing value code ({var_value}) in assessment {varversion} of variable {assessment_variable}')
            except MissingParticipantRowException as mr:
                logging.info(f'Skipping value: Missing row for participant [{participant_id}] in file [{assessment_file}]  when looking for of variable {assessment_variable}')
                        
        output[assessment_variable]=var_assessments

    return output    


def load_ids(ids_file)->List[str]:
    content_list:List[str] = []

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
            if progress_count%100==0:
                process_end_time = time.time()
                print(f'{progress_count} files processed. Elapsed time: {process_end_time - process_start_time} sec ({progress_count/(process_end_time - process_start_time)} rows/s)')
        except Exception as e:
            traceback.print_exc()
            process_end_time = time.time()
            print(f"An error occurred after processing {progress_count} rows: {str(e)}. Time elapsed: {process_end_time - process_start_time} sec.")               
            sys.exit(1)     

    process_end_time = time.time()
    print(f"{progress_count} files created on {args.output_folder} in {process_end_time - process_start_time} sec.")   

if __name__ == '__main__':
    main()