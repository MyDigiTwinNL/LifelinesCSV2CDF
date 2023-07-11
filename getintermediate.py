import pandas as pd
import time
import resource
import json
from typing import Set, Dict, List
import uuid

def load_val(data_frames:Dict[str,pd.core.frame.DataFrame],file:str,col:str,id:str)->int:
    return (data_frames[file].loc[id])[col]
    
def pseudo_ids()->List[str]:
    ids_list:List[str] = []
    for i in range(2,99):
        ids_list.append(str(uuid.uuid3(uuid.NAMESPACE_DNS,"row"+str(i)))[:15])
    return ids_list


data_frames:Dict[str,pd.core.frame.DataFrame] = {}

#load transformation configuration file
f = open('columns.json')
data = json.load(f)
keys = data.keys();
datafiles:Set[set] = set()

#identify required files to be loaded and indexed
start_load = time.time()

for assessment_variable in keys:
    var_assessment_files = data[assessment_variable]
    for varversion in var_assessment_files:        
        datafiles.add(list(varversion.values())[0])


#load dataframes from datafiles
for file in datafiles:
    data_frames[file] = pd.read_csv(file);
    data_frames[file].set_index('ID',inplace=True)

end_load = time.time()

print(f"Data indexed in {end_load-start_load} ms")

#Intermediate files generation

ids_list = pseudo_ids();

#for psid in ids_list:

psid = "316a03f0-93b6-3"

output = {"ID":psid}

#get key-values
for assessment_variable in keys:
    var_assessments = {}
    print(assessment_variable)
    var_assessment_files = data[assessment_variable]
    print('VAF:'+str(var_assessment_files))
    for varversion in var_assessment_files:
        assessment_name = list(varversion.keys())[0]
        assessment_file = list(varversion.values())[0]                 
        var_assessments[assessment_name] = load_val(data_frames,assessment_file,assessment_variable,psid)    
    output[assessment_variable]=var_assessments

print(output)


