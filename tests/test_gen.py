import unittest
import pandas as pd
from lifelinescsv_to_icdf import cdfgenerator
from typing import Set, Dict, List

# Define class to test the program
class CSVFilesToCDF(unittest.TestCase):

    def test_addition(self):      

        #Dataframes indexed by PROJECT_PSEUDO_ID
        file_a = {'PROJECT_PSEUDO_ID':  ['participantA','participantB','participantC'],
                  'var1':['1'           ,'5'          ,'7'],
                  'var2':['2'           ,'5'          ,'6']}
        file_b = {'PROJECT_PSEUDO_ID':  ['participantA','participantB','participantC'],
                  'var1':['20'          ,'90'          ,'70'],
                  'var2':['12'          ,'15'          ,'26']}
        file_c = {'PROJECT_PSEUDO_ID':  ['participantA','participantB','participantC'],
                  'var1':['100'         ,'500'         ,'700'],
                  'var2':['200'         ,'500'         ,'600']}

        df_dict:Dict[str,pd.core.frame.DataFrame]=dict()

        df_dict['file_a'] = pd.DataFrame(data=file_a)
        df_dict['file_a'].set_index('PROJECT_PSEUDO_ID',inplace=True)

        df_dict['file_b'] = pd.DataFrame(data=file_b)
        df_dict['file_b'].set_index('PROJECT_PSEUDO_ID',inplace=True)

        df_dict['file_c'] = pd.DataFrame(data=file_c)
        df_dict['file_c'].set_index('PROJECT_PSEUDO_ID',inplace=True)


        #Configuration file
        config = {}

        config['var1'] = [{"1A":'file_a'},{'1B':'file_b'},{'1C':'file_c'}]
        config['var2'] = [{"3A":'file_a'},{'3B':'file_b'},{'3C':'file_c'}]

        expected_output_participantA = {
            'PROJECT_PSEUDO_ID':{"1A":'participantA'},
            'var1':{"1A":"1","1B":"20","1C":"100"},
            'var2':{"3A":"2","3B":"12","3C":"200"}     
        }
  
                
        self.assertEqual(cdfgenerator.generate_csd('participantA',config,df_dict),expected_output_participantA,"Not equal")
