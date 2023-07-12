import unittest
import pandas as pd
from lifelinescsv_to_icdf import cdfgenerator
from typing import Set, Dict, List

# Define class to test the program
class CSVFilesToCDF(unittest.TestCase):

    def test_transformation_with_complete_data(self):      

        #Dataframes indexed by PROJECT_PSEUDO_ID
        file_a = {'PROJECT_PSEUDO_ID':  ['participantA','participantB','participantC'],
                               'var1':  ['1'           ,'5'          ,'7'],
                               'var2':  ['2'           ,'5'          ,'6']}
        file_b = {'PROJECT_PSEUDO_ID':  ['participantA','participantB','participantC'],
                               'var1':  ['20'          ,'90'          ,'70'],
                               'var2':  ['12'          ,'15'          ,'26']}
        file_c = {'PROJECT_PSEUDO_ID':  ['participantA','participantB','participantC'],
                               'var1':  ['100'         ,'500'         ,'700'],
                               'var2':  ['200'         ,'500'         ,'600']}

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

        expected_output_participantC = {
            'PROJECT_PSEUDO_ID':{"1A":'participantC'},
            'var1':{"1A":"7","1B":"70","1C":"700"},
            'var2':{"3A":"6","3B":"26","3C":"600"}     
        }
  
        self.assertEqual(cdfgenerator.generate_csd('participantA',config,df_dict),expected_output_participantA,"CSV to CDF transformation not generating the expected output.")
        self.assertEqual(cdfgenerator.generate_csd('participantC',config,df_dict),expected_output_participantC,"CSV to CDF transformation not generating the expected output.")


    def test_transformation_with_missing_values(self):      

        #Dataframes indexed by PROJECT_PSEUDO_ID
        file_a = {'PROJECT_PSEUDO_ID':  ['participantA','participantB','participantC'],
                  'var1':['$4'           ,'5'          ,'7'],
                  'var2':['2'           ,'5'          ,'6']}
        file_b = {'PROJECT_PSEUDO_ID':  ['participantA','participantB','participantC'],
                  'var1':['20'          ,'90'          ,'$5'],
                  'var2':['12'          ,'15'          ,'26']}
        file_c = {'PROJECT_PSEUDO_ID':  ['participantA','participantB','participantC'],
                  'var1':['100'         ,'500'         ,'$6'],
                  'var2':['$7'         ,'500'         ,'600']}

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
            'var1':{"1B":"20","1C":"100"},
            'var2':{"3A":"2","3B":"12"}     
        }
  
        expected_output_participantC = {
            'PROJECT_PSEUDO_ID':{"1A":'participantC'},
            'var1':{"1A":"7"},
            'var2':{"3A":"6","3B":"26","3C":"600"}     
        }
                
        self.assertEqual(cdfgenerator.generate_csd('participantA',config,df_dict),expected_output_participantA,"CSV (with missing values) to CDF transformation not generating the expected output.")
        self.assertEqual(cdfgenerator.generate_csd('participantC',config,df_dict),expected_output_participantC,"CSV (with missing values) to CDF transformation not generating the expected output.")

