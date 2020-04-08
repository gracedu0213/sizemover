# -*- coding: utf-8 -*-
"""
Version x.0 dated 

@author: Kim Criel and Taeyoung Park
"""

import numpy as np
import pandas as pd


###############################################################################

# TODO Define variables of download and result directories
output_directory = './master_dictionary/'

# Obtain from https://sraf.nd.edu/textual-analysis/resources/
master_dict_raw = 'LoughranMcDonald_MasterDictionary_2018.csv'
stop_word_raw = 'StopWords_Generic.txt'

###############################################################################

       
def read_csv(filename, header='infer'):
    return pd.read_csv(filename, sep = ",", header = header)

master_dict = read_csv(output_directory + master_dict_raw)

stop_words = read_csv(output_directory + stop_word_raw, header = None)

stop_words = stop_words[0].str.lower()

positive_words = master_dict.loc[master_dict['Positive'] > 0]['Word'].str.lower()

negative_words = master_dict.loc[master_dict['Negative'] > 0]['Word'].str.lower()

litigious_words  = master_dict.loc[master_dict['Litigious'] > 0]['Word'].str.lower()


stop_words.to_csv (output_directory + 'stop_words.csv', index = False, header = False)

positive_words.to_csv (output_directory + 'positive_words.csv', index = False, header = False)

negative_words.to_csv (output_directory + 'negative_words.csv', index = False, header = False)

litigious_words.to_csv (output_directory + 'litigious_words.csv', index = False, header = False)