# -*- coding: utf-8 -*-
"""
Version 1.0 dated 31-Mar-2020

@author: Kim Criel and Taeyoung Park

Before running this script, the dictionaries need to be retrieved from https://sraf.nd.edu/textual-analysis/resources/ - we need both the 2018 Master Dictionary in csv format and the Generic Stop Words file in txt format. This script will extract positive, negative, litigious and stop words and store the files in master_dictionary/ for subsequent steps.
"""

import pandas as pd

###############################################################################
output_directory = './master_dictionary/'

# Loughran & McDonald dictionaries for financial applications
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