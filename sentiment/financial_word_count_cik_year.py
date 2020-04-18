# -*- coding: utf-8 -*-
"""
Version 1.0 dated 13-Apr-2020

@author: Kim Criel and Taeyoung Park

This script computes the word count per Central Index Key per year in a JSON format for visualization.
"""

import pandas as pd
import os
from tqdm import tqdm
import json
import csv
import re

###############################################################################
# We are taking the output of the download_clean_10k script.

input_directory = './mda_extract/'
output_directory = './results/'
log_directory = './logs/'
master_index_df = 'master_index/master_index_filtered.csv'
dictionary_directory = './master_dictionary/'

word_count_stats_file = 'financial_word_count_cik_year_stats.log'
word_count_json_file = 'financial_word_count_cik_year_stats.json'

###############################################################################
# Re-using our code from cosine similarity

from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer

ps = PorterStemmer()

def stemmer(data, enabled):
    if not enabled:
        return data
    else:
        stemmed_words = []
        words = word_tokenize(data)

        for word in words:
            stemmed_words.append(ps.stem(word))

        return " ".join(stemmed_words)

stemmer_enabled = False

###############################################################################
# Function to read in csv files for the sentiment dictionaries

def dict_csv_reader(dict_file):
    output = []
    with open(dict_file, newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            output.append(row[0])
        return output

###############################################################################
# Import sentiment dictionaries based no Loughran & McDonald

stop_words_csv = 'stop_words.csv'
positive_words_csv = 'positive_words.csv'
negative_words_csv = 'negative_words.csv'
litigious_words_csv  = 'litigious_words.csv'

stop_words = dict_csv_reader(dictionary_directory + stop_words_csv)
positive_words = dict_csv_reader(dictionary_directory + positive_words_csv)
negative_words = dict_csv_reader(dictionary_directory + negative_words_csv)
litigious_words  = dict_csv_reader(dictionary_directory + litigious_words_csv)

###############################################################################
# Once again we are reading the master index and adding the relative paths to the files

master_index_df = pd.read_csv(master_index_df, sep = ',')
master_index_df['File'] = master_index_df['TXT'].str.split('/', expand = True)[6] + '/' + master_index_df['TXT'].str.split('/', expand = True)[7]

###############################################################################
# In below code we are calculating the relative proportional difference based on the dictionaries of Loughran & McDonald
# Define log files to inspect the processing

# Verify existence of log directory and create if not exists
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

statsf = open(log_directory + word_count_stats_file, 'w')
sentiment_results_dict = {}

cik_df = master_index_df['CIK'].unique()

# Not all MD&A reports can be extracted corrctly or sometimes refer to obscure page numbering (that afterwards gets lost in the way the original file is stored)
# We found that below 2 KB we have this behaviour consistently
mda_size_threshold = 2048

# Consolidate the three word lists
consolidated_words = positive_words + negative_words + litigious_words
word_count_results_dict = {}

# Include progress bar (tqdm library)
with tqdm(total=len(cik_df)) as pbar:
    for cik in cik_df:
        pbar.update(1)
        loop_df = master_index_df[master_index_df['CIK'] == cik].sort_values(by=['Date Filed'], ascending=True)

        word_count_results_dict[str(cik)] = {}

        for index, row in loop_df.iterrows():
            word_count_dict = {}
            year = row['Date Filed'][:4]
            try:
                with open(input_directory + row['File'], 'r+', encoding = 'mbcs') as f:
                    data = f.read().lower()

                    data_without_stop_words  = [word for word in re.split("\W+", data) if word not in stop_words and len(word) > 1]

                    for word in data_without_stop_words:
                        if word in consolidated_words:
                            if word_count_dict.get(word) != None:
                                word_count_dict[word] += 1
                            else:
                                word_count_dict[word] = 1
            except Exception as e:
                print(e)

            word_count_results_dict[str(cik)][str(year)] = word_count_dict

            for k, v in word_count_dict.items():
                statsf.write(str(cik) + ',' + str(year) + ',' + str(k) + ',' + str(v) +'\n')

# Verify existence of output directory and create if not exists
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

with open(output_directory + word_count_json_file, 'w') as f:
    json.dump(word_count_results_dict, f)
    f.close()

statsf.close()