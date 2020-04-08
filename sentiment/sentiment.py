# -*- coding: utf-8 -*-
"""
Version x.0 dated 

@author: Kim Criel and Taeyoung Park
"""

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics import jaccard_score
from sklearn.feature_extraction.text import TfidfVectorizer
import os
from tqdm import tqdm
import json
import csv
import re


###############################################################################
# We are taking the output of the download_clean_10k script.
# For 99.5% of the cases we have the right extraction, some other cases require to clean up once again.
# That's why we need to repeat the clean up regular expressions

input_directory = './mda_extract/'
output_directory = './results/'
log_directory = './logs/'
master_index_df = 'master_index/master_index_filtered.csv'
dictionary_directory = './master_dictionary/'

sentiment_stats_file = 'sentiment_stats.log'
sentiment_json_file = 'sentiment_list_stats.json'

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
        stem_sentence=[]
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
# Import sentiment dictionaries

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
# Define log files to inspect the processing

# Verify existence of log directory and create if not exists
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

statsf = open(log_directory + sentiment_stats_file, 'w')
sentiment_results_dict = {}

cik_df = master_index_df['CIK'].unique()

# Not all MD&A reports can be extracted corrctly or sometimes refer to obscure page numbering (that afterwards gets lost in the way the original file is stored)
# We found that below 2 KB we have this behaviour consistently
mda_size_threshold = 2048

# In below code we will calculate the cosine similarity after performing TF-IDF
# We did not opt to calculate the Jaccard similarity measure as the cosine one is better
# Jaccard suffers from a bias towards longer document and not taking into account term frequency
# Important remark here is that we are not stemming or cleaning the extracts
# TODO run code with stemmer for comparison

# Include progress bar (tqdm library)
with tqdm(total=len(cik_df)) as pbar:
    for cik in cik_df:
        pbar.update(1)
        loop_df = master_index_df[master_index_df['CIK'] == cik].sort_values(by=['Date Filed'], ascending=True)
        
        # We'll store the sfiled dates in a separate list for easier processing
        date_list = []
        sentiment_list = []
        for index, row in loop_df.iterrows():
            sentiment_score = 0.0
            pos = 0.0
            neg = 0.0
            lit = 0.0
            try:
                with open(input_directory + row['File'], 'r+', encoding = 'mbcs') as f:
                    data = f.read().lower()
                    
                    date_list.append(row['Date Filed'])
                    
                    data_without_stop_words  = [word for word in re.split("\W+", data) if word not in stop_words and len(word) > 1]
                    
                    for word in data_without_stop_words:
                        if word in positive_words:
                            pos += 1.0
                        if word in negative_words:
                            neg += 1.0
                        if word in litigious_words:
                            lit += 1.0
                    # Basic metric Relative Proportional Difference (between -1 and 1)
                    if (pos+neg) != 0.0:
                        sentiment_score = round((pos-neg) / (pos+neg), 4)
                    else:
                        sentiment_score = 0.0
            except Exception as e:
                print(e)
            sentiment_list.append(sentiment_score)

        sentiment_results_dict[str(cik)] = {dt:cs for (cs, dt) in zip(sentiment_list, date_list)}
        
        for cs, dt in zip(sentiment_list, date_list):
            statsf.write(str(cik) + ',' + str(dt) + ',' + str(cs) +'\n')

# Verify existence of output directory and create if not exists
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

with open(output_directory + sentiment_json_file, 'w') as f:
    json.dump(sentiment_results_dict, f)
    f.close()

statsf.close()