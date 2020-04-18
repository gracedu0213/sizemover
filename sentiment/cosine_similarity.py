# -*- coding: utf-8 -*-
"""
Version 1.0 dated 04-Apr-2020

@author: Kim Criel and Taeyoung Park

This script will calculate the change of language in the MD&A sections (comparing one year to the next) and the output is stored in a JSON format containing a nested structure of data and score per Central Index Key. The equivalent is also stored in a log file in logs/, whereas in results/ you can find the JSON file.
"""

import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import os
from tqdm import tqdm
import json

###############################################################################
# We are taking the output of the download_clean_10k script.
# For 99.5% of the cases we have the right extraction, some other cases require to clean up once again.
# That's why we need to repeat the clean up regular expressions

input_directory = './mda_extract/'
output_directory = './results/'
log_directory = './logs/'
master_index_df = 'master_index/master_index_filtered.csv'
cosine_stats_file = 'similarity_stats.log'
cosine_json_file = 'similarity_stats.json'

###############################################################################
# One area we wanted to test whether the cosine similarity gave different results before and after stemming
# Hint: it didn't turn out to influence the results significantly at all, so disabbled in general
# Adapted from https://www.datacamp.com/community/tutorials/stemming-lemmatization-python

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
# Once again we are reading the master index and adding the relative paths to the files

master_index_df = pd.read_csv(master_index_df, sep = ',')
master_index_df['File'] = master_index_df['TXT'].str.split('/', expand = True)[6] + '/' + master_index_df['TXT'].str.split('/', expand = True)[7]

###############################################################################
# Define log files to inspect the processing

# Verify existence of log directory and create if not exists
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

statsf = open(log_directory + cosine_stats_file, 'w')
cos_sim_results_dict = {}

cik_df = master_index_df['CIK'].unique()

# Not all MD&A reports can be extracted corrctly or sometimes refer to obscure page numbering (that afterwards gets lost in the way the original file is stored)
# We found that below 2 KB we have this behaviour consistently
mda_size_threshold = 2048

# In below code we will calculate the cosine similarity after performing TF-IDF
# We did not opt to calculate the Jaccard similarity measure as the cosine one is better
# Jaccard suffers from a bias towards longer document and not taking into account term frequency
# Important remark here is that we are not stemming or cleaning the extracts
# Running the code with a stemmer didn't give any noticeable difference in the results

# Include progress bar (tqdm library)
with tqdm(total=len(cik_df)) as pbar:
    for cik in cik_df:
        pbar.update(1)
        loop_df = master_index_df[master_index_df['CIK'] == cik].sort_values(by=['Date Filed'], ascending=True)

        tfidf_input = []
        # We'll store the size of the files and the filed dates in separate lists for easier processing
        # size_list contains booleans for those files below the threshold
        size_list = []
        date_list = []
        for index, row in loop_df.iterrows():
            try:
                with open(input_directory + row['File'], 'r+', encoding = 'mbcs') as f:
                    data = f.read()
                    if data == '':
                        tfidf_input.append(stemmer('empty', stemmer_enabled))
                    else:
                        tfidf_input.append(stemmer(data, stemmer_enabled))
                    size_list.append(os.path.getsize(input_directory + row['File']) < mda_size_threshold)
                    date_list.append(row['Date Filed'])
                f.close()
            except Exception as e:
                print(e)
                tfidf_input.append('empty')
        # Cosine similarity compares two values, so we'll get a result that's one less in lenght
        # If the preceding entry was below the treshold, then we need to correct: remove the last entry as we compare to the previous
        # For the dates, we remove the first entry as the cosine similarity goes into effect on the latter date
        date_list.pop(0)
        size_list_shifted = size_list.copy()
        size_list_shifted.pop()
        size_list.pop(0)
        # Term frequency-inverse document frequency weighing
        # https://nlp.stanford.edu/IR-book/html/htmledition/tf-idf-weighting-1.html
        # https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html
        # Example on http://blog.christianperone.com/2011/10/machine-learning-text-feature-extraction-tf-idf-part-ii/
        # https://www.analyticsvidhya.com/blog/2017/06/word-embeddings-count-word2veec/
        tfidf_vectorizer = TfidfVectorizer()
        tfidf_matrix = tfidf_vectorizer.fit_transform(tfidf_input)
        cos_sim = cosine_similarity(tfidf_matrix)
        cos_sim_result = [round(x, 4) for x in np.diag(cos_sim, k=1)]

        cos_sim_corrected_result = [cs if not sz and not szs else 1.0 for (cs, sz, szs) in zip(cos_sim_result, size_list, size_list_shifted)]
        # import matplotlib.pyplot as plt
        # plt.hist(cos_sim_corrected_result, bins = 5)
        # plt.show()

        cos_sim_results_dict[str(cik)] = {dt:cs for (cs, dt) in zip(cos_sim_corrected_result, date_list)}

        for cs, dt in zip(cos_sim_corrected_result, date_list):
            statsf.write(str(cik) + ',' + str(dt) + ',' + str(cs) +'\n')

# Verify existence of output directory and create if not exists
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

with open(output_directory + cosine_json_file, 'w') as f:
    json.dump(cos_sim_results_dict, f)
    f.close()

statsf.close()