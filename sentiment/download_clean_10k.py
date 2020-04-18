# -*- coding: utf-8 -*-
"""
Version 1.0 dated 31-Mar-2020

@author: Kim Criel and Taeyoung Park

This script downloads the annual reports and already filters out the binary files along with cleaning up the data structure in the raw text files. Unprocessed download size is 500 GB. We ran this particular script on multiple Azure Data Science Virtual Machines. The script already contains process parallelization to facilitate the download. The result is stored in download/ and has one folder per Central Index Key (company).
"""

import pandas as pd
import wget
import re
import os
from threading import Thread
import multiprocessing

###############################################################################
# Filtered master index file from edgar_master_index_zip.py
# Downloaded and pre-treated files to be stored locally

master_index_df = 'master_index/master_index_filtered.csv'
output_directory = './edgar_download/'

###############################################################################
# In order to download the 10-K forms (hosted on SEC EDGAR), we'll need to process nearly 500 GB of data
# Below function takes four arguments:
    # dataframe that contains the download URIs
    # download path
    # chunk parameter: which section of the download list get processed
    # chunk total parameter: total number of chunks

def dl_clean_10k(df, dl_dir, chunk, chunk_total):

    # Add log file for incomplete downloads and/or other messages
    log_file = 'dl_error' + str(chunk) + '.log'

    # Read in master index and append target CIK subdirectory and file
    master_index_df = pd.read_csv(df, sep = ',')
    master_index_df['File'] = master_index_df['TXT'].str.split('/', expand = True)[6] + '/' + master_index_df['TXT'].str.split('/', expand = True)[7]

    # Logic to chunk the master index into different ranges
    range_len = int(round(int(master_index_df.shape[0]) / int(chunk_total), 0))

    df_range_start = (chunk-1) * range_len
    if chunk == chunk_total:
        df_range_end = int(master_index_df.shape[0]) + 1
    else:
        df_range_end = chunk * range_len
    # print(df_range_start, df_range_end)

    master_index_df = master_index_df[df_range_start:df_range_end]


    # Verify existence of output directory and create if not exists
    if not os.path.exists(dl_dir):
        os.makedirs(dl_dir)

    logf = open(log_file, "w")

    for index, row in master_index_df.iterrows():
        if os.path.isfile(output_directory  + row['File']):
            # print('File exists: {0}'.format(str(row['File'])))
            # logf.write('File exists: {0}\n'.format(str(row['File'])))
            pass
        else:
            try:
                link_dir = output_directory + str(row['CIK'])  + '/'
                if not os.path.exists(link_dir):
                    os.makedirs(link_dir)

                link = wget.download(row['TXT'], out = link_dir)

                with open(link, 'r+') as f:
                    data = f.read()

                    f.seek(0)
                    # The following line will remove inline spreadsheets, images and so on
                    # Aim is to reduce file size drastically here (10-K reports can easily be tens to hundreds of MB in size)
                    # With those two lines we bring that down to about 1 MB and that contains the textual information of the 10-K
                    find_indices = [m for m in re.finditer('<DOCUMENT>\n<TYPE>10-K[\s\S]*?<\/DOCUMENT>', data, flags=re.I)]
                    data = data[find_indices[0].start():find_indices[0].end()]
                    # Refer to extract_mda.py for reasons behind the below (as we started that code first before scaling to the entire dataset)
                    data = re.sub('\n', ' ', data)
                    data = re.sub('>', '>\n', data)
                    data = re.sub('<.*?>','', data)
                    data = re.sub('&nbsp;', ' ', data)
                    data = re.sub(' {2,}', ' ', data)
                    data = re.sub('/&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});','', data)
                    data = re.sub('\/s\/\s|\/s\/','', data)
                    data = re.sub('[^0-9a-zA-Z][\s\t]+?&-\/\'', '', data)
                    data = re.sub('&#\d*;','', data)
                    data = re.sub('\n\s*\n','\n', data)

                    f.truncate()
                    f.write(data)
                    f.close()

            except Exception as e:
                # print('Failed to download {0}: {1}\n'.format(str(row['File']), str(e))
                logf.write('Failed to download {0}: {1}\n'.format(str(row['File']), str(e)))
    logf.close()

# # dl_clean_10k(master_index_df, output_directory, 1, 5)
# # dl_clean_10k(master_index_df, output_directory, 2, 5)
# # dl_clean_10k(master_index_df, output_directory, 3, 5)
# # dl_clean_10k(master_index_df, output_directory, 4, 5)
# # dl_clean_10k(master_index_df, output_directory, 5, 5)

# Verify that output path already exists, otherwise create (wget falls over otherwise)
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Implementation with concurrent threads to speed up downloading
# concurrent_threads = 5
# threads = []
# for index in range(concurrent_threads):
#     process = Thread(target=dl_clean_10k, args=[master_index_df, output_directory, index+1, concurrent_threads])
#     process.start()
#     threads.append(process)

# for process in threads:
#     process.join()

# Implementation with concurrent processes to speed up downloading
# Number of processes to be tweaked in function of CPO
# In the end we ran three machines concurrently on yet another subset (above)
if __name__ == '__main__':
    concurrent_processes = 5
    processes = []
    for index in range(concurrent_processes):
        process = multiprocessing.Process(target=dl_clean_10k, args=(master_index_df, output_directory, index+1, concurrent_processes,))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

# if __name__ == "__main__":
#     df = sys.argv[1]
#     dl = sys.argv[2]
#     chunk = int(sys.argv[3])
#     chunk_total = int(sys.argv[4])
#     dl_clean_10k(df, dl, chunk, chunk_total)