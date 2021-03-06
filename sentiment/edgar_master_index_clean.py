# -*- coding: utf-8 -*-
"""
Version 1.0 dated 7-Apr-2020

@author: Kim Criel and Taeyoung Park

This script takes the compressed master index and merges it with the companies listed in the monthly return data file (sheet named "Mapping"). The output only considers 10-K forms (i.e. annual reports). The corresponding path to the monthly returns mapping sheet must be set in this script (e.g. data/). The output is file named master_index_filtered.csv that contains the CIK, filing date, form type and URI to access the annual report.
"""

import glob
import pandas as pd
from zipfile import ZipFile

###############################################################################
# Cleanup of the file obtained in edgar_master_index_download.py

edgar_dir = './master_index/'
data_dir = '../data/'

# Full dataset
# monthly_archive = ZipFile('../Data/na_monthly_1980_to_2020.zip', 'r')
# monthly_file = monthly_archive.open('na_monthly 1980_to_2020.csv')
# Cleaned dataset mapping (mapping sheet taken from the excel and stored as csv
# monthly_file = data_dir + 'NA_Ret_Data_Clean_v1.csv'
monthly_file = data_dir + 'NA_Ret_Data_Clean_v1.xlsx'

###############################################################################
# We run the below on the compressed archive that we obtained in the previous step
# Alternative is to use glob to match tsv files and return those file in a list for looping

# extension = '*.tsv'
# edgar_index = glob.glob(edgar_dir + extension)

edgar_archive = ZipFile(edgar_dir + 'master_index.zip', 'r')
edgar_index = edgar_archive.namelist()

# Index file contains the following information
# Company Name, Form Type, CIK (Central Index Key), Date Filed, and File Name (text and html)
# In the pre-processing we need a few steps:
# Drop the HTML column as the text files suffice
# Prefix the rest of the URI to retrieve the text files
# Filter on 10-K and its variants (e.g. 10-K405)

edgar_columns = ['CIK', 'Company Name', 'Form Type', 'Date Filed', 'TXT', 'HTML']
edgar_df = pd.DataFrame(columns = edgar_columns)
edgar_df.drop(['HTML'], axis = 1, inplace = True)
edgar_archive_uri = 'https://www.sec.gov/Archives/'

number_of_rows = 0

for index in edgar_index:
    compressed_file = edgar_archive.open(index)
    qtr_df = pd.read_csv(compressed_file, sep = '|', header = None)
    # Add column headers
    qtr_df.set_axis(edgar_columns, axis=1, inplace = True)
    # Drop last column
    qtr_df.drop(['HTML'], axis = 1, inplace = True)
    # Search for 10-K variants 10-K, 10-KSB and 10-K405; we take out the Non-Timely reports
    qtr_df = qtr_df[qtr_df['Form Type'].str.contains("^10-K$|10-K405$|10-KSB$")]
    qtr_df['TXT'] = edgar_archive_uri + qtr_df['TXT'].astype(str)
    edgar_df = pd.concat([edgar_df, qtr_df])
    number_of_rows += qtr_df.shape[0]
edgar_archive.close()

print('Total number of entries in filtered master index: ', number_of_rows)

# Turned out to be very manageable in size (from 2+GB to 20 MB)
# compression_opts = dict(method = 'zip', archive_name = 'edgar_index_10k.csv')
# edgar_df.to_csv('edgar_index_10k.zip', index = False, compression=compression_opts)
edgar_df.to_csv(edgar_dir + 'edgar_index_10k.csv', index = False)

###############################################################################
# We need to extract the relevant Central Index Keys from our monthly return dataset

# monthly_df = pd.read_csv(monthly_file, sep = ';')
monthly_df = pd.read_excel(monthly_file, sheet_name='Mapping')
# We need to fill NA values the int casting won't work
monthly_df['cik'] = monthly_df['cik'].fillna(0.0).astype(int)
# monthly_df = monthly_df[monthly_df['fic'] == 'USA']

# cik_list = monthly_df['cik']
monthly_df.drop_duplicates(subset='cik', inplace = True)
monthly_df = monthly_df[monthly_df['cik'] != 0]
monthly_df.rename(columns={'cik' : 'CIK'}, inplace = True)

edgar_filtered_df = pd.merge(monthly_df['CIK'], edgar_df, on='CIK', how='inner')
edgar_filtered_df = edgar_filtered_df[edgar_df.columns]

edgar_filtered_df.to_csv(edgar_dir + 'master_index_filtered2.csv', index = False)

edgar_filtered_df = edgar_filtered_df.reset_index(drop = True)