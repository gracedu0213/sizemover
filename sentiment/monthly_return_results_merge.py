# -*- coding: utf-8 -*-
"""
Version 1.0 dated 12-Apr-2020

@author: Kim Criel and Taeyoung Park
"""

import pandas as pd
import json

###############################################################################
data_dir = '../../Data/'
results_dir = './results/'

monthly_file = data_dir + 'NA_Ret_Data_Clean_v1.xlsx'
cosine_json_file = 'similarity_stats.json'
sentiment_json_file = 'sentiment_stats.json'
output_file = data_dir + 'NA_Ret_Data_Clean_Sentiment_v2.xlsx'

###############################################################################
# Load JSON result files into dataframes for manipulation

with open(results_dir + cosine_json_file) as json_file:
    cosine_dict = json.load(json_file)

with open(results_dir + sentiment_json_file) as json_file:
    sentiment_dict = json.load(json_file)

cosine_df = pd.DataFrame.from_dict(cosine_dict, orient='index').stack().reset_index()
cosine_df.columns = ['cik', 'date', 'similarity']
cosine_df.sort_values(['cik', 'date'], inplace=True)
cosine_df.reset_index(drop=True, inplace=True)
cosine_df['date'] = cosine_df['date'].str.replace('-', '')

sentiment_df = pd.DataFrame.from_dict(sentiment_dict, orient='index').stack().reset_index()
sentiment_df.columns = ['cik', 'date', 'sentiment']
sentiment_df.sort_values(['cik', 'date'], inplace=True)
sentiment_df.reset_index(drop=True, inplace=True)
sentiment_df['date'] = sentiment_df['date'].str.replace('-', '')

merged_sentiment_cosine = pd.merge(sentiment_df, cosine_df, on=['cik','date'], how='left')
merged_sentiment_cosine['YM'] = merged_sentiment_cosine['date'].str[:6]
merged_sentiment_cosine.drop(['date'], axis=1, inplace=True)
merged_sentiment_cosine['cik'] = merged_sentiment_cosine['cik'].astype(int)
merged_sentiment_cosine['YM'] = merged_sentiment_cosine['YM'].astype(int)

###############################################################################
# Read in monthly file and merge with sentiment/similarity results

monthly_df = pd.read_excel(monthly_file, sheet_name='DATA')

# monthly_df['year_month'] = monthly_df['datadate'].str[6:10] + monthly_df['datadate'].str[0:2]
monthly_df['cik'] = monthly_df['cik'].fillna(0.0).astype(int)
monthly_df['cik'] = monthly_df['cik'].astype(int)
monthly_df['YM'] = monthly_df['YM'].astype(int)

merged_df = pd.merge(monthly_df, merged_sentiment_cosine, on=['cik', 'YM'], how='left')

merged_df.to_excel(output_file, sheet_name='DATA', index=False)