# -*- coding: utf-8 -*-
"""
Version 1.0 dated 12-Apr-2014

@author: Kim Criel and Taeyoung Park
"""

import pandas as pd
import re
import os
from tqdm import tqdm

###############################################################################
# We are taking the output of the download_clean_10k script.
# For 99.5% of the cases we have the right extraction, some other cases require to clean up once again.
# That's why we need to repeat the clean up regular expressions

download_directory = './edgar_download/'
output_directory = './mda_extract/'
log_directory = './logs/'
master_index_df = 'master_index/master_index_filtered.csv'
extract_error_file = 'extract_error.log'
extract_stats_file = 'extract_stats.log'

debug = False

# Files below this size typically do not contain MD&A information
size_threshold = 3*1024

# Create output file path
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

###############################################################################
# Once again we are reading the master index and adding the relative paths to the files

master_index_df = pd.read_csv(master_index_df, sep = ',')
master_index_df['File'] = master_index_df['TXT'].str.split('/', expand = True)[6] + '/' + master_index_df['TXT'].str.split('/', expand = True)[7]

###############################################################################
# We will define three variants for processing to optimize the number of extracted MD&A sections

def mda_extract(data):
    # The lookback serves one purpose: eliminate item 7 references, that way we can identify the right start of item 7
    # List was made based on extracting all item 7 references and compiling that into one list
    # If an item in the report gets referenced, then typically you well find one of the following words just before
    lookback = [" Part II", " found ", " see ", " refer to ", " included in "," contained in ", " set forth ", " under ",
            " market risk", " Data ", " end of this ", " in ", " Note "]
    lookback = [word.lower() for word in lookback]

    lookback_max = len(max(lookback, key=len))+1

    # Remove all kinds of attachments except the core 10-K or 10-K405
    data = re.sub('<TYPE>(?!10-K|10-K405)[\S\s]*<\/TEXT>', '', data)
    # Remove HTML tags and pre-processing (second time as we had leftovers following the download for <0.1% of the dataset)
    data = re.sub('\n', ' ', data)
    data = re.sub('>', '>\n', data)
    # Remove newlines within HTML tags
    data = re.sub('<.*?>','', data)
    # Remove non-breaking spaces (HTML leftovers)
    data = re.sub('&nbsp;', ' ', data)
    # Replace double spaces
    data = re.sub(' {2,}', ' ', data)
    # Remove all HTML entities: https://developer.mozilla.org/en-US/docs/Glossary/Entity
    # and https://stackoverflow.com/questions/26127775/remove-html-entities-and-extract-text-content-using-regex
    data = re.sub('/&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});','', data)
    # Clean up signatures
    data = re.sub('\/s\/\s|\/s\/','', data)
    # Final cleaning (keep words with slashes, ampersands and dashes)
    data = re.sub('[^0-9a-zA-Z][\s\t]+?&-\/\'', '', data)
    # Clean up previous line
    data = re.sub('&#\d*;','', data)
    data = re.sub('\n\s*\n','\n', data)
    data = re.sub('\r', '\n', data)

    data = re.sub('Ite[m\s:]*\d\.*[\S ]*\.+"*\n', '', data, flags=re.I)

    find_items = re.findall('Ite[m\s\n]*7[^A(]|Ite[m\s\n]*8', data, flags=re.I)
    find_indices = [m.start() for m in re.finditer('Ite[m\s\n]*7[^A(]|Ite[m\s\n]*8', data, flags=re.I)]

    item_matches = []
    new_find_items = []
    new_find_indices = []

    for index in range(len(find_items)):
        if not any(word in data[find_indices[index]-lookback_max:find_indices[index]].lower() for word in lookback):
            new_find_items.append(find_items[index])
            new_find_indices.append(find_indices[index])

    for index in range(len(new_find_items)-1):
        if '7' in new_find_items[index] and '8' in new_find_items[index+1]:
            range7 = new_find_indices[index] # 30 characters allows to look for the word discussion
            range8 = new_find_indices[index+1] # 30 characters allows to look for the word financial
            if 'discussion' in data[range7:range7+40].lower() and 'financial' in data[range8:range8+40].lower():
                item_matches.append((new_find_indices[index],new_find_indices[index+1]))
    new_item_matches = []

    if len(item_matches) > 1:
        new_item_matches.append(max(item_matches,key=lambda item:item[1]-item[0]))
        data = data[new_item_matches[0][0] : new_item_matches[0][1]]
        statsf.write('Multiple match ' + str(row['File']) + ':' + str(new_item_matches[0]) + 'len' + str(len(item_matches)) + '\n')

    elif len(item_matches) == 0:
        data = ''
        statsf.write('Empty match ' + str(row['File']) + '\n')

        if debug == True:
            print(row['File'])
            print(find_items)
            print(find_indices)
            print(new_find_items)
            print(new_find_indices)
            # print(item_matches)
    else:
        new_item_matches.append(item_matches[0])
        data = data[new_item_matches[0][0] : new_item_matches[0][1]]
        statsf.write('Single match ' + str(row['File']) + ':' + str(new_item_matches[0]) + '\n')

    # As we are looking for textual changes, we are going to remove all numbers
    data = re.sub('\d','', data)
    # Final replacement of double spaces
    data = re.sub(' {2,}', ' ', data)

    # Finally we prepare the output for sentiment analysis based on the Loughran McDonaldMaster Dictionary
    # by removing parens and possessives as well as leftovers from Item 7A
    # https://drive.google.com/file/d/0B4niqV00F3msQ3lVeGpKSEg4QUU/view

    data = re.sub("'s|'|\(|\)", '', data)
    data = re.sub('\n\.', '', data)
    data = re.sub('[%|$|+|-]\n', '', data)
    data = re.sub('[+|-]\.\n', '', data)
    data = re.sub('\n\n', '', data)

    return data

def mda_extract_without_in(data):
    lookback = [" Part II", " found ", " see ", " refer to ", " included in "," contained in ", " set forth ", " under ",
            " market risk", " Data ", " end of this ", " Note "]
    lookback = [word.lower() for word in lookback]

    lookback_max = len(max(lookback, key=len))+1

    data = re.sub('<TYPE>(?!10-K|10-K405)[\S\s]*<\/TEXT>', '', data)
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
    data = re.sub('\r', '\n', data)

    data = re.sub('Ite[m\s:]*\d\.*[\S ]*\.+"*\n', '', data, flags=re.I)

    find_items = re.findall('Ite[m\s\n]*7[^A(]|Ite[m\s\n]*8', data, flags=re.I)
    find_indices = [m.start() for m in re.finditer('Ite[m\s\n]*7[^A(]|Ite[m\s\n]*8', data, flags=re.I)]

    item_matches = []
    new_find_items = []
    new_find_indices = []

    for index in range(len(find_items)):
        if not any(word in data[find_indices[index]-lookback_max:find_indices[index]].lower() for word in lookback):
            new_find_items.append(find_items[index])
            new_find_indices.append(find_indices[index])

    for index in range(len(new_find_items)-1):
        if '7' in new_find_items[index] and '8' in new_find_items[index+1]:
            range7 = new_find_indices[index]
            range8 = new_find_indices[index+1]
            if 'discussion' in data[range7:range7+40].lower() and 'financial' in data[range8:range8+40].lower():
                item_matches.append((new_find_indices[index],new_find_indices[index+1]))
    new_item_matches = []

    if len(item_matches) > 1:
        new_item_matches.append(max(item_matches,key=lambda item:item[1]-item[0]))
        data = data[new_item_matches[0][0] : new_item_matches[0][1]]
        statsf.write('Multiple match ' + str(row['File']) + ':' + str(new_item_matches[0]) + 'len' + str(len(item_matches)) + '\n')

    elif len(item_matches) == 0:
        data = ''
        statsf.write('Empty match ' + str(row['File']) + '\n')

        if debug == True:
            print(row['File'])
            print(find_items)
            print(find_indices)
            print(new_find_items)
            print(new_find_indices)
    else:
        new_item_matches.append(item_matches[0])
        data = data[new_item_matches[0][0] : new_item_matches[0][1]]
        statsf.write('Single match ' + str(row['File']) + ':' + str(new_item_matches[0]) + '\n')

    data = re.sub('\d','', data)
    data = re.sub(' {2,}', ' ', data)
    data = re.sub("'s|'|\(|\)", '', data)
    data = re.sub('\n\.', '', data)
    data = re.sub('[%|$|+|-]\n', '', data)
    data = re.sub('[+|-]\.\n', '', data)
    data = re.sub('\n\n', '', data)

    return data

def mda_extract_regex_change(data):
    # Actually we're not using lookback here, below word does not exist, code to be cleaned up
    # Difference is at the start with additional regular expressions as well as a five word lookback before the word Item
    # This serves on eliminating references to items
    lookback = [' Kabelsalat ']
    lookback = [word.lower() for word in lookback]
    lookback_max = len(max(lookback, key=len))+1

    data = re.sub('\r', '', data )
    data = re.sub('I\r\ntem', 'Item', data, flags=re.I)
    data = re.sub('It\r\nem', 'Item', data, flags=re.I)
    data = re.sub('Ite\r\nm', 'Item', data, flags=re.I)

    data = re.sub('I\ntem', 'Item', data, flags=re.I)
    data = re.sub('It\nem', 'Item', data, flags=re.I)
    data = re.sub('Ite\nm', 'Item', data, flags=re.I)

    data = re.sub('<TYPE>(?!10-K|10-K405)[\S\s]*<\/TEXT>', '', data)
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
    data = re.sub('\r', '\n', data)

    data = re.sub('([a-zA-Z\"\'-]+ +){5}Ite[m\s:]*\d', r'\1', data, flags=re.I)

    find_items = re.findall('Ite[m\s\n]*7[^A(]|Ite[m\s\n]*8', data, flags=re.I)
    find_indices = [m.start() for m in re.finditer('Ite[m\s\n]*7[^A(]|Ite[m\s\n]*8', data, flags=re.I)]

    item_matches = []
    new_find_items = []
    new_find_indices = []

    for index in range(len(find_items)):
        if not any(word in data[find_indices[index]-lookback_max:find_indices[index]].lower() for word in lookback):
            new_find_items.append(find_items[index])
            new_find_indices.append(find_indices[index])

    for index in range(len(new_find_items)-1):
        if '7' in new_find_items[index] and '8' in new_find_items[index+1]:
            range7 = new_find_indices[index]
            range8 = new_find_indices[index+1]
            if 'discussion' in data[range7:range7+40].lower() and 'financial' in data[range8:range8+40].lower():
                item_matches.append((new_find_indices[index],new_find_indices[index+1]))
    new_item_matches = []

    if len(item_matches) > 1:
        new_item_matches.append(max(item_matches,key=lambda item:item[1]-item[0]))
        data = data[new_item_matches[0][0] : new_item_matches[0][1]]
        statsf.write('Multiple match ' + str(row['File']) + ':' + str(new_item_matches[0]) + 'len' + str(len(item_matches)) + '\n')

    elif len(item_matches) == 0:
        data = ''
        statsf.write('Empty match ' + str(row['File']) + '\n')

        if debug == True:
            print(row['File'])
            print(find_items)
            print(find_indices)
            print(new_find_items)
            print(new_find_indices)
    else:
        new_item_matches.append(item_matches[0])
        data = data[new_item_matches[0][0] : new_item_matches[0][1]]
        statsf.write('Single match ' + str(row['File']) + ':' + str(new_item_matches[0]) + '\n')

    data = re.sub('\d','', data)
    data = re.sub(' {2,}', ' ', data)
    data = re.sub("'s|'|\(|\)", '', data)
    data = re.sub('\n\.', '', data)
    data = re.sub('[%|$|+|-]\n', '', data)
    data = re.sub('[+|-]\.\n', '', data)
    data = re.sub('\n\n', '', data)
    return data

###############################################################################
# Define log files to inspect the processing

# Verify existence of log directory and create if not exists
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

errorf = open(log_directory + extract_error_file, 'w')
statsf = open(log_directory + extract_stats_file, 'w')

with tqdm(total=master_index_df.shape[0]) as pbar:
    for index, row in master_index_df.iterrows():
        pbar.update(1)

        # Create subdirectory to store the results for each CIK
        if os.path.isfile(output_directory  + row['File']):
            errorf.write('File exists: {0}\n'.format(str(row['File'])))
        #else:
        elif os.path.isfile(download_directory  + row['File']):
            # print(row['File'])

            # Instead of running line by line, we'll take the entire file (as the input comes from a pdf)
            # https://stackoverflow.com/questions/454456/how-do-i-re-search-or-re-match-on-a-whole-file-without-reading-it-all-into-memor
            # https://docs.python.org/3/library/mmap.html
            # Small note on the encoding, there were parsing errors and had to revert to mbcs (multi-byte character set) as we had charmap decode errors
            # See https://stackoverflow.com/questions/53954988/python-unicodedecodeerror-charmap-codec-cant-decode-byte-0x9d-in-position

            with open(download_directory + row['File'], 'r+', encoding = 'mbcs') as f:
                if not os.path.exists(output_directory + str(row['CIK'])):
                    os.makedirs(output_directory + str(row['CIK']))

                data = f.read()

                size_list = []

                data_mda = mda_extract(data)
                if len(data_mda) >= size_threshold:
                    data = data_mda
                else:
                    data_mda_regex = mda_extract_regex_change(data)
                    if len(data_mda_regex) >= size_threshold:
                        data = data_mda_regex
                    else:
                        data_mda_without_in = mda_extract_without_in(data)
                        if len(data_mda_without_in) >= size_threshold:
                            data = data_mda_without_in
                        else:
                            data = data_mda

                # Write the extract Management Discussion part into a new file
                outf = open(output_directory  + row['File'], 'w')
                outf.write(data)
                outf.close()
                f.close()

    errorf.close()
    statsf.close()