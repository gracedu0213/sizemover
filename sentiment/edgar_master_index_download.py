# -*- coding: utf-8 -*-
"""
Version 1.0 dated 30-Mar-2020

@author: Kim Criel and Taeyoung Park

This script retrieves the master index of all SEC filings and will store the results in a compressed file named master_index.zip in master_index/ (compressed the file size is ~300 MB versus 2.6 GB uncompressed). We retrieve all the filings as of 1999.
"""

import edgar
import os
import shutil

###############################################################################
# Download master index of SEC EDGAR since 1999. No need to do this everytime,but once
# Documentation of library: https://pypi.org/project/python-edgar/

if __name__ == "__main__":
    edgar_dir = './master_index/'
    edgar_since_year = 1999

    if not os.path.exists(edgar_dir):
        os.makedirs(edgar_dir)

    edgar.download_index(edgar_dir, edgar_since_year, skip_all_present_except_last=False)
    
    shutil.make_archive('master_index', 'zip', edgar_dir)

    list_files = os.listdir(edgar_dir)
    tsv_files = [file for file in list_files if file.endswith('.tsv')]
    for file in tsv_files:
    	# print(os.path.join(edgar_dir, file))
    	os.remove(os.path.join(edgar_dir, file))
    
    shutil.move('master_index.zip', edgar_dir + 'master_index.zip')