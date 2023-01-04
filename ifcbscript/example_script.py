# django configuration preamble

import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'ifcbscript.settings')

import django

django.setup()

# script proper

from scripts.api import data_directory, import_bins, select_bins

SOURCE_DIR = r'C:/Data/ifcb_data/api_testing'
DEST_DIR = os.path.join(SOURCE_DIR, 'destination')

# configure a directory as a source of raw data
dd = data_directory(SOURCE_DIR)

# scan the data source and record basic metadata about each bin
bins = import_bins(dd)

# do a time range query on those bins
bins = bins.filter(end_time='2022-04-12')

# copy the raw data for the matching bins to the destination directory
bins.copy(DEST_DIR)
