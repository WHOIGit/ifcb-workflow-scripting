# django configuration preamble

import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'ifcbscript.settings')

import django

django.setup()

# script proper

from scripts.api import *

SOURCE_DIR = r'C:/Data/ifcb_data/api_testing'
DEST_DIR = os.path.join(SOURCE_DIR, 'destination')

# configure a directory as a source of raw data
dd = data_directory(SOURCE_DIR)

# scan the data source and record basic metadata about each bin
import_bins(dd)

# query all bins for a subset of the bins
# this queries all known bins, not just the ones that were imported,
# and known bins persist in the database
bins = select_bins(end_time='2022-04-12')

# given those bins and the data source, copy the raw data to a destination directory
bins.with_data(dd).copy(DEST_DIR)