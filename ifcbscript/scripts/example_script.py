# django configuration preamble

import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'ifcbscript.settings')

import django

django.setup()

# script proper

from scripts.api import *

SOURCE_DIR = r'C:/Data/ifcb_data/api_testing'
DEST_DIR = os.path.join(SOURCE_DIR, 'destination')

dd = data_directory(SOURCE_DIR)

import_bins(dd)

bins = select_bins(end_time='2022-04-12')

bins.with_data(dd).copy(DEST_DIR)