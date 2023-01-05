# django configuration preamble

import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'ifcbscript.settings')

import django

django.setup()

# script proper

from scripts.api import data_directory

SOURCE_DIR = r'C:/Data/ifcb_data/api_testing'
DEST_DIR = os.path.join(SOURCE_DIR, 'destination')

# configure a directory as a source of raw data
dd = data_directory(SOURCE_DIR)

# scan the data source and record basic metadata about each bin
bins = dd.import_bins()

# do a time range query on those bins
bins = bins.filter(end_time='2022-04-12')

# copy the raw data for the matching bins to the destination directory
bins.copy(DEST_DIR, layout='day')

# desired API stuff
# logging
# bins.set(sample_type='discrete')
# import_metadata('foo.csv')
# bins.export_metadata('bar.csv')
# bins.parse_metadata() # parses HDR file
# bins.compute('ml_analyzed')
# bins.zip('/some/output/dir')
# export_images('image_list.csv', '/some/output/dir') # how to specify source data
# export_images('image_list.csv', 'foo.zip')
## the image list could include a column specifying an output directory name per-image

