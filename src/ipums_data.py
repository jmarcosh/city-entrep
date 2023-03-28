from ipumspy import IpumsApiClient, UsaExtract, readers
from utils.load_config_files import load_config
import os



ipums = IpumsApiClient(load_config()['ipums_api_key'])
DOWNLOAD_DIR = os.path.dirname(os.path.abspath(__file__))

ddi = readers.read_ipums_ddi('usa_00011.xml')
ipums_df = readers.read_microdata(ddi, ddi.file_description.filename)


recent_extracts = ipums.retrieve_previous_extracts('usa')
extract_info = recent_extracts[0]

# extract_id = extract_info['number']
#
#
# # Get a BaseExtract object from the extract ID and format
# extract = ipums.resubmit_purged_extract(extract_id)
#
# ipums.wait_for_extract(extract)
#
# ipums.download_extract(extract, download_dir=DOWNLOAD_DIR, stata_command_file=True)
#
# # Get the DDI
# ddi_file = list(DOWNLOAD_DIR.glob("*.xml"))[0]
# ddi = readers.read_ipums_ddi(ddi_file)
#
# # Get the data
# ipums_df = readers.read_microdata(ddi, DOWNLOAD_DIR / ddi.file_description.filename)
#
# # specify the path to the .dat.gz file
# file_path = "usa_00010.dat.gz"
#
# # read the file using gzip and numpy
# with gzip.open(file_path, 'rb') as f:
#     data = np.frombuffer(f.read(), dtype=np.float32)
#
# import pandas as pd

ipums = IpumsApiClient(load_config()['ipums_api_key'])

# submit an extract request to the Microdata Extract API
extract = UsaExtract(
    ["us2021a"],
    ["STATEFIP", "COUNTYFIP", "MET2013", "CITY", "PERWT", "EDUCD", "EMPSTATD", "OCC", "INDNAICS"],
    description = "Employment Status - City2"
)
ipums.submit_extract(extract)

# check status of a request
extract_status = ipums.extract_status(extract)

# convenience method that will wait until extract processing is complete before returning.
ipums.wait_for_extract(extract)

# download an extract to the current working directory
# you can use the optional download_dir="DIRECTORY_NAME" parameter
# to specify a different location
ipums.download_extract(extract, stata_command_file=True)


print()
