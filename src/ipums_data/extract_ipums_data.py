from ipumspy import IpumsApiClient, UsaExtract, readers
from utils.load_config_files import load_config


ipums = IpumsApiClient(load_config()['ipums_api_key'])

# submit an extract request to the Microdata Extract API
# extract = UsaExtract(
#     ["us2021a"],
#     ["STATEFIP", "COUNTYFIP", "MET2013", "CITY", "PERWT", "EDUCD", "CLASSWKRD", "OCC", "IND", "AGE", "LABFORCE"],
#     description="Employment Status - City5"
# )

extract = UsaExtract(
    ["us2021a"],
    ["STATEFIP", "COUNTYFIP", "MET2013", "CITY", "PERWT", "EDUCD", "CLASSWKRD", "OCC", "IND", "AGE", "LABFORCE",
     "SEX", "MARST", "RACE", "HISPAN"],
    description="Sample with more variables"
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