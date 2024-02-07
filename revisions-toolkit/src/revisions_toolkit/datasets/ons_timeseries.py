
from pathlib import PurePosixPath
from typing import Any, Dict

import sys
import logging
import re
import zipfile
import io
import time
import pandas as pd

from kedro.io import AbstractDataset

try:
    import boerequests
    sys.modules["requests"] = boerequests # Replace all requests with boerequests
except:
    import requests as boerequests

logger = logging.getLogger(__name__)


class ons_timeseries(AbstractDataset):
    """A class representing the ONS Timeseries dataset.

    This dataset contains the monthly revisions of the Headline Monthly GDP data.

    Args:
        folderpath (str): The folder path where the data will be saved.

    Attributes:
        _base_url (str): The base URL for downloading the data.
        _folderpath (PurePosixPath): The folder path where the data will be saved.
        _extracted_files (list): A list of extracted file names.
        _release_date (str): The release date of the dataset.
    """


    def __init__(self, writepath: str, base_url: str, dataset_name: str):
        self._writepath = PurePosixPath(writepath)
        self._base_url = base_url
        self._name = dataset_name

    def _load(self) -> Dict[str, pd.DataFrame]:

        df_dict = {}
        
        # Send a GET request to the ONS website
        response = boerequests.get(self._base_url)
        
        # Check if the response was successful
        if response.status_code == 200:
            identified_file_links =  re.findall(r'"(/file\?uri=[^"]*.csv)', response.text)
            
            for link in identified_file_links:
                download_link = "https://www.ons.gov.uk" + link
                
                # Load the data as a dictionary to hold each sheet within a df
                data = {}
                
                # Get the right encoding for the csv file
                # Get the content-type header
                content_type = response.headers.get('Content-Type')

                # Split the content-type header to get the parameters
                params = content_type.split(';')
                
                # Find the charset parameter
                charset = next((param for param in params if 'charset' in param), None)

                if charset:
                    # Extract the encoding from the charset parameter
                    encoding = charset.split('=')[1]
                else:
                    # If the charset parameter is not present, assume the encoding is utf-8
                    encoding = 'utf-8'
                
                with io.StringIO(response.content) as z:
                    data = pd.read_csv(io.StringIO(response.content.decode(encoding)))

                df_dict[link] = data
        return df_dict        
       