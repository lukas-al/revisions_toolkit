from pathlib import PurePosixPath
from typing import Any, Dict

import sys
import logging
import re
import zipfile
import io
import pandas as pd

from kedro.io import AbstractDataset

try:
    import boerequests
    sys.modules["requests"] = boerequests # Replace all requests with boerequests
except:
    import requests as boerequests

logger = logging.getLogger(__name__)

class HeadlineGDPVintage(AbstractDataset):
    """``HeadlineGDPVintage`` loads and saves the most recent version of the Quarterly UK headline GDP revisions from the ONS website.
    
    Example:
    ::
    
    >>> HeadlineGDPVintage()
    """
    
    def __init__(self, folderpath: str):
        self._base_url = "https://www.ons.gov.uk/economy/grossdomesticproductgdp/datasets/revisionstrianglesforukgdpabmi"
        self._folderpath = PurePosixPath(folderpath)
        self._extracted_files = None
        self._release_date = None
        
    def _load(self) -> pd.DataFrame:
        """Loads data from the ONS website."""
        
        logger.info("Downloading the latest release from the ONS website...")
        
        # Send a GET request to the ONS website
        response = boerequests.get(self._base_url)
        
        # Check if the response was successful
        if response.status_code == 200:
            
            # Find the latest release URL from the page content
            pattern = r'<a href="(/file\?uri=.*?)".*?>'
            latest_release_url = re.search(pattern, response.text).group(1)
            latest_release_url = "https://www.ons.gov.uk" + latest_release_url
            logger.info("Found the latest release URL: " + latest_release_url)
            
            # Get the release date from the URL
            quarter = re.findall(r"quarter(\d)", latest_release_url)
            year = re.findall(r"(\d{4})", latest_release_url)

            self._release_date = "Q" + quarter[0] + " " + year[0]
            
            # Send a GET request to the latest release URL and get the response
            response = boerequests.get(latest_release_url)

            # Check if the request was successful
            if response.status_code == 200:
                # Return the data as the zipfile object
                
                # First check the zipfile has the expected file
                expected_file = "ABMI - Quarterly GDP at Market Prices.xlsx"
                if expected_file not in zipfile.ZipFile(io.BytesIO(response.content)).namelist():
                    logger.error("[bold red blink]Failed to download the latest release due to missing file: " + expected_file, extra={"markup": True})
                    raise FileNotFoundError
                
                # Load the data as a dictionary to hold each sheet within a df
                data = {}
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    for filename in z.namelist():
                        with z.open(filename) as f:
                            data[filename] = pd.read_excel(f)
                
                # Return the data as a dictionary of dataframes
                return data

            else:
                logger.error("[bold red blink]Failed to download the latest release due to status code: " + str(response.status_code), extra={"markup": True})
                raise ConnectionError
        else:
            logger.error("[bold red blink]Failed to access the data page due to status code: " + str(response.status_code), extra={"markup": True})
            raise ConnectionError
        
    def _save(self, data: dict[pd.DataFrame]) -> None:
        """Saves data to the specified filepath."""

        for filename, df in data.items():
            df.to_excel(self._folderpath / filename)

        self._extracted_files = data.namelist()  # Log the list of extracted files
        logger.info("Successfully saved the latest release data from the ONS website.")

    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes the attributes of the dataset."""
        return dict(folderpath=self._folderpath, release_date=self._release_date, extracted_files=self._extracted_files)
