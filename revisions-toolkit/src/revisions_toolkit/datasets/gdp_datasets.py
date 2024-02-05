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

class HeadlineQGDPVintage(AbstractDataset):
    """``HeadlineGDPVintage`` loads and saves the most recent version of the Quarterly UK headline GDP revisions from the ONS website.

    Args:
        folderpath (str): The folder path where the dataset will be saved.

    Attributes:
        _base_url (str): The base URL of the ONS website.
        _folderpath (PurePosixPath): The folder path where the dataset will be saved.
        _extracted_files (None or List[str]): The list of extracted files from the dataset.
        _release_date (None or str): The release date of the dataset.

    Methods:
        _load(): Loads the quarterly GDP vintages (revision triangles) data from the ONS website.
        _save(data: Dict[str, pd.DataFrame]) -> None: Saves the data to the specified filepath.
        _describe() -> Dict[str, Any]: Returns a dictionary that describes the attributes of the dataset.
    """
    
    def __init__(self, folderpath: str):
        self._base_url = "https://www.ons.gov.uk/economy/grossdomesticproductgdp/datasets/revisionstrianglesforukgdpabmi"
        self._folderpath = PurePosixPath(folderpath)
        self._extracted_files = None
        self._release_date = None
        
    def _load(self) -> Dict[str, pd.DataFrame]:
        """Loads the quarterly GDP vintages (revision triangles) data from the ONS website.
        
        Returns:
            A dictionary containing the dataframes for each sheet in the downloaded release.
        
        Raises:
            FileNotFoundError: If the expected file is missing in the downloaded release.
            ConnectionError: If there is an issue with downloading the release or accessing the data page.
        """
        
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
                            data[filename] = pd.read_excel(f, sheet_name=None)
                
                # Return the data as a dictionary of dataframes
                return data

            else:
                logger.error("[bold red blink]Failed to download the latest release due to status code: " + str(response.status_code), extra={"markup": True})
                raise ConnectionError
        else:
            logger.error("[bold red blink]Failed to access the data page due to status code: " + str(response.status_code), extra={"markup": True})
            raise ConnectionError
        
    def _save(self, data: Dict[str, pd.DataFrame]) -> None:
            """
            Saves the data to the specified filepath.

            Args:
                data (Dict[str, pd.DataFrame]): A dictionary containing the data to be saved.
                    The keys represent the filenames, and the values represent the corresponding
                    pandas DataFrame objects.

            Returns:
                None
            """

            for filename, df in data.items():
                df.to_excel(self._folderpath / filename)

            self._extracted_files = data.namelist()  # Log the list of extracted files
            logger.info("Successfully saved the latest QGDP release data from the ONS website.")

    def _describe(self) -> Dict[str, Any]:
            """Returns a dictionary that describes the attributes of the dataset.

            Returns:
                A dictionary containing the following attributes:
                - folderpath: The folder path of the dataset.
                - release_date: The release date of the dataset.
                - extracted_files: The list of extracted files from the dataset.
            """
            return dict(folderpath=self._folderpath, release_date=self._release_date, extracted_files=self._extracted_files)


class HeadlineMGDPVintage(AbstractDataset):
    """A class representing the Headline Monthly GDP Vintage dataset.

    This dataset contains the monthly revisions of the Headline Monthly GDP data.

    Args:
        folderpath (str): The folder path where the data will be saved.

    Attributes:
        _base_url (str): The base URL for downloading the data.
        _folderpath (PurePosixPath): The folder path where the data will be saved.
        _extracted_files (list): A list of extracted file names.
        _release_date (str): The release date of the dataset.
    """


    def __init__(self, folderpath: str):
        self._base_url = "https://www.ons.gov.uk/file?uri=/economy/grossdomesticproductgdp/datasets/revisionstrianglesformonthlygdp/current/" 
        self._folderpath = PurePosixPath(folderpath)
        self._extracted_files = None
        self._release_date = None


    def _load(self) -> Dict[str, pd.DataFrame]:
        """Load the Headline Monthly GDP Vintage dataset.

        Returns:
            dict: A dictionary containing the dataframes for each sheet in the dataset.

        Raises:
            FileNotFoundError: If the expected file is not found in the downloaded zip file.
        """

        logger.info("Downloading the latest Monthly GDP release from the ONS website...")
        
        # Load the data
        response_data = get_latest_data(self._base_url)

        expected_file = "MGDP Revision triangle (M on M).xlsx"
        if expected_file not in zipfile.ZipFile(io.BytesIO(response_data.content)).namelist():
            logger.error("[bold red blink]Failed to download the latest release due to missing file: " + expected_file, extra={"markup": True})
            raise FileNotFoundError(f"The expected file {expected_file} is not in the zip file.")
        
        # Load the data as a dictionary to hold each sheet within a df
        data = {}
        with zipfile.ZipFile(io.BytesIO(response_data.content)) as z:
            for filename in z.namelist():
                with z.open(filename) as f:
                    data[filename] = pd.read_excel(f, sheet_name=None)
        
        # Return the data as a dictionary of dataframes
        return data


    def _save(self, data: Dict[str, pd.DataFrame]) -> None:
        """Save the data to the specified filepath.

        Args:
            data (dict): A dictionary containing the dataframes to be saved.
        """

        for filename, df in data.items():
            df.to_excel(self._folderpath / filename)

        self._extracted_files = data.namelist()
        logger.info("Successfully saved the latest MGDP release data from the ONS website.")


    def _describe(self) -> Dict[str, Any]:
            """Returns a dictionary that describes the attributes of the dataset.

            Returns:
                A dictionary containing the following attributes:
                - folderpath: The folder path of the dataset.
                - release_date: The release date of the dataset.
                - extracted_files: The list of extracted files from the dataset.
            """
            return dict(folderpath=self._folderpath, release_date=self._release_date, extracted_files=self._extracted_files)

class ComponentQGDPVintage(AbstractDataset):
    
    def __init__(self, folderpath: str):
        self._base_url = "https://www.ons.gov.uk/economy/grossdomesticproductgdp/datasets/revisionstrianglesforcomponentsfortheexpenditureapproachtothemeasureofukgdp"
        self._folderpath = PurePosixPath(folderpath)
        self._extracted_files = None
        self._release_date = None
        
    def _load(self) -> Dict[str, pd.DataFrame]:
        """Loads the quarterly GDP vintages (revision triangles) data from the ONS website.
        
        Returns:
            A dictionary containing the dataframes for each sheet in the downloaded release.
        
        Raises:
            FileNotFoundError: If the expected file is missing in the downloaded release.
            ConnectionError: If there is an issue with downloading the release or accessing the data page.
        """
        
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
                
                # # First check the zipfile has the expected file
                # expected_file = "ABMI - Quarterly GDP at Market Prices.xlsx"
                # if expected_file not in zipfile.ZipFile(io.BytesIO(response.content)).namelist():
                #     logger.error("[bold red blink]Failed to download the latest release due to missing file: " + expected_file, extra={"markup": True})
                #     raise FileNotFoundError
                
                # Load the data as a dictionary to hold each sheet within a df
                data = {}
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    for filename in z.namelist():
                        with z.open(filename) as f:
                            data[filename] = pd.read_excel(f, sheet_name=None)
                
                # Return the data as a dictionary of dataframes
                return data

            else:
                logger.error("[bold red blink]Failed to download the latest release due to status code: " + str(response.status_code), extra={"markup": True})
                raise ConnectionError
        else:
            logger.error("[bold red blink]Failed to access the data page due to status code: " + str(response.status_code), extra={"markup": True})
            raise ConnectionError
        
    def _save(self, data: Dict[str, pd.DataFrame]) -> None:
            """
            Saves the data to the specified filepath.

            Args:
                data (Dict[str, pd.DataFrame]): A dictionary containing the data to be saved.
                    The keys represent the filenames, and the values represent the corresponding
                    pandas DataFrame objects.

            Returns:
                None
            """

            for filename, df in data.items():
                df.to_excel(self._folderpath / filename)

            self._extracted_files = data.namelist()  # Log the list of extracted files
            logger.info("Successfully saved the latest QGDP release data from the ONS website.")

    def _describe(self) -> Dict[str, Any]:
            """Returns a dictionary that describes the attributes of the dataset.

            Returns:
                A dictionary containing the following attributes:
                - folderpath: The folder path of the dataset.
                - release_date: The release date of the dataset.
                - extracted_files: The list of extracted files from the dataset.
            """
            return dict(folderpath=self._folderpath, release_date=self._release_date, extracted_files=self._extracted_files)
def get_latest_data(base_url):
    """
    Retrieves the latest data from a given base URL by iterating back in months and testing URLs.

    Args:
        base_url (str): The base URL to construct the test URLs.

    Returns:
        requests.Response: The response object containing the data if a working URL is found.

    Raises:
        ValueError: If no working URL is found.
    """
    # Get the current date
    current_date = pd.Timestamp.now()

    # Adjust the current date to the midpoint of the current month. Might avoid some datetime weirdness
    current_date = current_date.replace(day=15)

    # Iterate back in months from the current date
    for i in range(0, 12):
        time.sleep(0.3)
        # Calculate the date to test
        test_date = current_date - pd.DateOffset(months=i)

        # Construct the URL to test
        test_url = f"{base_url}{test_date.strftime('%b%y').lower()}.zip"

        # Send a request to the URL
        response = boerequests.get(test_url)

        # Check if the response is successful
        if response.status_code == 200:
            return response

    raise ValueError("No working URL found.")