"""Nodes are the building blocks of pipelines, and represent tasks. 
Pipelines are used to combine nodes to build workflows, which range from simple data engineering workflows 
to end-to-end production workflows.
"""

import logging
import pandas as pd
import re

from revisions_toolkit.pipelines.headline_Qgdp.nodes import construct_revision_series

log = logging.getLogger(__name__)


def load_monthly_data(data_dict: dict) -> pd.DataFrame:
    """
    Load the monthly data from the given data dictionary and return the dataframe.
    
    Parameters:
        data_dict (dict): A dictionary containing the data to be loaded.
        
    Returns:
        pd.DataFrame: The loaded data as a pandas DataFrame.
    """
    log.info("Loading the data...")
    
    xl_name = "mgdp revision triangle (m on m).xlsx"
    
    for filename, dict_of_sheets in data_dict.items():
        if xl_name in filename.lower():
            for sheet_name, df in dict_of_sheets.items():
                if "estimate" in sheet_name.lower():
                    return df
        
    log.error(f"No sheet matching {sheet_name} in the given data dictionary.")
    raise ValueError(f"No sheet matching {sheet_name} in the given data dictionary.")
                

def clean_monthly_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the monthly data by performing the following steps:
    1. Set the index of the DataFrame to the first column.
    2. Drop the first two rows.
    3. Drop the first column.
    4. Set the column names to the values in the first row.
    5. Drop the first row.
    6. Drop the second row.
    7. Transpose the DataFrame.
    8. Drop the last column.
    9. Clean the index by removing non-alphanumeric characters and converting it to datetime format.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the monthly data.
        
    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    df.index = df.iloc[:, 0]
    df = df.drop(index=df.index[0:2])
    df = df.drop(columns=[df.columns[0]])
    df.columns = df.iloc[0, :]
    df = df.drop(index=df.index[0])
    df = df.drop(index=df.index[0])
    df = df.T
    df = df.drop(columns=[df.columns[-1]])
    
    clean_index = df.index.map(lambda x: ' '.join(re.findall(r'[A-Za-z]+|\d+', x)))
    df.index = pd.to_datetime(clean_index, format='%Y %b')
    
    return df