"""Nodes are the building blocks of pipelines, and represent tasks. 
Pipelines are used to combine nodes to build workflows, which range from simple data engineering workflows 
to end-to-end production workflows.
"""
## Steps:
# 1. Load the data
# 2. Clean the data
# 3. Transform the data
# 4. Save the data

import logging
from typing import List
import pandas as pd
import warnings

log = logging.getLogger(__name__)

def load_data(
    data_dict: dict, 
    list_of_filenames_to_load: List[str]
) -> List[pd.DataFrame]:
    """
    Load the quarterly data from the given data dictionary and return the dataframe.
    
    Parameters:
        data_dict (dict): A dictionary containing the data to be loaded.
        
    Returns:
        pd.DataFrame: The loaded data as a pandas DataFrame.
    """
    log.info("Loading the data...")
    df_holder = []
    
    for filename, dict_of_sheets in data_dict.items(): # For file extracted from the zip
        for file_to_load in list_of_filenames_to_load: # For the list of files to load in config
            if file_to_load.lower() in filename.lower(): # If the names match
                for sheet_name, df in dict_of_sheets.items(): # For the sheets in the file
                    if "triangle" in sheet_name.lower(): # If the sheet name contains "triangle"
                        df_holder.append(df) # Append to a list of returns
                    elif sheet_name.lower() == "estimate": # If the sheet name is "estimate"
                        df_holder.append(df) # Append to a list of returns
    
    return df_holder


def clean_quarterly_data(data_list: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Cleans the given GDP vintages dataset by performing the following steps:
    1. Drops unnecessary rows.
    2. Replaces the index with the first column.
    3. Drops the first column.
    4. Sets the first row as the column names.
    5. Drops the first row.
    6. Rotates the table.
    7. Replaces empty data with NaNs.

    Parameters:
    - gdp_vintages_ons (pandas.DataFrame): The Q-GDP vintages dataset to be cleaned.

    Returns:
    - pandas.DataFrame: The cleaned Q-GDP vintages dataset.
    """
    log.info("Cleaning the data...")
    
    clean_data_list = []
    
    for data in data_list:
    
        # Drop some unnecessary rows
        data = data.drop(
            data.index[[0,1,3,4,5,-1]]
        )
        # Replace the index with the first column
        data.index = data.iloc[:, 0]
        
        # Drop the first column now it's been moved
        data = data.drop(data.columns[0], axis=1)
        
        # Make the first row into the names of the columns
        data.columns = data.iloc[0]
        
        # Drop the first row
        data = data.drop(data.index[0])

        # Rotate the table
        data = data.T
        
        # Replace empty data with NaNs
        pd.set_option('future.no_silent_downcasting', True) # Silly warning
        data = data.replace(' ', pd.NA)
        
        # Turn the index into a datetime index which excel can read
        data.index = data.index.map(lambda x: x.replace('Q1', '01').replace('Q2', '04').replace('Q3', '07').replace('Q4', '10'))
       
        with warnings.catch_warnings():  # Suppress the warning about not specifying a format
            warnings.filterwarnings("ignore", category=UserWarning)
            data.index = pd.to_datetime(data.index).to_period('Q') 
        
        
        clean_data_list.append(data)
    
    return clean_data_list


def transform_and_combine(data_list: List[pd.DataFrame]) -> dict:
    """
    Transforms the input DataFrame by constructing revision series for different quarters
    and combines them into a new DataFrame along with the original data.

    Args:
        data (pd.DataFrame): The input DataFrame containing the revisions triangle.

    Returns:
        pd.DataFrame: The combined DataFrame containing the revisions triangle and the revision series.
    """
    t_data_list = []
    log.info("Transforming the data...")
    
    for data in data_list:
            
        transformed_revisions = pd.DataFrame(index=data.index)
        
        transformed_revisions["First Estimate"] = construct_revision_series(data, 0).values
        transformed_revisions["1st Period"] = construct_revision_series(data, 1).values
        transformed_revisions["2nd Period"] = construct_revision_series(data, 2).values
        transformed_revisions["3rd Period"] = construct_revision_series(data, 3).values
        transformed_revisions["4th Period"] = construct_revision_series(data, 4).values
        transformed_revisions["12th Period"] = construct_revision_series(data, 12).values
        transformed_revisions["36th Period"] = construct_revision_series(data, 36).values
        
        total_gdp_vintage = {
            'Revisions triangle': data,
            'Revisions series': transformed_revisions
        }
        
        t_data_list.append(total_gdp_vintage)
        
        
    return t_data_list


def save_data(data_list: List[dict], input_name_list: List[str], save_path: str) -> None:
    """Save the data to the specified filepath.
    
    Args:
        data (pd.DataFrame): The DataFrame containing the data to be saved.
    
    Returns:
        None
    """
    log.info("Saving the data...")
    
    for data, name in zip(data_list, input_name_list):
        with pd.ExcelWriter(f"{save_path}{name}_PROCESSED.xlsx") as writer:
            for sheet_name, df in data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=True)
            
    

def construct_revision_series(revisions_triangle: pd.DataFrame, periods: int) -> pd.Series:
    """
    Construct the revision series from the revisions triangle, given the specified period.
    Convenience function used in the transform_and_merge node.
    
    Parameters:
        revisions_triangle (pd.DataFrame): The revisions triangle containing the estimates over time.
        periods (int): The number of periods to consider for the revision series.
        
    Returns:
        pd.Series: The revision series calculated based on the specified period.
        
    If the passed period = 0, then the first estimate series is returned.
    """
    revision_series = pd.Series(index=revisions_triangle.index, name=f"{periods}_period_revision_series")
    
    if periods == 0:
        # Assume the user wants the first estimate series
        for idx in revisions_triangle.index:
            # Get the first estimate
            try:
                first_estimate = revisions_triangle.loc[idx].dropna().iloc[0]
            except IndexError:
                first_estimate = pd.NA   
                
            revision_series.loc[idx] = first_estimate         
    else:
        # Calculate the revisions series
        for idx in revisions_triangle.index:
            
            # Get the first estimate            
            # Get the estimate relevant to the period
            try:
                first_estimate = revisions_triangle.loc[idx].dropna().iloc[0]
                final_estimate = revisions_triangle.loc[idx].dropna().iloc[periods]
            except IndexError:
                first_estimate = pd.NA
                final_estimate = pd.NA

            # Calculate the revision series
            if final_estimate is pd.NA:
                revision_series.loc[idx] = pd.NA
            else:
                revision_series.loc[idx] = round(final_estimate - first_estimate, 3)

    return revision_series