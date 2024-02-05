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
import pandas as pd

log = logging.getLogger(__name__)

def load_quarterly_data(data_dict: dict) -> pd.DataFrame:
    """
    Load the quarterly data from the given data dictionary and return the dataframe.
    
    Parameters:
        data_dict (dict): A dictionary containing the data to be loaded.
        
    Returns:
        pd.DataFrame: The loaded data as a pandas DataFrame.
    """
    log.info("Loading the data...")
    
    for filename, dict_of_sheets in data_dict.items():
        if "ABMI - Quarterly GDP at Market Prices" in filename:
            for sheet_name, df in dict_of_sheets.items():
                if "triangle" in sheet_name.lower():
                    return df

def save_raw_data(data: pd.DataFrame, name: str) -> None:
    """Save the data to the specified filepath.
    
    Args:
        data (pd.DataFrame): The DataFrame containing the data to be saved.
    
    Returns:
        None
    """
    log.info("Saving the raw data...")
    
    with pd.ExcelWriter(f"data/01_raw/{name}") as writer:
        data.to_excel(writer, sheet_name="Raw data", index=True)
        

def clean_quarterly_data(data: pd.DataFrame) -> pd.DataFrame:
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
    data = data.replace(' ', pd.NA).infer_objects(copy=False)
    
    # Turn the index into a datetime index which excel can read
    data.index = data.index.map(lambda x: x.replace('Q1', '01').replace('Q2', '04').replace('Q3', '07').replace('Q4', '10'))
    data.index = pd.to_datetime(data.index).to_period('Q')
    
    return data


def transform_and_combine(data: pd.DataFrame) -> dict:
    """
    Transforms the input DataFrame by constructing revision series for different quarters
    and combines them into a new DataFrame along with the original data.

    Args:
        data (pd.DataFrame): The input DataFrame containing the revisions triangle.

    Returns:
        pd.DataFrame: The combined DataFrame containing the revisions triangle and the revision series.
    """
    log.info("Transforming the data...")
    transformed_QGDP_revisions = pd.DataFrame(index=data.index)
    
    transformed_QGDP_revisions["First Estimate"] = construct_revision_series(data, 0).values
    transformed_QGDP_revisions["1st Period"] = construct_revision_series(data, 1).values
    transformed_QGDP_revisions["2nd Period"] = construct_revision_series(data, 2).values
    transformed_QGDP_revisions["3rd Period"] = construct_revision_series(data, 3).values
    transformed_QGDP_revisions["4th Period"] = construct_revision_series(data, 4).values
    transformed_QGDP_revisions["12th Period"] = construct_revision_series(data, 12).values
    transformed_QGDP_revisions["36th Period"] = construct_revision_series(data, 36).values
    
    total_gdp_vintage = {
        'Revisions triangle': data,
        'Revisions series': transformed_QGDP_revisions
    }
    
    return total_gdp_vintage


def save_data(data: dict, name: str) -> None:
    """Save the data to the specified filepath.
    
    Args:
        data (pd.DataFrame): The DataFrame containing the data to be saved.
    
    Returns:
        None
    """
    log.info("Saving the data...")
    
    with pd.ExcelWriter(f"data/02_intermediate/{name}") as writer:
        for sheet_name, df in data.items():
            df.to_excel(writer, sheet_name=sheet_name, index=True)
            
    

def construct_revision_series(revisions_triange: pd.DataFrame, periods: int) -> pd.Series:
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
    revision_series = pd.Series(index=revisions_triange.index, name=f"{periods}_period_revision_series")
    
    if periods == 0:
        # Assume the user wants the first estimate series
        for idx in revisions_triange.index:
            # Get the first estimate
            first_estimate = revisions_triange.loc[idx].dropna().iloc[0]
            revision_series.loc[idx] = first_estimate
            
    else:
        # Calculate the revisions series
        for idx in revisions_triange.index:
            # Get the first estimate
            first_estimate = revisions_triange.loc[idx].dropna().iloc[0]
            
            # Get the estimate relevant to the period
            try:
                final_estimate = revisions_triange.loc[idx].dropna().iloc[periods]
            except IndexError:
                final_estimate = pd.NA

            # Calculate the revision series
            if final_estimate is pd.NA:
                revision_series.loc[idx] = pd.NA
            else:
                revision_series.loc[idx] = round(final_estimate - first_estimate, 3)

    return revision_series