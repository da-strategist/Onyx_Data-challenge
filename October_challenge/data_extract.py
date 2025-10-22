"""
This module handles the ETL (Extract, Transform, Load) process for customer complaints data.
It includes functions for loading data from Excel files, transforming the data for analysis,
and uploading the processed data to Azure Blob Storage.
"""

import os
import numpy as np
import pandas as pd
import openpyxl
import datetime
from azure.storage.blob import BlobServiceClient
import constants
from io import BytesIO

# Path to the source Excel file containing both fact and dimension tables
data = './October_challenge/Fin_consumer.xlsx'

def load_fact_data():
    """
    Load the complaints fact table from Excel file.
    
    Returns:
        pandas.DataFrame: Raw complaints data with customer complaints information
    """
    fact_data = pd.read_excel(data, sheet_name= 'Complaints (Fact)', header= 0)
    return fact_data

def load_dim_data():
    """
    Load the company dimension table from Excel file.
    
    Returns:
        pandas.DataFrame: Raw company data with company details
    """
    dim_data = pd.read_excel(data, sheet_name= 'Company', header= 0)
    return dim_data

def transform_fact_data(fact_data):
    """
    Transform the complaints fact table by standardizing dates and column names.
    
    Args:
        fact_data (pandas.DataFrame): Raw complaints data
    
    Returns:
        pandas.DataFrame: Transformed complaints data with standardized column names and date formats
    """
    # Convert Excel dates to datetime objects
    # Using 1899-12-30 as origin since Excel dates are counted from 1900-01-01
    fact_data['Date submitted'] = pd.to_datetime(fact_data['Date submitted'], origin= '1899-12-30', unit= 'D')
    fact_data['Date received'] = pd.to_datetime(fact_data['Date received'], origin= '1899-12-30', unit= 'D')
    fact_data['Company_Response_Date'] = pd.to_datetime(fact_data['Company_Response_Date'], origin= '1899-12-30', unit= 'D')

    # Rename columns to standardized format:
    # - Remove spaces and special characters
    # - Use underscores for word separation
    # - Shorten long names where appropriate
    fact_data.rename(columns= { 'Complaint ID' : 'Complaint_ID',
                               'Submitted via ': 'Channel',
                               'Date submitted': 'Date_submitted',
                               'Date received': 'Date_received',
                               'State_Longitude': 'Longitude',
                               'State_Latitude ': 'Latitude',
                               'Company public response': 'Comp_pub_res',
                               'Company response to consumer': 'Res_to_customer',
                               'Timely response?': 'Timely_response',
                               'Census_Region': 'Region',
                               'Census_Division': 'Division',
                               'Company_Response_Date': 'Response_date',
                               'Company_ID_1081': 'Company_ID'
    },  inplace= True)
    return fact_data

def transform_dim_data(dim_data):
    """
    Transform the company dimension table by standardizing column names.
    
    Args:
        dim_data (pandas.DataFrame): Raw company data
    
    Returns:
        pandas.DataFrame: Transformed company data with standardized column names
    """
    # Standardize the Company ID column name to match the fact table
    dim_data.rename(columns= {'Company_ID_1081': 'Company_ID'}, inplace= True)
    return dim_data

def upload_to_azure(fact_data, dim_data):
    """
    Upload transformed fact and dimension tables to Azure Blob Storage.
    
    Args:
        fact_data (pandas.DataFrame): Transformed complaints data
        dim_data (pandas.DataFrame): Transformed company data
    """
    # Get connection string from constants file for security
    connection_string = constants.connection_string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Prepare data for upload: (DataFrame, blob_name) pairs
    prep_data = [(fact_data, 'cus_comp_fact'),
                 (dim_data, 'cus_comp_dim')]

    # Upload each DataFrame to Azure Blob Storage
    for df, blob_name in prep_data:
        # Convert DataFrame to CSV in memory
        buffer = BytesIO()
        data = df.to_csv(buffer, index= False)
        buffer.seek(0)  # Reset buffer position to start

        # Create blob client and upload data
        blob_client = blob_service_client.get_blob_client(
            container= 'consumercompliant',
            blob= blob_name
        )

        blob_client.upload_blob(data= buffer, overwrite= True)
        print(f"{blob_name}: Data upload successful")

if __name__ == '__main__':
    """
    Main execution block - runs only if this script is executed directly.
    Demonstrates the complete ETL pipeline:
    1. Load raw data from Excel
    2. Transform data (standardize dates and column names)
    3. Upload transformed data to Azure
    4. Display sample data for verification
    """
    # Step 1: Load raw data from Excel files
    print("Loading data from Excel...")
    fact_data = load_fact_data()
    dim_data = load_dim_data()
    
    # Step 2: Transform the data
    print("\nTransforming data...")
    fact_data = transform_fact_data(fact_data)
    dim_data = transform_dim_data(dim_data)
    
    # Step 3: Upload transformed data to Azure
    print("\nUploading data to Azure...")
    upload_to_azure(fact_data, dim_data)
    
    # Step 4: Display sample data for verification
    print("\nVerifying data transformation:")
    print("Fact Data Sample (first 5 rows):")
    print(fact_data.head())
    print("\nDimension Data Sample (first 5 rows):")
    print(dim_data.head())


def transform_fact_data(fact_data):
    # Standardizing date columns
    fact_data['Date submitted'] = pd.to_datetime(fact_data['Date submitted'], origin= '1899-12-30', unit= 'D')
    fact_data['Date received'] = pd.to_datetime(fact_data['Date received'], origin= '1899-12-30', unit= 'D')
    fact_data['Company_Response_Date'] = pd.to_datetime(fact_data['Company_Response_Date'], origin= '1899-12-30', unit= 'D')

    # Standardizing column names
    fact_data.rename(columns= { 'Complaint ID' : 'Complaint_ID',
                               'Submitted via ': 'Channel',
                               'Date submitted': 'Date_submitted',
                               'Date received': 'Date_received',
                               'State_Longitude': 'Longitude',
                               'State_Latitude ': 'Latitude',
                               'Company public response': 'Comp_pub_res',
                               'Company response to consumer': 'Res_to_customer',
                               'Timely response?': 'Timely_response',
                               'Census_Region': 'Region',
                               'Census_Division': 'Division',
                               'Company_Response_Date': 'Response_date',
                               'Company_ID_1081': 'Company_ID'
    },  inplace= True)
    return fact_data

def transform_dim_data(dim_data):
    # Standardizing column names for dimension table
    dim_data.rename(columns= {'Company_ID_1081': 'Company_ID'}, inplace= True)
    return dim_data

#print(dim_data.head())
#print(dim_data.info())
#print(dim_data.describe())
#loading data to azure
def upload_to_azure(fact_data, dim_data):
    connection_string = constants.connection_string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    prep_data = [(fact_data, 'cus_comp_fact'),
                 (dim_data, 'cus_comp_dim')]

    for df, blob_name in prep_data:
        buffer = BytesIO()
        data = df.to_csv(buffer, index= False)
        buffer.seek(0)

        blob_client = blob_service_client.get_blob_client(
            container= 'consumercompliant',
            blob= blob_name
        )

        blob_client.upload_blob(data= buffer, overwrite= True)
    print(f"{blob_name}: Data upload successful")





