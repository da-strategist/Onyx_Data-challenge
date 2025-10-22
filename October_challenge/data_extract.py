import os
import numpy as np
import pandas as pd
import openpyxl
import datetime
from azure.storage.blob import BlobServiceClient
import constants
from io import BytesIO

data = './October_challenge/Fin_consumer.xlsx'

def load_fact_data():
    fact_data = pd.read_excel(data, sheet_name= 'Complaints (Fact)', header= 0)
    return fact_data

def load_dim_data():
    dim_data = pd.read_excel(data, sheet_name= 'Company', header= 0)
    return dim_data

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

if __name__ == '__main__':
    # This code only runs if data_extract.py is run directly
    # Load data
    fact_data = load_fact_data()
    dim_data = load_dim_data()
    
    # Transform data
    fact_data = transform_fact_data(fact_data)
    dim_data = transform_dim_data(dim_data)
    
    # Upload to Azure (only if needed)
    upload_to_azure(fact_data, dim_data)
    
    # Print sample data for verification
    print("Fact Data Sample:")
    print(fact_data.head())
    print("\nDimension Data Sample:")
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





