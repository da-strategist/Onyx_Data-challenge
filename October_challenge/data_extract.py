import os
import numpy as np
import pandas as pd
import openpyxl
import datetime
from azure.storage.blob import BlobServiceClient
import constants

from io import BytesIO


data = './October_challenge/Fin_consumer.xlsx'

fact_data = pd.read_excel(data, sheet_name= 'Complaints (Fact)', header= 0)
#print(fact_data.head())

dim_data = pd.read_excel(data, sheet_name= 'Company', header= 0)

#print(fact_data.head())
#print(dim_data.head())


#here we start transaforming our datasets


""""
We shall start with our fact table
"""

#print(fact_data.describe())
#print(fact_data.info())

#print(fact_data.columns)
#print(dim_data.columns)


#print(fact_data['Date submitted'].head())

#Standardizing date columns

fact_data['Date submitted'] = pd.to_datetime(fact_data['Date submitted'], origin= '1899-12-30', unit= 'D')
fact_data['Date received'] = pd.to_datetime(fact_data['Date received'], origin= '1899-12-30', unit= 'D')
fact_data['Company_Response_Date'] = pd.to_datetime(fact_data['Company_Response_Date'], origin= '1899-12-30', unit= 'D')


#standardizing column names

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

#now we move on to the dimension table


dim_data.rename(columns= {'Company_ID_1081': 'Company_ID'}, inplace= True)

#print(dim_data.head())
#print(dim_data.info())
#print(dim_data.describe())
#loading data to azure



connection_string = constants.connection_string

BlobServiceClient = BlobServiceClient.from_connection_string(connection_string)

prep_data = [(fact_data, 'cus_comp_fact'),
             (dim_data, 'cus_comp_dim')]


for df, blob_name in prep_data:
    buffer = BytesIO()
    data = df.to_csv(buffer, index= False)
    sss = buffer.seek(0)


    blob_client = BlobServiceClient.get_blob_client(
        container= 'consumercompliant',
        blob= blob_name
        )

    blob_client.upload_blob(data= buffer, overwrite= True)
    print(f"{blob_name}: Data upload successful")





