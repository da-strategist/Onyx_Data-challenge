import os
import numpy as np
import pandas as pd
import openpyxl
import datetime


data = './October_challenge/Fin_consumer.xlsx'

fact_data = pd.read_excel(data, sheet_name= 'Complaints (Fact)', header= 0)
#print(fact_data.head())

dim_data = pd.read_excel(data, sheet_name= 'Company', header= 0)

#print(fact_data.head())
#print(dim_data.head())


#here we start transafroming out datasets


""""
We shall start with our fact table
"""

print(fact_data.describe())
print(fact_data.info())

#print(fact_data.columns)
#print(dim_data.columns)


#print(fact_data['Date submitted'].head())

#correcting date columns

fact_data['Date submitted'] = pd.to_datetime(fact_data['Date submitted'], origin= '1899-12-30', unit= 'D')
fact_data['Date received'] = pd.to_datetime(fact_data['Date received'], origin= '1899-12-30', unit= 'D')
fact_data['Company_Response_Date'] = pd.to_datetime(fact_data['Company_Response_Date'], origin= '1899-12-30', unit= 'D')


print(fact_data.head())





