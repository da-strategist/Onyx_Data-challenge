import os
import numpy as np
import pandas as pd
import openpyxl


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

print(fact_data.columns)
print(dim_data.columns)
