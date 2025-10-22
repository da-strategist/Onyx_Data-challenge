import pandas as pd
import numpy as np
import matplotlib as plt
import plotly
from data_extract import load_fact_data, load_dim_data

# Load data only when needed
fact_data = load_fact_data()
dim_data = load_dim_data()



#we shall start our eda by addressing the first objective

"Identify complaint patterns across financial products, services, companies, and geographic regions."
""
"Questions to be answered"
""
"1. Total complaints recieved"
"2. Complaint distribution across channels, most frequently used channels"
"3. Complaint across companies, size, category etc"
"4. Complaints across regions, identifying hotspots"
"5. Complaints across products and sub products, identifying most common issues, issue prone products"
"6. Response rate, overall and distribution across companies. most responsive and quickest in terms of issue resolution"
"7a. Trends, yearly, monthly, weekly and daily distribution of complaints, peak complaints periods and low complaint periods"
"7b Change in complaints over time i.e progress"
"8. correlations,"
""
"   is there any correlation between timely resolution of complaints and company size or category "

""
""

#shall start attending to the questions above

# Total complaints

cus_comp_fact_data = fact_data
cus_comp_dim_data = dim_data


#print(cus_comp_fact_data.head())
#print(cus_comp_dim_data.head())

#total complaint

tot_complaint_recieved = cus_comp_fact_data['Complaint_ID'].count()
print(f"total complaint is: {tot_complaint_recieved}")