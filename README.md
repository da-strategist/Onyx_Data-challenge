
Project Title: Rising Complaints Among Customers

Dataset Source
Onyx Data October 2025 Data Challenge — United States Consumer Financial Protection Bureau (CFPB)

Project Description

This is a simple analytics project that focuses on identifying factors influencing the rising number of customer complaints. It does not involve complex data engineering tasks such as large-scale extractions or routine data updates.

Core Tools:
* Python: Used for data extraction, transformation, loading, small-scale analytics, and correlational analysis to identify potential drivers of the issue.
* Power BI: Used for interactive data visualization and dashboard creation.

The analysis is divided into two parts:
1. Descriptive Analysis – Summarizes and explores the dataset.
2. Inferential Analysis – Examines correlations and relationships to draw deeper insights.
(Further details can be found in the Business Case document.)

Environment Setup
The environment for this project was set up using the uv package installer. Follow the steps below to replicate the setup:
1. Determine your preferred folder structure for organizing project files.
2. Open the project folder in VS Code.
3. In the terminal, initialize the project: uv init
4. Create a virtual environment:
    uv venv
5. Activate the virtual environment:
    source .venv/bin/activate
6. Add project dependencies:
    uv add numpy pandas matplotlib plotly azure-identity azure-storage-blob openpyxl

Dependencies:
* pandas
* numpy
* matplotlib
* plotly
* azure-identity
* azure-storage-blob
* openpyxl

Repository
Alternatively, you can clone the repository directly using:
git clone https://github.com/da-strategist/Onyx_Data-challenge.git
          