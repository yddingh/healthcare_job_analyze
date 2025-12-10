# Healthcare Jobs Data Analysis Project

## Project Overview

This project uses a real-world job postings dataset from the eMedCareers job portal to simulate an end-to-end data engineering and data analytics workflow. The goal is to practice data cleaning, data modeling (using SQL/dbt), and BI dashboard creation.


## About the Dataset

The dataset was obtained from Kaggle: "30000+ healthcare jobs from eMedCareers (Europe)"

Dataset source: https://www.kaggle.com/datasets/jobspikr/30000-latest-healthcare-jobs-emedcareers-europe


The dataset consists of 30,000 job postings collected from eMedCareers, a specialized job search platform for pharmaceutical, biotechnology, and healthcare roles across Europe.

Data was scraped and delivered via JobsPikr, a machine-learning powered job data extraction platform. The original dataset includes the following fields:

- category – Job field or domain
- location – Country or region of posting
- company_name – Employer name
- job_title – Title of the job posting
- job_description – Full text description from the posting
- job_type – Employment type (e.g., Full-time / Temporary)
- salary_offered – Salary information (if provided)
- post_date – Date of job posting

Raw data file location in this project:
healthcare_job_analyze\healthcare_job_dbt\data\emed_careers_eu.csv

## Details

1. Data Cleansing (Python)

I used Python to perform the initial data cleansing steps:

- Removed duplicate and empty entries
- Normalized text fields
- Standardized job types
- Extracted structured fields from unstructured descriptions (e.g., keyword counts)
- Parsed posting dates into proper datetime format

After cleansing, the transformed dataset was exported into a database file to be used for downstream modeling.

Python scripts:
healthcare_job_analyze\healthcare_job_dbt\script\data_cleansing.ipynb
healthcare_job_analyze\healthcare_job_dbt\script\csv_to_sqlite.py

2. Data Modeling (SQL & dbt)

To simulate a standard data engineering workflow, the cleaned dataset was loaded into a dbt project and modeled into:

    Fact Table
    ├── fact_jobs
    │
    Dimension Tables
    ├── dim_company
    ├── dim_category
    ├── dim_job_type
    └── dim_salary


This follows typical star schema principles, making the data easier to analyze and scale.

Model files location:
\healthcare_job_analyze\healthcare_job_dbt\models

3. BI Dashboard & Analysis(Tableau)

Finally, I designed several BI dashboards usaing Tableau to visualize insights.

##

These dashboards summarize the dataset effectively and demonstrate an end-to-end workflow from raw data → cleaned data → modeled data → insights.


