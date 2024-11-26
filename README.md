# Accident Severity Prediction Using Big Data and Machine Learning

This project leverages big data tools and machine learning to predict accident severity using traffic and accident data from the TFL (Transport for London) API.

## Project Architecture

The project follows a structured architecture involving multiple stages:

1. **Data Sources**:
   - Data is fetched from the TFL API, providing traffic and accident data.

2. **Data Ingestion**:
   - REST API calls are made to collect data.
   - The raw data is stored in **Google Cloud Storage (GCS)**.

3. **Data Processing**:
   - **Google Cloud Dataproc** with PySpark is used for data cleaning, transformation, and enrichment.
   - Preprocessing includes handling missing values, feature engineering, and preparing the dataset for machine learning.

4. **Data Storage**:
   - Cleaned and processed data is stored in **Google BigQuery** for querying and analysis.

5. **Machine Learning**:
   - A **Random Forest Classifier** is trained in PySpark on Dataproc to predict accident severity.
   - Features include:
     - Date-time extraction (year, month, day of the week, hour).
     - Categorical feature indexing (vehicle type, borough).
     - Vector assembly for ML modeling.
   - The trained model is saved back to **GCS**.

6. **Output**:
   - The model is evaluated for accuracy and provides predictions for accident severity.
