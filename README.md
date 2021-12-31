# Capstone Project

## Summary 
This project will load in data from the SAS files containing I94 immigration data, temperatures by city data, demographic data, and airport data. Other data used in this project is the information from the i94 description file. Each of these datasets are preprocessed and cleaned in order to be loaded into S3. Once the data is loaded into S3 the data is then copied into Redshift in staging tables. Once the staging tables are loaded into Redshift, the data is then extracted and transformed to create the fact and dimension tables. The data ends in the Redshift database to be used and accessed for further analysis. Besides Pyspark, S3, and Redshift, other tools used in this project are Jupyter Notebooks and AWS Console.

## Datasets
The main dataset will include data on immigration to the United States, and supplementary datasets will include data on airport codes, U.S. city demographics, and temperature data. The data sets include the following:


<h4>I94 Immigration Data</h4><br>
    This data comes from the US National Tourism and Trade Office. This data is stored as a set of SAS7BDAT files. SAS7BDAT is a database storage file created by Statistical Analysis System (SAS) software to store data. A separate SAS file `I94_SAS_Labels_Descriptions.SAS` is also provided to describe the data found in the source data. This description also provides the port and country codes mapped to the decoded strings. This data is the source of most of the data in the data model and represents 12 months of data for the year 2016. <br>
This data is initially processed using spark.

<h4>World Temperature Data</h4><br>
    This CSV dataset contains the recorded temperatures by city from 1743-11-01 to 2013-09-01. In order to select temperatures that were relevant to our dataset, I chose the most recent recording of each city by sorting and dropping duplicates.
    
<h4>U.S. City Demographic Data</h4><br>
    This CSV data contains information about the demographics of the U.S. This data will support the port data to gather more information about the demographic at each port
    
<h4>Airport Code Table</h4><br>
    This is a simple CSV table of airport codes and corresponding cities

## Data Model
View the data model at `capstone.pdf`

## Pipeline
The necessary steps for the pipeline are performed in `pipeline.py`
<br><br>
The steps performed are: <br>
Preprocess data --> Stage data --> Extract data --> Check Data Quality
    <ol type = "1">
         <li>Preprocess Data `preprocessing.py` : Data is processed from the raw data using Spark and then written to S3 </li>
         <li>Stage Data `stage_data.py` : Data is copied from S3 into Redshift staging tables</li>
         <li>Extract Data `extract_data.py` : Data is extracted from staging tables into fact and dimesion tables</li>
         <li>Check Data Quality `quality_check.py` : Data quality is checked after the extracted data is organized</li>
    </ol>
## Running the pipeline
To run the pipeline, run the following command in the terminal
```python pipeline.py```

For more information about the data, how the data is cleaned and transformed, or quality checks, reference the `Capstone Project Template.ipynb` notebook