# Udacity Data Engineering Capstone Project

# US IMMIGRATION DATA MODEL
### Data Engineering Capstone Project

#### Project Summary
This is the Uacity Data Engineering Capstone Project, which presents a usecase where both big data engineering concepts and technologies are utilized to bring useful insights out of large and diverse data sources. In this project, a data model is built to enable analysts and business users to answer different types of questions related to the immigration trends to the US.

Questions like *is there a peak season for immigration?* *Do immigrants from warm countries prefer certain States versus those who are coming from cold countries?* *What is the percentage of immigrants with business visas versus those with tourist visas?* All these questions and many more can be easily answered once our data model is built and filled with data.

The project follows these steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

Our data comes from different sources and in different formats as described in the following:

* I94 Immigration Data: This data comes from the US National Tourism and Trade Office. The data comes in .sas format, and it has information about entries made to the US in 2016. This dataset is relatively big as it contains several million records. The data also comes with labels descriptions file which provides additional information about the main dataset. More about this dataset can be found [here](https://render.githubusercontent.com/view/trade.gov/national-travel-and-tourism-office).
* World Temperature Data: This dataset came from Kaggle, and it keeps track of the global weather information. The data is provided as a .csv file. More about this dataset can be found [here](https://render.githubusercontent.com/view/kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
* U.S. City Demographic Data: This data is offered by OpenSoft, and it provides basic information about different city demographics. The data is also provided in .csv format. You can read more about it [here](https://render.githubusercontent.com/view/public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
* Airport Code Table: This is a simple table of airport codes and corresponding cities. The data is also provided in .csv format. It comes from [here](https://render.githubusercontent.com/view/datahub.io/core/airport-codes#data).

Star schema and pyspark have been used for the implementation of our data model. The implementation details can be found in the [Capstone Project Template.ipynb](https://github.com/qusay-elewy/udacity_data_engineering_capstone_project/blob/main/Capstone%20Project%20Template.ipynb) file.

The code in the Jupiter note is organized in a logical order where each code block represents a task or a number of related tasks. The ETL code is divided into a set of  functions in the [etl.py](https://github.com/qusay-elewy/udacity_data_engineering_capstone_project/blob/main/etl.py) file. Necessary imports and configurations are done at the begining of the code; no further imports or installs are needed.

To execute this ETL, user needs to enter *"python3 etl.py"* in Terminal, where *etl.py* is the name of our ETL code file.

Some data quality checks are performed after processing and saving data. Processed data are saved in .parquet format in a separate folder in this project, namely *output*. Some partitioning is applied to the saved data to achieve better performance during the data read operations.

The ETL has been tested and completed successfully before submission.
