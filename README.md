# Udacity Data Engineering Capstone Project

# US IMMIGRATION DATA MODEL
### Data Engineering Capstone Project

#### Project Summary
This is the Uacity Data Engineering Capstone Project, which presents a usecase where both big data engineering concepts and technologies are utilized to bring useful insights out of large and diverse data sources. In this project, a data model is built to enable analysts and business users to answer different types of questions related to the immigration trends to the US.

Questions like *is there a peak season for immigration?* *Do immigrants from warm countries prefer certain States versus those who are coming from cold countries?* *What is the percentage of immigrants with business visas versus those with tourist visas?* All these questions and many more can be easily answered once our data model is built and filled with data.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

Star schema and pyspark have been used for the implementation of our data model. Implementation details can be found in the Capstone Project Template.ipynb file.

The code in the Jupiter note is organized in a logical order where each code block represents a task or a number of related tasks. The ETL code is divided into a set of logical functions in the etl.py file.

Necessary imports and configurations are done at the begining of the code. No further imports or installs are needed.

Implementation code is divided into logical functions which can be found in etl.py file. To execute this ETL, you can run it in Terminal by executing "python3 etl.py".

Some data quality checks are performed after processing and saving data. Processed data are saved in .parquet format in a separate folder in this project, namely *output*. Some partitioning is applied to the saved data to achieve better performance during the data read operations.

The ETL has been tested and completed successfully before submission.
