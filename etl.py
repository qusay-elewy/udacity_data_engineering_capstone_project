import configparser
import pandas as pd
import os
import psycopg2
import datetime as dt
from pyspark.sql import SparkSession
from datetime import timedelta, datetime
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, month, year, dayofweek, dayofmonth, weekofyear, when, col, \
                                    split, trim, upper, row_number, monotonically_increasing_id             
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType, FloatType, DecimalType, DateType


#Reading configuration settings
configParser = configparser.RawConfigParser()
configParser.read("config.cfg")
jave_home = configParser.get("OS", "JAVA_HOME")
path = configParser.get("OS", "PATH")
spark_home = configParser.get("OS", "SPARK_HOME")
hadoop_home = configParser.get("OS", "HADOOP_HOME")
spark_memory = configParser.get("Spark", "Memory")
spark_broadcast_timeout = configParser.get("Spark", "Broadcast_Timeout")

#Setting environment settings
os.environ["JAVA_HOME"] = jave_home
os.environ["PATH"] = path
os.environ["SPARK_HOME"] = spark_home
os.environ["HADOOP_HOME"] = hadoop_home
#Increasing the memory usage on the drive to to 15GB to avoid running out of memory
spark = SparkSession.builder.config("spark.driver.memory", spark_memory)\
                            .config("spark.sql.broadcastTimeout", spark_broadcast_timeout)\
                            .getOrCreate()


"""
Converts SAS dates to normal dates
"""
convert_date = udf(lambda x : x if x is None else (datetime(1960, 1, 1).date() + timedelta(x)).isoformat())


with open('./I94_SAS_Labels_Descriptions.SAS') as f:
    f_content = f.read()
    f_content = f_content.replace('\t', '')


def code_mapper(file, idx):
    """
    Extracts Labels Descriptions from SAS file
    """
    
    try:
        f_content2 = f_content[f_content.index(idx):]
        f_content2 = f_content2[:f_content2.index(';')].split('\n')
        f_content2 = [i.replace("'", "") for i in f_content2]
        dic = [i.split('=') for i in f_content2[1:]]
        dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
        return dic
    except Exception as e:
        print(f"Error while processing SAS Labels Descriptions file. {e}")


@udf(StringType())
def get_season(x):
    """
    Calculates the weather season of the passed month
    """
    
    try:
        if (x == 12 or x == 1 or x == 2):
            return "Winter"
        elif (x == 3 or x == 4 or x == 5):
            return "Spring"
        elif (x == 6 or x == 7 or x == 8):
            return "Summer"
        else:
            return "Autumn"
    except:
        return None        
        

def create_spark_session():
    """
    Creates a spark session
    """
    
    #Increases memory usage on the drive to 15GB to avoid running out of memory, and
    #session timeout to an hour to avoid timeouts
    spark = SparkSession.builder.config("spark.driver.memory", "15g")\
                                .config("spark.sql.broadcastTimeout", "36000")\
                                .getOrCreate()
    return spark


def process_i94_data(spark, data_file):
    """
    Generates fact_I94 table
    """
    
    try:
        print("\nProcessing fact_I94 data...")
        i94_df = spark.read.load(data_file)
        i94_df = i94_df.drop("visapost", "occup", "entdepu", "insnum")\
                       .drop("count", "entdepd", "entdepa", "entdepu", "matflag", "dtaddto", "biryear", "admnum")
        i94_df = i94_df.na.drop(subset=["airline", "gender", "i94addr"])
        i94_df = i94_df.dropna(how="all")
    
        #Converts SAS dates to normal dates
        convert_date = udf(lambda x : x if x is None else (datetime(1960, 1, 1).date() + timedelta(x)).isoformat())

        #Calculate stay length (in days)
        stay = F.datediff(F.to_date(convert_date(i94_df.depdate)), F.to_date(convert_date(i94_df.arrdate)))
        i94_df = i94_df.withColumn("stay", stay)
        i94_df = i94_df.withColumn("cicid", i94_df.cicid.cast(IntegerType()))\
                        .withColumn("i94yr", i94_df.i94yr.cast(IntegerType()))\
                        .withColumn("i94mon", i94_df.i94mon.cast(IntegerType()))\
                        .withColumn("i94cit", i94_df.i94cit.cast(IntegerType()))\
                        .withColumn("i94res", i94_df.i94res.cast(IntegerType()))\
                        .withColumn("i94mode", i94_df.i94mode.cast(IntegerType()))\
                        .withColumn("i94bir", i94_df.i94bir.cast(IntegerType()))\
                        .withColumn("i94visa", i94_df.i94visa.cast(IntegerType()))\
                        .withColumn("arrdate", i94_df.arrdate.cast(IntegerType()))\
                        .withColumn("depdate", i94_df.depdate.cast(IntegerType()))
        print("----- Generating fact_I94 sample data -----")
        i94_df.show(10)
    except Exception as e:
        print("Error while processing fact_I94 data. {e}\n")
        return
    try:
        print("\nSaving fact_I94 data...")
        i94_df.write.mode("overwrite").parquet("output/fact_i94.parquet")
        print(f"fact_I94 table saved successfully.\n")
        return i94_df
    except Exception as e:
        print(f"Error occured while saving fact_I94 table. {e} \n")
    
    
def process_demographics_data(spark, data_file, i94addr_df):
    """
    Generates dim_Demographics table
    """
    
    try:
        print("\nProcesing dim_Demographics data...")
        demos_df = spark.read.option("header", "true").option("delimiter", ";").csv(data_file)
        
        #Rename the columns into a format that's easier to deal with while coding
        demos_df = demos_df.withColumnRenamed("Median Age", "median_age")\
                            .withColumnRenamed("Male Population", "male_population")\
                            .withColumnRenamed("Female Population", "female_population")\
                            .withColumnRenamed("Total Population", "total_population")\
                            .withColumnRenamed("City", "city")\
                            .withColumnRenamed("State", "state")\
                            .withColumnRenamed("State Code", "state_code")\
                            .withColumnRenamed("Number of Veterans", "number_of_veterans")\
                            .withColumnRenamed("Foreign-born", "foreign_born")\
                            .withColumnRenamed("Average Household Size", "average_household_size")\
                            .withColumnRenamed("Race", "race")\
                            .withColumnRenamed("Count", "total")

        #Changing the data types of the numeric columns from string to the corresponding data types. 
        #This step is needed so that we could use aggregate functions with this data
        demos_df = demos_df.withColumn("median_age",demos_df.median_age.cast(DoubleType()))\
                            .withColumn("average_household_size",demos_df.average_household_size.cast(DoubleType()))\
                            .withColumn("male_population",demos_df.male_population.cast(IntegerType()))\
                            .withColumn("female_population",demos_df.female_population.cast(IntegerType()))\
                            .withColumn("total_population",demos_df.total_population.cast(IntegerType()))\
                            .withColumn("number_of_veterans",demos_df.number_of_veterans.cast(IntegerType()))\
                            .withColumn("foreign_born",demos_df.foreign_born.cast(IntegerType()))\
                            .withColumn("count",demos_df.total.cast(IntegerType()))\
                            .drop("total")
        
        #Pivoting data
        #Group by state_code, state, and city
        fixed_df = demos_df.groupby(["state", "city"])\
                            .agg({"state_code": "first", "median_age": "first", "male_population": "first",\
                                  "female_population": "first", "total_population": "first",\
                                  "number_of_veterans": "first", "foreign_born": "first",\
                                  "average_household_size": "first"})

        #Pivot our data by race
        pivot_df = demos_df.groupby(["state", "city"]).pivot("race").sum("count")

        #Join both dataframes and do the necessary transofrmation (i.e., rename columns, and fill null numeric values with 0)
        demos_df = fixed_df.join(other=pivot_df, on=["state", "city"], how="inner")\
                        .withColumnRenamed("American Indian and Alaska Native", "american_indian_and_alaska_native")\
                        .withColumnRenamed("Asian", "asian")\
                        .withColumnRenamed("Black or African-American", "black_or_african_American")\
                        .withColumnRenamed("Hispanic or Latino", "hispanic_or_latino")\
                        .withColumnRenamed("White", "white")\
                        .withColumnRenamed("first(foreign_born)", "foreign_born")\
                        .withColumnRenamed("first(male_population)", "male_population")\
                        .withColumnRenamed("first(average_household_size)", "average_household_size")\
                        .withColumnRenamed("first(total_population)", "total_population")\
                        .withColumnRenamed("first(median_age)", "maiden_age") \
                        .withColumnRenamed("first(number_of_veterans)", "number_of_veterans")\
                        .withColumnRenamed("first(female_population)", "female_population")\
                        .withColumnRenamed("first(state_code)", "state_code")\
                        .na.fill(0)
        
        #Join demos_df with i94addr_df to create a single dataframe for State information
        #Redundat columns are deleted after the join is performed
        print("Merging demographics data with i94addr data...")
        demos_df = i94addr_df.join(demos_df, i94addr_df.code == demos_df.state_code, "Left")\
                                .drop(i94addr_df.code)\
                                .drop(demos_df.state)

        #Creates an id column to be used as a primary key
        #In a standard relational model, state_code and city could be used as composite primary key
        demos_df = demos_df.withColumn("id", monotonically_increasing_id())
        print("\n----- Geerating dim_Demographics sample data -----\n")
        demos_df.show(10)
    except Exception as e:
        print(f"Error occured while processing dim_Demographocs data. {e}")
        return
    try:
        print("\nSaving dim_Demographics table...")
        demos_df.write.partitionBy("state_code", "city").mode("overwrite").parquet("output/dim_demographics.parquet")
        print(f"dim_Demographics table saved successfully.")
        return demos_df
    except Exception as e:
        print(f"Error occured while saving dim_Demographics table. {e}")


def process_i94visa_data(spark, f_content):
    """
    Generates dim_I94visa table
    """
    
    try:
        print("\nProcessing dim_I94visa table...")
        i94visa = {'1':'Business', '2': 'Pleasure', '3' : 'Student'}
        i94visa_list = list(i94visa.items())
        i94visa_df = spark.createDataFrame(i94visa_list)
        i94visa_df = i94visa_df.withColumnRenamed("_1", "id")\
                                .withColumnRenamed("_2", "type")
        i94visa_df = i94visa_df.withColumn("id", i94visa_df.id.cast(IntegerType()))
        print("\n----- Generating dim_I94visa sample data -----\n")
        i94visa_df.show()
    except Exception as e:
        print("Error occured while extracting i94visa data.")
        return
    try:
        print("\nSaving dim_I94visa table...")
        i94visa_df.write.partitionBy("id").mode("overwrite").parquet("output/dim_i94visa.parquet")
        print(f"dim_I94visa table saved successfully.")
        return i94visa_df
    except:
        print("Error while saving dim_I94visa table.")

        
def process_i94mode_data(spark, f_content):
    """
    Genrates dim_I94mode table
    """
    
    try:
        print("\nProcessing dim_I94mode table...")
        i94mode = code_mapper(f_content, "i94model")
        i94mode_list = list(i94mode.items())
        i94mode_df = spark.createDataFrame(i94mode_list)
        i94mode_df = i94mode_df.withColumnRenamed("_1", "id")
        i94mode_df = i94mode_df.withColumnRenamed("_2", "mode")
        i94mode_df = i94mode_df.withColumn("id", i94mode_df.id.cast(IntegerType()))
        print("\n----- Generating dim_I94mode sample data -----\n")
        i94mode_df.show()
    except Exception as e:
        print(f"Error occured while extracting i94mode data. {e}")
        return
    try:
        print("\nSaving dim_I94mode table...")
        i94mode_df.write.partitionBy("id").mode("overwrite").parquet("output/dim_i94mode.parquet")
        print(f"dim_I94mode table saved successfully.")
        return i94mode_df
    except Exception as e:
        print(f"Error occured while saving dim_I94mode table. {e}")

        
def process_i94citres_data(spark, f_content):
    """
    Processes i94cit_res data
    """
    
    try:
        print("\nExtracting i94cit_res data...")
        i94cit_res = code_mapper(f_content, "i94cntyl")
        i94cit_res_list = list(i94cit_res.items())
        i94cit_res_df = spark.createDataFrame(i94cit_res_list)
        i94cit_res_df = i94cit_res_df.withColumnRenamed("_1", "code")
        i94cit_res_df = i94cit_res_df.withColumnRenamed("_2", "country")
        i94cit_res_df = i94cit_res_df.withColumn("code", i94cit_res_df.code.cast(IntegerType()))
        i94cit_res_df = i94cit_res_df.dropna().drop_duplicates()
        i94cit_res_df = i94cit_res_df.filter((~F.lower(i94cit_res_df.country).contains('country')) & \
                                             (~F.lower(i94cit_res_df.country).contains('invalid')) & \
                                             (~F.lower(i94cit_res_df.country).contains('not show')))\
                                        .orderBy("country")

        #Fixing the name of Mexico so that it can be linked with the coresponding values in other tables
        i94cit_res_df = i94cit_res_df.withColumn("country", 
                                         F.when(F.col("code") == '582', "MEXICO").otherwise(F.col("country")))
        i94cit_res_df = i94cit_res_df.withColumn("code", i94cit_res_df.code.cast(IntegerType()))
        print("\n----- Generating i94cit_res sample data -----\n")
        i94cit_res_df.show(10)
        return i94cit_res_df
    except Exception as e:
        print(f"Error while extracting i94cit_res data. {e}")


def process_i94addr_data(spark, f_content):
    """
    Extracts i94addr data
    """
    
    try:
        print("\nExtracting i94addr data...")
        i94addr = code_mapper(f_content, "i94addrl")
        i94addr_list = list(i94addr.items())
        i94addr_df = spark.createDataFrame(i94addr_list)
        i94addr_df = i94addr_df.withColumnRenamed("_1", "code")\
                                .withColumnRenamed("_2", "state")  
        i94addr_df = i94addr_df.dropna()\
                                .drop_duplicates()
        print("----- Generating i94addr sample data -----")
        i94addr_df.show(10)
        return i94addr_df
    except Exception as e:
        print(f"Error while extracting i94addr data. {e}")


def process_i94port_data(spark, f_content):
    """
    Generates dim_I94port table
    """
    
    try:
        print("\nProcessing dim_I94port table...")
        i94port = code_mapper(f_content, "i94prtl")
        i94port_list = list(i94port.items())
        i94port_df = spark.createDataFrame(i94port_list)
        i94port_df = i94port_df.withColumnRenamed("_1", "code")
        i94port_df = i94port_df.withColumn("code", trim(col("code")))
        i94port_df = i94port_df.withColumn("port", trim(split(col("_2"), ", ").getItem(0)))\
                                .withColumn("state_code", trim(split(col("_2"), ", ").getItem(1)))\
                                .drop("_2")\
                                .dropDuplicates()\
                                .dropna()\
                                .orderBy("state_code", "port")
        print("\n----- Generating dim_I94port sample data -----\n")
        i94port_df.show(10)
    except Exception as e:
        print(f"Error while processing i94port data. {e}")
        return
    try:
        print("\nSaving dim_I94port table...")
        i94port_df.write.partitionBy("state_code").mode("overwrite").parquet("output/dim_i94port.parquet")
        print(f"dim_I94port table saved successfully.")
        return i94port_df
    except Exception as e:
        print(f"Error occured while saving dim_I94port table. {e}")

        
def process_country_data(spark, data_file, i94citres_df):
    """
    Generates dim_Country table
    """
    
    try:
        print("\nProcessing dim_Country table...\n")
        print("Extracting temperatures data...")
        temperatures_df = spark.read.option("header", "true").csv(data_file)
        temperatures_df = temperatures_df.drop("dt", "AverageTemperatureUncertainty", "City")
        temperatures_df = temperatures_df.dropna().drop_duplicates()

        #Capitalize the country field so that we could link it with i94cit dataset
        temperatures_df = temperatures_df.withColumn("Country", upper(col("Country")))

        #Group by country
        temperatures_df = temperatures_df.groupby("Country")\
                                            .agg({"AverageTemperature": "mean", "Latitude": "first", "Longitude": "first"})\
                                            .orderBy("Country")

        temperatures_df = temperatures_df.withColumnRenamed("Country", "country")\
                                            .withColumnRenamed("avg(AverageTemperature)", "average_temperature")\
                                            .withColumnRenamed("first(Latitude)", "latitude")\
                                            .withColumnRenamed("first(Longitude)", "longitude")
        temperatures_df = temperatures_df.withColumn("average_temperature", temperatures_df.average_temperature.cast(DoubleType()))\
        
        print("----- Generating Temperature sample data -----\n")
        temperatures_df.show(10)
        print("Merging temperature data with i94cit_res data...\n")
        #Join i94cit_res_df and temperatures_df and drop the redundant colum
        countries_df = i94citres_df.join(temperatures_df,i94citres_df.country ==  temperatures_df.country,"Left")\
                                    .drop(temperatures_df.country) #To avoid adding the common column twice
        print("Generating a sample from the merged data frames...\n")
        countries_df.show(10)
        
    except Exception as e:
        print(f"Error while processing dim_Country table. {e}")
        return
    try:
        print("\nSaving dim_Country table...")
        countries_df.write.partitionBy("code").mode("overwrite").parquet("output/dim_country.parquet")
        print(f"dim_Country table saved successfully.")
        return countries_df
    except:
            print("Error occured while saving dim_Country table.")

            
def process_date_data(spark, i94_df):
    """
    Generates dim_Date table
    """
    
    try:
        print("\nProcessing dim_Date table...")
        #Get a unique list of both arrival and departure dates from i94_df, and then combine them together
        arrdates_df = i94_df.select("arrdate").distinct()
        depdates_df = i94_df.select("depdate").distinct()
        date_df = arrdates_df.union(depdates_df).dropDuplicates()
        date_df = date_df.withColumnRenamed("arrdate", "sasdate")
        
        iso_date = convert_date(date_df.sasdate)
        dt = F.to_date(iso_date)
        year = F.year(dt)
        month = F.month(dt)
        day = F.dayofmonth(dt)
        week = F.weekofyear(dt)
        day_of_week = F.dayofweek(dt)
        is_weekend = day_of_week.isin([1,7]).cast("int")

        #Compose the dataframe using the values extracted from the sasdate field
        date_df = date_df.withColumn("isodate", dt.cast(DateType()))\
                            .withColumn("year", year.cast(IntegerType()))\
                            .withColumn("month", month.cast(IntegerType()))\
                            .withColumn("week", week.cast(IntegerType()))\
                            .withColumn("day", day.cast(IntegerType()))\
                            .withColumn("dayofweek", day_of_week.cast(IntegerType()))\
                            .withColumn("isweekend", is_weekend)\
                            .withColumn("season", get_season(month))\
                            .dropna()\
                            .orderBy("isodate")
        print("----- Generating dim_Date sample data -----")
        date_df.show(10)
    except Exception as e:
        print(f"Error occured while processing dim_Data table. {e}")
        return
    try:
        print("\nSaving dim_Date table...")
        date_df.write.partitionBy("sasdate").mode("overwrite").parquet("output/dim_date.parquet")
        print(f"dim_Date table saved successfully.")
        return date_df
    except Exception as e:
        print(f"Error occured while saving dim_Date table. {e}")

        
def validate_row_count(data_frame, table_name):
    """
    Checks if the passed dataframe has data
    """
    
    try:
        print(f"\nChecking {table_name}...")
        row_count = data_frame.count()
        if(row_count > 0):
            print(f"{table_name} created successfully. {row_count} rows found.")
        else:
            print(f"No data found in {table_name} table.")
        return row_count
    except Exception as e:
        print(f"Error occured while validating row count. {e}")

        
def validate_model_row_count(i94mode_df, i94visa_df, i94port_df, country_df, demos_df, date_df, i94_df):
    """
    Validates that the processed tables are not empty
    """
    
    try:
        print("\nChecking model row count...")
        mode_count = validate_row_count(i94mode_df, "dim_I94mode")
        visa_count = validate_row_count(i94visa_df, "dim_I94visa")
        port_count = validate_row_count(i94port_df, "dim_I94port")
        country_count = validate_row_count(country_df, "dim_Country")
        demos_count = validate_row_count(demos_df, "dim_Demographics")
        date_count = validate_row_count(date_df, "dim_Date")
        i94_count = validate_row_count(i94_df, "fact_I94")

        if (mode_count > 0) & \
            (visa_count > 0) & \
            (port_count > 0) & \
            (country_count > 0) & \
            (demos_count > 0) & \
            (date_count > 0) & \
            (i94_count > 0):
            print("\nRow count check completed successfully.")
            return True
        else:
            print("\nRow count check failed. ETL failed to generate some tables.")
            return False
    except Exception as e:
        print(e)
        
        
def validate_model_unique_keys(i94mode_df, i94visa_df, i94port_df, country_df, demos_df, date_df):
    """
    Counts unique values in each datarame and compare them to the number of rows in that dataframe. Both numbers should match.
    """
    
    print("\nValidating unique keys...\n")
    
    mode_dis_count = i94mode_df.select("id").distinct().count()
    i94mode_count = i94mode_df.count()
    mode_valid = (mode_dis_count == i94mode_count)
    print("Vlidating dim_I94mode. Pass?", mode_valid)
    
    visa_dis_count = i94visa_df.select("id").distinct().count()
    i94visa_count = i94visa_df.count()
    visa_valid = (visa_dis_count == i94visa_count)
    print("Validating dim_I94visa. Pass?", visa_valid)
    
    port_dis_count = i94port_df.select("code").distinct().count()
    i94port_count = i94port_df.count()
    port_valid = (port_dis_count == i94port_count)
    print("Validating dim_I94port. Pass?", port_valid)
    
    country_dis_count = country_df.select("code").distinct().count()
    country_count = country_df.count()
    country_valid = (country_dis_count == country_count)
    print("Validating dim_Country. Pass?", country_valid)
    
    date_dis_count = date_df.select("sasdate").distinct().count()
    date_count = date_df.count()
    date_valid = (date_dis_count == date_count)
    print("checking dim_Date. Pass?", date_valid)

    demos_dis_count = demos_df.select("state", "city").distinct().count()
    demos_count = demos_df.count()
    demos_valid = (demos_dis_count == demos_count)
    print("Validating dim_Demographics. Pass?", demos_valid)

    return (country_valid & port_valid & mode_valid & visa_valid & date_valid & demos_valid)
        
    
def validate_country_data(country_df):
    """
    Makes sure that the changes made to i94cit_res_df during the cleansing process are persisted. This is just to test one of those changes
    """
    
    print("\nValidating data...\n")
    invalid_df = country_df[F.lower(country_df["country"]).contains("country") |\
                               F.lower(country_df["country"]).contains("invalid") |\
                               F.lower(country_df["country"]).contains("not show")]
    if(invalid_df.count() == 0):
        print("Data validation passed.")
        return True
    else:
        print("Data validation failed. Some changes made during data transformation were not persisted.")
        return False
    

def main():
    spark = create_spark_session()
    
    #Process data
    i94visa_df = process_i94visa_data(spark, f_content)
    i94mode_df = process_i94mode_data(spark, f_content)
    i94citres_df = process_i94citres_data(spark, f_content)
    country_df = process_country_data(spark, "../../data2/GlobalLandTemperaturesByCity.csv", i94citres_df)
    i94port_df = process_i94port_data(spark, f_content)
    i94addr_df = process_i94addr_data(spark, f_content)
    demos_df = process_demographics_data(spark, "us-cities-demographics.csv", i94addr_df)
    i94_df = process_i94_data(spark, "./sas_data")
    date_df = process_date_data(spark, i94_df)
    
    #Data quality check #1
    row_count_check_result = validate_model_row_count(i94mode_df, i94visa_df,i94port_df, country_df, demos_df, date_df, i94_df)
        
    #Data quality check #2
    unique_keys_check_result = validate_model_unique_keys(i94mode_df, i94visa_df, i94port_df, country_df, demos_df, date_df)
        
    #Data quality check #3
    data_check_result = validate_country_data(country_df)
    
    if (row_count_check_result & unique_keys_check_result & data_check_result):
        print("\nAll data quality checks passed successfully.")
    else:
        print("\nSome data quality checks failed.")

        
if __name__ == "__main__":
    main()
    
