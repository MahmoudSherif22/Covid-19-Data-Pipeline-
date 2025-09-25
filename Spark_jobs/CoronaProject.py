#import Spark libraries
from pyspark.sql import SparkSession
import os
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType
from pyspark.sql.functions import *
#from DataQuality import IngestDataStage_qualitychecks
#from Extract import define_schema ,find_file,get_csv_file,read_csv_file

def define_schema():
    data_schema = StructType([StructField('FIPS',StringType() ,True),StructField('Admin2',StringType() ,True),StructField('Province_State',StringType() ,True),
                              StructField('Country_Region',StringType() ,True),StructField('Last_update',StringType() ,True),StructField('Lat',FloatType() ,True),
                              StructField('Long_',FloatType() ,True),StructField('Confirmed',IntegerType() ,True),StructField('Deaths',IntegerType() ,True),
                              StructField('Recovered',IntegerType() ,True),StructField('Active',IntegerType() ,True),StructField('Combined_Key',StringType() ,True),
                              StructField('Incident_Rate',FloatType() ,True),StructField('Case_Fatality_Ratio',FloatType() ,True)])
    return data_schema

def find_file(path, current_date):
    for f in os.listdir(path):
        if f.split('.')[0] == current_date:
            print(f"File found in {path}")
            return os.path.join(path, f)
    return None

def get_csv_file(path1, path2, current_date):
    csv_1 = find_file(path1, current_date)
    csv_2 = find_file(path2, current_date)
    return csv_1, csv_2

def read_csv_file(file1,file2,schema,spark):
    if file1 != None and file2 != None:
        df = spark.read\
        .option("header",True)\
        .option("enforceSchema", True) \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(schema)\
        .csv([file1,file2]).repartition('Country_Region')
    elif file1 == None and file2 != None:
        df = spark.read\
        .option("header",True)\
        .option("enforceSchema", True) \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(schema)\
        .csv(file2).repartition('Country_Region')
    elif file1 != None and file2 == None:
        df = spark.read\
        .option("header",True)\
        .option("enforceSchema", True) \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(schema)\
        .csv(file1).repartition('Country_Region')
    else :
        return None

    return df
#start spark session
def Start_SparkSession(appName):
    spark = SparkSession.builder\
           .master('spark://spark-master:7077').\
           appName(appName).\
           getOrCreate()
    return spark

##########################################Transformation####################################################

def clean_data(df):
    #trim column
    df1 = df.withColumn('Province_state',trim(col('Province_state')))
    #standaradize US to United States
    df = df1.na.replace({'US':'United States'},subset = ['Country_Region'])
    #Handling Nulls
    # identify important columns if its contain Null values drop
    validdate = df.select(first('Last_update',ignorenulls = True)).first()[0]
    df_NullsHandled = df.fillna({
        "Province_State": "Unknown",
        "Lat": 0.0,
        "Long_": 0.0,
        "Confirmed": 0,
        "Deaths": 0,
        "Recovered": 0,
        "Active": 0,
        "Incident_Rate": 0.0,
        "Case_Fatality_Ratio": 0.0
    })

    df_NullsHandled = df_NullsHandled.drop('FIPS').drop('Admin2').drop('Combined_Key')


    #df_NullsHandled.select('Incident_Rate').show()
    #convert all date to standard YYYY-MM-DD
    df_DateStandardized = df_NullsHandled.withColumn('Last_update',to_date(col('Last_update').cast("date"),'d MM y'))
    return df_DateStandardized


if __name__ == "__main__":
    #start spark session
    try:
        spark = Start_SparkSession('Corona')
    except Exception as e:
        print(f"Error in starting spark session")

    #define schema

    try:
        schema = define_schema()
    except Exception as e :
        print(f"Error in defining schema {e}")

    #Get csv files
    
    try:
        csv1 ,csv2 = get_csv_file('/opt/data/Corona_data','/opt/data/Corona_data_us','01-01-2021') #'/home/jovyan/work/csv_data/Corona_data_us'
    except Exception as e :
        print(f"Error in Getting files : {e}")

    #Read csv files
    
    try:
        df = read_csv_file(csv1,csv2,schema,spark)
    except Exception as e :
        print(f"Error in reading csv files : {e}")

    #start cleaning data
    try:
        new_df = clean_data(df)
    except Exception as e:
        print(f"error in cleaning Data {e}")

    new_df.select('*').show()
    #Write to postgres temporary Staging area 
    try:
        new_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_staging:5432/staging_db") \
        .option("dbtable", "staging_table") \
        .option("user", "staging_user") \
        .option("password", "staging_pass") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    except Exception as e:
        print(f"error in writing data to postgres :{e}")
