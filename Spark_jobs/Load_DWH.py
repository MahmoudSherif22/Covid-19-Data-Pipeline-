from pyspark.sql import SparkSession
from pyspark.sql import functions
import datetime
from pyspark.sql.types import DateType
def start_SparkSession():
    spark = SparkSession.builder\
    .appname('LoadToDWH')\
    .master(Local['*'])\
    .getOrCreate()
    return spark

# Read data from staging postgres table 
def Read_Staging(spark):
    staging_df = spark.read\
    .format('jdbc')\
    .option('dbtable','staging_table')\
    .oprtion('user','staging_user')\
    .option('password','staging_pass')\
    .option("driver", "org.postgresql.Driver")\
    .load()
    return staging_df

#Create Location Dimension from Staging Area 

def Loc_Dim_Staging(spark,staging_df):
    Loc_Dim_df = staging_df.select('Province_State','Lat','Long_','Country_Region').drop_duplicates()
    return Loc_Dim_df

#Get the Location Dimension from the DWH (Will be used to detect new changes ,Handling SCD )

def Get_Loc_Dim_DWH(spark):
    Loc_Dim_df_DWH = spark.read\
    .format('jdbc')\
    .option('dbtable','Loc_table')\
    .oprtion('user','staging_user')\
    .option('password','staging_pass')\
    .option("driver", "org.postgresql.Driver")\
    .load()
    return Loc_Dim_df_DWH

#Detect new records using joins and write the new Loc_Dim 
def Loc_Dim(staging_df,loc_dim_df):
    join_cond = [
    staging_df["Country_Region"] == loc_dim_df["country_region"],
    staging_df["Province_State"] == loc_dim_df["province_state"]
    ]
    merged_df = staging_df.join(loc-dim_df,join_cond,'left') 

   #Get the new locations 
   new_locations = merged_df.filter(loc_dim_df['Loc_Sk'].isNull())\ # if its new location the Loc_sk will equal null
    .select("Country_Region", "Province_State", "Lat", "Long_")


   # B. Changed attributes (example: Lat/Long changed)
   changed_records = merged_df.filter(
    (loc_dim_df["loc_sk"].isNotNull()) &
    ((staging_df["Lat"] != loc_dim_df["lat"]) |
     (staging_df["Long_"] != loc_dim_df["long"]))
    )

   # Insert new record with updated attributes
   new_version_records = changed_records.select(
    staging_df["Country_Region"].alias("country_region"),
    staging_df["Province_State"].alias("province_state"),
    staging_df["Lat"].alias("lat"),
    staging_df["Long_"].alias("long")
    )
   # Append new/changed records
   (new_records.union(new_version_records)) \
    .write.mode("append").jdbc("jdbc:postgresql://postgres_staging:5432/staging_db", "loc_dim", 
                               properties={
    "user": "myuser",
    "password": "mypassword",
    "driver": "org.postgresql.Driver"
                                          })
#Date dimension table (fixed table)
def Date_dim():
    #Define 
    start_date = datetime.date(2020,1,1)
    end_date = datetime.date(2022,12,30)
    dates = [(start_date + datetime.timedelta(days=x),) 
         for x in range((end_date - start_date).days + 1)]
    date_df = spark.createDataFrame(dates, ["full_date"]) \
    .withColumn("day", dayofmonth("full_date")) \
    .withColumn("month", month("full_date")) \
    .withColumn("year", year("full_date")) \
    .withColumn("quarter", quarter("full_date")) \
    .withColumn("weekday_name", date_format("full_date", "EEEE")) \
    .withColumn("is_weekend", when(dayofweek("full_date").isin([1,7]), True).otherwise(False)
    date_df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_staging:5432/staging_db") \
        .option("dbtable", "Date_Dim") \
        .option("user", "staging_user") \
        .option("password", "staging_pass") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
    

#create fact table by joining between staging table and dimension tables
def Covid_fact(staging_df,Loc_dim_df,Date_dim_df):
    join_cond = [
    staging_df["Country_Region"] == loc_dim_df["country_region"],
    staging_df["Province_State"] == loc_dim_df["province_state"],
    staging_df["Lat"] == Loc_dim_df["Lat"]
    staging_df["Day"] == Date_dim_df["Day"]
    staging_df["Month"] == Date_dim_df["Month"]
    staging_df["Year"] == Date_dim_df["Year"]
    ]
    covid_fact = staging_df.join(Loc_dim_df,,'inner').join(Date_dim_df,,'inner')\
    .select(staging_df['Confirmed'],staging_df['Deaths'],staging_df['Recovered'],staging_df['Active'],
            staging_df['Incident_Rate'],staging_df['Case_Fatality_Ratio'],staging_df['Loc_sk_pk'].alias('Loc_SK_FK'),
            staging_df['Date_SK_Pk'].alias('Date_SK_FK'))
    
    covid_fact.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_staging:5432/staging_db") \
        .option("dbtable", "fact_covid") \
        .option("user", "staging_user") \
        .option("password", "staging_pass") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
    
    


    