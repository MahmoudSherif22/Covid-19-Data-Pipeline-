from airflow import DAG 
from datetime import datetime,timedelta
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(dag_id = "firstDag",description = "this is dag to run spark jobs for data cleaning",
         start_date =datetime(2025,9,22,11,47),schedule_interval=timedelta(minutes = 5),catchup=False,
         dagrun_timeout= timedelta(minutes=3),tags =['Data Cleaning']) as dag :
         
    preprocess_task  = SparkSubmitOperator(
        task_id = 'preprocess_data',
        application = "/opt/airflow/spark_jobs/CoronaProject.py",
        conn_id="spark_default",
        verbose = True ,
        jars="/opt/spark/jars/postgresql-42.7.3.jar",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.app.name": "Corona"
        }
        )
    
    create_DWH_task = SparkSubmitOperator()
        task_id = "create_DWH_task",
        application = "/opt/airflow/spark_jobs/Create_DWH.py",
        conn_id="spark_default",
        verbose = True ,
        jars="/opt/spark/jars/postgresql-42.7.3.jar",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.app.name": "DWH"
        }
    preprocess_task >> Create_DWH_task
        

        

                     
     
