import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('/resources/.env')
load_dotenv(dotenv_path=dotenv_path)

postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('RuangDataProject')
        .setMaster('local')
    ))
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_dw_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

#read csv data
df_csv = spark.read.format("csv").option("header","true")\
    .load("/resources/data/owid-covid-data.csv")

# Register the DataFrame as a SQL temporary view
df_csv.createOrReplaceTempView("owid_covid")

sqlDF = spark.sql("""
                   SELECT location, count(total_cases) as total_cases, count(total_deaths) as total_deaths
                  FROM owid_covid
                  group by location
                  order by total_cases desc
                  """).show()

# Ensure sqlDF is not None before writing
if sqlDF is not None and sqlDF.rdd.count() > 0:
    sqlDF.write.mode("overwrite").jdbc(
        jdbc_url,
        'public.count_owid_covid',
        properties=jdbc_properties
    )

#read data again
spark.read.jdbc(
        jdbc_url,
        'public.count_owid_covid',
        properties=jdbc_properties
    ).show()