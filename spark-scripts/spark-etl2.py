from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
dotenv_path = Path('/resources/.env')
load_dotenv(dotenv_path=dotenv_path)

postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

# Initialize Spark session with JDBC driver
spark = SparkSession.builder \
    .appName("RuangDataProject") \
    .config("spark.driver.extraClassPath", "/opt/airflow/jars/postgresql-42.2.18.jar") \
    .config("spark.executor.extraClassPath", "/opt/airflow/jars/postgresql-42.2.18.jar") \
    .getOrCreate()

jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_dw_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

# Read CSV
df_csv = spark.read.format("csv").option("header", "true").load("/resources/data/owid-covid-data.csv")

# Register DataFrame as a temporary view
df_csv.createOrReplaceTempView("owid_covid")

# Run SQL query
sqlDF = spark.sql("""
                   SELECT location, count(total_cases) as total_cases, count(total_deaths) as total_deaths
                   FROM owid_covid
                   WHERE continent = 'Asia'
                   GROUP BY location
                   ORDER BY total_cases DESC
                   """)

# Ensure DataFrame is not empty before writing
if sqlDF.count() > 0:
    sqlDF.write.mode("overwrite").jdbc(
        jdbc_url,
        'public.count_owid_covid_asia',
        properties=jdbc_properties
    )

# Read data again
spark.read.jdbc(
    jdbc_url,
    'public.count_owid_covid_asia',
    properties=jdbc_properties
).show()