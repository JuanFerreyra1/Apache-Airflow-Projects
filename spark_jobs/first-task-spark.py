from pyspark.sql import SparkSession 
import os


airflow_home = os.getenv('HOME')

def get_contexts():
      spark = SparkSession.builder \
            .appName("Spark-script")\
            .master('local')\
            .config("spark.driver.memory","8g")\
            .config("spark.executor.memory","8g")\
            .getOrCreate()
      return spark

if __name__ == "__main__":
      spark_context = get_contexts() 
      my_dataframe = spark_context.read.csv(path="/airflow/data/organizations.csv",header=True)
      my_dataframe = my_dataframe.withColumn("indexx",my_dataframe.indexx.cast('int'))
      
      my_dataframe.write.format("jdbc")\
            .option("url","jdbc:postgresql://localhost:5432/project1")\
            .option("dbtable","public.pets")\
            .option("user", "locked")\
            .option("password", "locked").save(mode="append")
