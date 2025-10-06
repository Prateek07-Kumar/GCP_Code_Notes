# import sparkSession : entry point to the pyspark functionality
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

#create SparkSession
spark = SparkSession.builder \
                    .master("local") \
                    .appName("job_1") \
                    .getOrCreate()
# source connection
source_path = "gs://landing_dataset/sampledata.csv"
df1 = spark.read.csv(source_path, header=True, inferSchema=True)
print("establish source connection successfully")

# transformations
df2 = df1.withColumn("amount", col("item_price") * col("quantity"))

# total amount by each customer
df3 = df2.groupBy("customer_id").agg(sum("amount").alias("total_amount"))
print("got total amount by each customer")

#Load it to destination
temp_bucket = "landing_dataset/temp_23"

df3.write.format("bigquery") \
         .option("temporaryGcsBucket", temp_bucket) \
         .option("table", "new-gcp-cloud-sql-project.master_ds.pysparkTable2") \
         .mode("overwrite") \
         .save()
print("loaded to bq successfully")

