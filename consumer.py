from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Initialize Spark with Kafka and S3 Connectors
spark = SparkSession.builder \
    .appName("FintechStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

# 2. Configure S3/MinIO Access
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "password")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# 3. Define the Schema of the incoming JSON
schema = StructType([
    StructField("price", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("time", StringType(), True)
])

# 4. Read the Stream from Redpanda
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "market-topic") \
    .load()

# 5. Parse the JSON and convert price to Double
clean_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("price", col("price").cast(DoubleType()))

# 6. Write to MinIO as Parquet
query = clean_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://market-data/btc_prices") \
    .option("checkpointLocation", "s3a://market-data/checkpoints") \
    .start()

print("Spark is now processing moola... Check MinIO in a few minutes!")
query.awaitTermination()