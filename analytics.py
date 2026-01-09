from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, col, count

# 1. Initialize Spark (Same Setup as Consumer)
spark = SparkSession.builder \
    .appName("FintechAnalytics") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

# 2. Configure MinIO Access
spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
spark.conf.set("spark.hadoop.fs.s3a.access.key", "admin")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "password")
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

print("--- READING FROM DATA LAKE ---")

# 3. Read the Parquet files into a DataFrame
df = spark.read.parquet("s3a://market-data/btc_prices")

# 4. Run Analytics
# We calculate count, average price, and the spread (Max - Min)
stats = df.select(
    count("*").alias("Total_Trades_Recorded"),
    avg("price").alias("Avg_Price"),
    (max("price") - min("price")).alias("Price_Volatility_Spread")
)

stats.show()

print("--- SAMPLE DATA ---")
df.orderBy(col("time").desc()).show(5)

spark.stop()