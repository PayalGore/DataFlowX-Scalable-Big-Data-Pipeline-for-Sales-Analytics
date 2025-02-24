from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, year, month, col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("S3DataIngestion") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Load the dataset from S3
df = spark.read.csv("s3a://payagoreb1/Superstore Dataset.csv", header=True, inferSchema=True)

# Convert timestamp to human-readable date and extract Year and Month
df_transformed = df.withColumn("Year", year(col("Order Date"))) \
                   .withColumn("Month", month(col("Order Date")))

# Coalesce to a single partition and write to S3 as a single CSV file
df_transformed.coalesce(1).write.csv("s3a://payagoreb1/output/year_month_transformed_single_file", header=True, mode="overwrite")

# Stop the SparkSession
spark.stop()
