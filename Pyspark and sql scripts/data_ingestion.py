from pyspark.sql import SparkSession

# Create SparkSession with S3 configurations
spark = SparkSession.builder \
    .appName("S3DataIngestion") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.1000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Read JSON data from S3
df = spark.read.csv("s3a://payagoreb1/Superstore Dataset.csv", header=True, inferSchema=True)


# Display schema
print("Dataset Schema:")
df.printSchema()

# Show sample data
print("\nSample Data:")
df.show(5)

# Get basic statistics
print("\nDataset Statistics:")
print("Number of rows:", df.count())
print("Number of columns:", len(df.columns))
