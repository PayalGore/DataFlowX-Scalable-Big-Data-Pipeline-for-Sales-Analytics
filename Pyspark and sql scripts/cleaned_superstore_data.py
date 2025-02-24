# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("S3DataIngestion") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Load the dataset from S3
input_path = "s3a://payagoreb1/Superstore Dataset.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Display schema and data
print("Original Data Schema:")
df.printSchema()
df.show(5)

# Convert 'Order Date' and 'Ship Date' columns to datetime format
df = df.withColumn("Order Date", to_date(col("Order Date"), "yyyy-MM-dd"))
df = df.withColumn("Ship Date", to_date(col("Ship Date"), "yyyy-MM-dd"))

# Remove rows where 'Ship Date' is earlier than 'Order Date'
df = df.filter(col("Ship Date") >= col("Order Date"))

# Drop duplicates
df = df.dropDuplicates()

# Display cleaned data
print("Cleaned Data Schema:")
df.printSchema()
df.show(5)

# Save the cleaned data back to S3
output_path = "s3a://payagoreb1/cleaned_superstore_data.csv"  # Replace <your-bucket-name> with your S3 bucket name
df.write.csv(output_path, header=True, mode="overwrite")

print(f"Cleaned dataset saved to {output_path}")
