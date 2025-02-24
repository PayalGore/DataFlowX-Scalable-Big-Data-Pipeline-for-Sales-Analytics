from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, sum, avg, col, desc

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("S3DataIngestion") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Load the dataset from S3
df = spark.read.csv("s3a://payagoreb1/Superstore Dataset.csv", header=True, inferSchema=True)

# Add 'Year' and 'Month' columns
df_transformed = df.withColumn("Year", year(col("Order Date"))) \
                   .withColumn("Month", month(col("Order Date")))

# 1. Total revenue by region
revenue_by_region = df_transformed.groupBy("Region").agg(sum("Sales").alias("Total Revenue"))
revenue_by_region.show(10)

# Write the result to CSV
revenue_by_region.write.csv("s3a://payagoreb1/output/total_revenue_by_region", header=True, mode="overwrite")

# 2. Monthly sales trends
monthly_sales = df_transformed.groupBy("Year", "Month").agg(sum("Sales").alias("Monthly Sales"))
monthly_sales.show(10)

# Write the result to CSV
monthly_sales.write.csv("s3a://payagoreb1/output/monthly_sales_trends", header=True, mode="overwrite")

# 3. Top 10 customers by transaction value
top_customers = df_transformed.groupBy("Customer Name").agg(sum("Sales").alias("Total Sales"))
top_customers.orderBy(desc("Total Sales")).limit(10).show(10)

# Write the result to CSV
top_customers.orderBy(desc("Total Sales")).limit(10) \
    .write.csv("s3a://payagoreb1/output/top_10_customers", header=True, mode="overwrite")

# 4. Average profit margin by category
profit_margin = df_transformed.groupBy("Category").agg(avg(col("Profit") / col("Sales")).alias("Average Profit Margin"))
profit_margin.show(10)

# Write the result to CSV
profit_margin.write.csv("s3a://payagoreb1/output/average_profit_margin", header=True, mode="overwrite")

# 5. Most frequently purchased products
top_products = df_transformed.groupBy("Product Name").agg(sum("Quantity").alias("Total Quantity"))
top_products.orderBy(desc("Total Quantity")).limit(10).show(10)

# Write the result to CSV
top_products.orderBy(desc("Total Quantity")).limit(10) \
    .write.csv("s3a://payagoreb1/output/top_products", header=True, mode="overwrite")

# Stop the SparkSession
spark.stop()
