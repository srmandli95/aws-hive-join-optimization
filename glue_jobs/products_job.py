import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext, DynamicFrame
from awsglue.job import Job
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import random

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Generate products data
num_records = 100000
categories = [f'Category{i}' for i in range(1, 6)]
products = []

for i in range(1, num_records + 1):
    products.append((i, f'Product{i}', round(random.uniform(10, 100), 2), random.choice(categories)))

# Create DataFrame
columns = ["product_id", "title", "price", "category"]
products_df = spark.createDataFrame(products, schema=columns)

# Add current date as a column
date_str = datetime.now().strftime('%Y-%m-%d')
products_df = products_df.withColumn("partition_date", lit(date_str))

# Convert to DynamicFrame
products_dynamic_frame = DynamicFrame.fromDF(products_df, glueContext, "products_dynamic_frame")

# Write partitioned data back to S3 in ORC format
glueContext.write_dynamic_frame.from_options(
    frame=products_dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://srikar95-bucket/processed/products/", "partitionKeys": ["partition_date"]},
    format="orc"
)

job.commit()