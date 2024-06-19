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

# Generate ratings data
num_records = 20000000
ratings = []

for _ in range(num_records):
    ratings.append((random.randint(1, 100000), random.randint(1, 5)))

# Create DataFrame
columns = ["product_id", "rating"]
ratings_df = spark.createDataFrame(ratings, schema=columns)

# Add current date as a column
date_str = datetime.now().strftime('%Y-%m-%d')
ratings_df = ratings_df.withColumn("partition_date", lit(date_str))

# Convert to DynamicFrame
ratings_dynamic_frame = DynamicFrame.fromDF(ratings_df, glueContext, "ratings_dynamic_frame")

# Write partitioned data back to S3 in ORC format
glueContext.write_dynamic_frame.from_options(
    frame=ratings_dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://srikar95-bucket/processed/ratings/", "partitionKeys": ["partition_date"]},
    format="orc"
)

job.commit()