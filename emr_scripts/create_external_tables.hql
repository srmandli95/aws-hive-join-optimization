CREATE EXTERNAL TABLE IF NOT EXISTS processed_products (
    product_id INT,
    title STRING,
    price FLOAT,
    category STRING
)
PARTITIONED BY (partition_date STRING)
STORED AS ORC
LOCATION 's3://your-bucket-name/processed/products/';

CREATE EXTERNAL TABLE IF NOT EXISTS processed_ratings (
    product_id INT,
    rating INT
)
PARTITIONED BY (partition_date STRING)
STORED AS ORC
LOCATION 's3://your-bucket-name/processed/ratings/';

-- Create the curated_data table
CREATE EXTERNAL TABLE IF NOT EXISTS curated_data (
    product_id INT,
    title STRING,
    price FLOAT,
    category STRING,
    rating INT
)
PARTITIONED BY (partition_date STRING)
STORED AS ORC
LOCATION 's3://your-bucket-name/curated_data/';


MSCK REPAIR TABLE processed_products;
MSCK REPAIR TABLE processed_products;




