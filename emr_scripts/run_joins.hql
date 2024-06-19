-- Enable Map Join (Broadcast Join)
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=10485760;  -- 10 MB threshold

-- Perform the join and load data into the curated_data table
INSERT OVERWRITE TABLE curated_data PARTITION (partition_date)
SELECT a.product_id, a.title, a.price, a.category, b.rating, a.partition_date
FROM processed_products a
JOIN processed_ratings b
ON a.product_id = b.product_id
AND a.partition_date = b.partition_date;

-- Enable Bucketed Map Join
SET hive.optimize.bucketmapjoin=true;
SET hive.optimize.bucketmapjoin.sortedmerge=true;

-- Perform the join with bucketed tables (if bucketed tables are created)
--INSERT OVERWRITE TABLE curated_data PARTITION (partition_date)
--SELECT a.product_id, a.title, a.price, a.category, b.rating, a.partition_date
--FROM bucketed_products a
--JOIN bucketed_ratings b
--ON a.product_id = b.product_id
--AND a.partition_date = b.partition_date;

-- Enable Sort-Merge Join
SET hive.optimize.bucketmapjoin=true;
SET hive.optimize.bucketmapjoin.sortedmerge=true;
SET hive.optimize.sortmerge.join=true;

-- Perform the join with sorted and bucketed tables (if sorted bucketed tables are created)
--INSERT OVERWRITE TABLE curated_data PARTITION (partition_date)
--SELECT a.product_id, a.title, a.price, a.category, b.rating, a.partition_date
--FROM sorted_bucketed_products a
--JOIN sorted_bucketed_ratings b
--ON a.product_id = b.product_id
--AND a.partition_date = b.partition_date;


MSCK REPAIR TABLE curated_data;
