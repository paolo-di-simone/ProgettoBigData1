#!/usr/bin/env python3
"""spark application"""

from pyspark.sql import SparkSession

warehouse_path = "/user/hive/warehouse"

# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Spark SQL Quality") \
    .config("spark.sql.warehouse.dir", warehouse_path) \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("DROP TABLE IF EXISTS reviews_doc")
spark.sql("DROP TABLE IF EXISTS reviews_tmp")

spark.sql("""
    CREATE TABLE reviews_doc (
        id STRING, product_id STRING, user_id STRING, profile_name STRING,
        helpfulness_numerator INT, helpfulness_denominator INT,
        score INT, time_string INT, summary STRING, text STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
""")

spark.sql('''
    LOAD DATA LOCAL INPATH 
	'/home/paolods/Desktop/ProgettoBigData/Dataset/Reviews-Parsed.csv' 
	OVERWRITE INTO TABLE reviews_doc;
''')

spark.sql("""
    CREATE TABLE reviews_tmp AS
        SELECT user_id, AVG(quality) as mean_quality
        FROM (
            SELECT user_id, CASE
                WHEN helpfulness_numerator = 0 OR helpfulness_denominator = 0 THEN 0
                ELSE 1.0 * helpfulness_numerator / helpfulness_denominator
              END AS quality
            FROM reviews_doc
        ) tmp
        GROUP BY user_id
""")

spark.sql('''
    INSERT OVERWRITE LOCAL DIRECTORY 
	'/home/paolods/Desktop/ProgettoBigData/esercizio2/SparkSQL/dir0'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    SELECT * FROM reviews_tmp ORDER BY mean_quality DESC
''')

spark.sql("DROP TABLE IF EXISTS reviews_doc")
spark.sql("DROP TABLE IF EXISTS reviews_tmp")

spark.stop()