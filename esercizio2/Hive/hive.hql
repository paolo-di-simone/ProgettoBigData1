
DROP TABLE IF EXISTS reviews_doc;
DROP TABLE IF EXISTS reviews_tmp;

CREATE TABLE reviews_doc (id STRING, product_id STRING, user_id STRING, profile_name STRING,
                               helpfulness_numerator INT, helpfulness_denominator INT,
                               score INT, time_string INT, summary STRING, text STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/paolods/Desktop/ProgettoBigData/Dataset/Reviews-Parsed.csv' OVERWRITE INTO TABLE reviews_doc;
    
CREATE TABLE reviews_tmp AS
    SELECT user_id, AVG(quality) as mean_quality
    FROM (
        SELECT user_id, CASE
            WHEN helpfulness_numerator = 0 OR helpfulness_denominator = 0 THEN 0
            ELSE 1.0 * helpfulness_numerator / helpfulness_denominator
          END AS quality
        FROM reviews_doc
    ) tmp
    GROUP BY user_id;
    
INSERT OVERWRITE LOCAL DIRECTORY '/home/paolods/Desktop/ProgettoBigData/esercizio2/Hive/dir0'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * FROM reviews_tmp ORDER BY mean_quality DESC;

DROP TABLE IF EXISTS reviews_doc;
DROP TABLE IF EXISTS reviews_tmp;
