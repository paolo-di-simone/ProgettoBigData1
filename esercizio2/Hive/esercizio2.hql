DROP TABLE IF EXISTS reviews_doc;
DROP TABLE IF EXISTS reviews;
DROP TABLE IF EXISTS reviews_tmp;

CREATE TABLE reviews_doc (id STRING, product_id STRING, user_id STRING, profile_name STRING,
                               helpfulness_numerator INT, helpfulness_denominator INT,
                               score INT, time_string INT, summary STRING, text STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/paolods/Desktop/ProgettoBigData/Dataset/n_input/Reviews-100000.csv' OVERWRITE INTO TABLE reviews_doc;

ADD FILE Desktop/ProgettoBigData/esercizio2/Hive/make_quality.py;

CREATE TABLE reviews AS
	SELECT TRANSFORM(reviews_doc.user_id, reviews_doc.helpfulness_numerator, reviews_doc.helpfulness_denominator)
	    USING 'python3 Desktop/ProgettoBigData/esercizio2/Hive/make_quality.py' AS user_id, quality
	FROM reviews_doc;

CREATE TABLE reviews_tmp AS
    SELECT user_id, AVG(quality) as mean_quality
    FROM reviews AS r
    GROUP BY r.user_id;
    

INSERT OVERWRITE LOCAL DIRECTORY '/home/paolods/Desktop/ProgettoBigData/esercizio2/Hive/dir0'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * FROM reviews_tmp ORDER BY mean_quality DESC;

DROP TABLE IF EXISTS reviews_doc;
DROP TABLE IF EXISTS reviews;
DROP TABLE IF EXISTS reviews_tmp;
