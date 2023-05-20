
DROP TABLE IF EXISTS reviews;
DROP TABLE IF EXISTS reviews_for_year;
DROP TABLE IF EXISTS top_product;
DROP TABLE IF EXISTS words;
DROP TABLE IF EXISTS temp_word;
DROP TABLE IF EXISTS word_list;

CREATE TABLE reviews (id STRING, productId STRING, userId STRING, profileName STRING,
                               helpfulnessNumerator INT, helpfulnessDenominator INT,
                               score INT, time INT, summary STRING, text STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';


LOAD DATA LOCAL INPATH '/home/paolods/Desktop/ProgettoBigData/Dataset/Reviews-Parsed.csv' OVERWRITE INTO TABLE reviews;


CREATE TABLE reviews_for_year AS
       SELECT time, productid, count(*) AS number_reviews 
       FROM reviews
       GROUP BY time, productid; 


CREATE TABLE top_product AS 
       SELECT *
       FROM (SELECT time, productid, number_reviews, ROW_NUMBER() OVER (PARTITION BY time ORDER BY number_reviews DESC) as rank FROM reviews_for_year) t 
       WHERE rank <= 10;



CREATE TABLE words AS
       SELECT top_product.time as time, top_product.productid as id, reviews.text as text
       FROM top_product JOIN reviews ON (top_product.productid = reviews.productid);
       


CREATE TABLE temp_word AS
       SELECT words.time as time, words.id as id , exploded_line.word as word
       FROM words
       LATERAL VIEW explode(split(words.text, ' ')) exploded_line AS word;



CREATE TABLE word_list AS
	SELECT temp_word.time as year, temp_word.id as id, temp_word.word as word, count(*) number_word  
	FROM temp_word
	WHERE length(word) >= 4
	GROUP BY temp_word.time, temp_word.id, temp_word.word;


INSERT OVERWRITE LOCAL DIRECTORY '/home/paolods/Desktop/ProgettoBigData/esercizio1/Hive/dir0'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT *
FROM (SELECT year, id, word, number_word, ROW_NUMBER() OVER (PARTITION BY year,id ORDER BY number_word DESC) as rank FROM word_list) t 
WHERE rank <= 5 ;



DROP TABLE IF EXISTS reviews;
DROP TABLE IF EXISTS reviews_for_year;
DROP TABLE IF EXISTS top_product;
DROP TABLE IF EXISTS words;
DROP TABLE IF EXISTS temp_word;
DROP TABLE IF EXISTS word_list;


