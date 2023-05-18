CREATE TABLE reviews(id string, productId string, userId string, profileName string, helpfulnessNumerator int, helpfulnessDenominator int, score int, time int, summary string, text string) 
row format delimited fields terminated by ',';


LOAD DATA LOCAL INPATH "/home/filippo/Documenti/Progetto_Big_Data/Reviews.csv" OVERWRITE INTO TABLE reviews;


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


CREATE TABLE top_words AS 

SELECT *
FROM (SELECT year, id, word, number_word, ROW_NUMBER() OVER (PARTITION BY year,id ORDER BY number_word DESC) as rank FROM word_list) t 
WHERE rank <= 5 ;





