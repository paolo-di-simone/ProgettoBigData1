#!/usr/bin/env python3
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark Sql").getOrCreate()

df  = spark.read.csv("/home/filippo/Documenti/Progetto_Big_Data/Reviews.csv", header  = True).cache()

df.createOrReplaceTempView("reviews")

df_reviews_for_year = spark.sql("SELECT time, productid, count(*) AS number_reviews FROM reviews GROUP BY time, productid")

df_reviews_for_year.createOrReplaceTempView("reviews_for_year")

df_top_product = spark.sql("SELECT time as year, productid, number_reviews FROM (SELECT time, productid, number_reviews, ROW_NUMBER() OVER (PARTITION BY time ORDER BY number_reviews DESC) as rank FROM reviews_for_year) t WHERE rank <= 10")


df_top_product.createOrReplaceTempView("top_product")
df.createOrReplaceTempView("reviews")

df_words = spark.sql(" SELECT top_product.year as year, top_product.productid as id, reviews.text as text FROM top_product JOIN reviews ON (top_product.year = reviews.time and top_product.productid = reviews.productid);")


df_words.createOrReplaceTempView("words")

df_temp_word = spark.sql("SELECT words.year as year, words.id as id , exploded_line.word as word FROM words LATERAL VIEW explode(split(words.text, ' ')) exploded_line AS word")


df_temp_word.createOrReplaceTempView("temp_word")

df_word_list = spark.sql("SELECT temp_word.year as year, temp_word.id as id, temp_word.word as word, count(*) number_word FROM temp_word WHERE length(word) >= 4 GROUP BY temp_word.year, temp_word.id, temp_word.word")


df_word_list.createOrReplaceTempView("word_list")


df_top_words = spark.sql("SELECT year, id ,word, number_word FROM (SELECT year, id, word, number_word, ROW_NUMBER() OVER (PARTITION BY year,id ORDER BY number_word DESC) as rank FROM word_list) t WHERE rank <= 5 ")


df_top_words.createOrReplaceTempView("top_words")
df_top_product.createOrReplaceTempView("top_product")

df_total = spark.sql("SELECT top_product.year, top_product.productid, top_product.number_reviews, top_words.word, top_words.number_word FROM  top_words JOIN top_product ON( top_words.year = top_product.year and top_words.id = top_product.productid)")


df_total.createOrReplaceTempView("total")

df_final_total = spark.sql("SELECT year, productid, number_reviews, collect_list((word, number_word)) AS word_list FROM total GROUP BY year, productid, number_reviews ORDER BY year")