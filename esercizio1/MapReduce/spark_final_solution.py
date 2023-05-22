#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession 
from collections import Counter


my_path ="/home/filippo/Documenti/Progetto_Big_Data/Reviews.csv"

output_path = "/home/filippo/Documenti/Risultati_progetto_Big_Data/spark.txt"


#-----------------------------def funzioni ausiliari------------------------------#

def parse_line(line):
    fields = line.strip().split(",")
    key = str(fields[1])+"-"+str(fields[7])
    return (key, 1)


def parse_line_bis(line):
    fields = line.strip().split(",")
    key = str(fields[1])+"-"+str(fields[7])
    word = fields[9].split(" ")
    for elem in word:
        return (key, elem)
    

def get_top_10_reviews(iterable):
    top_10 = sorted(iterable, key=lambda x: x[1], reverse=True)[:10]
    return top_10


def get_top_words(line):
    list = []
    for elem in line[1]:
        if len(elem) >= 4:
            list.append(elem)
    word_counts = Counter(list)
    sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
    return (line[0], sorted_words[:5])

#-------------------------------------------TOP 10 PRODUCT PER RECENSIONI PER OGNI ANNO ---------------------------------------------------------------#

spark = SparkSession.builder.appName("job1").getOrCreate()


csv_file = spark.sparkContext.textFile(my_path)

header = csv_file.first()
data_lines = csv_file.filter(lambda line: line != header)

id_year = data_lines.map(lambda line: parse_line(line))

group_by_year_id = id_year.reduceByKey(func=lambda a,b : a + b)

year_reviewsRDD = group_by_year_id.map(lambda x: (x[0].split("-")[1], (x[0].split("-")[0], x[1])))

groupedRDD = year_reviewsRDD.groupByKey()

top_10_reviewsRDD = groupedRDD.mapValues(get_top_10_reviews)

results = top_10_reviewsRDD.collect()


for year, top_10 in results:
    print("Year:", year)
    for id_count in top_10:
        print("ID:", id_count[0], "Count:", id_count[1])
    print()

#--------------------------------------------TOP 5 PAROLE PER PRODOTTO E ANNO -------------------------------------------------------------------#

id_year_text = data_lines.map(lambda line:( line.split(",")[1], line.split(",")[7], line.split(",")[9].split()) )

id_year_list = id_year_text.map(lambda line:(line[0]+"-"+line[1], line[2]))

id_year = id_year_list.reduceByKey(lambda a,b: a+b) 

result = id_year.map(lambda x: get_top_words(x)) 

result_rdd = top_10_reviewsRDD.flatMap(lambda x: [(f"{element[0]}-{x[0]}", element[1]) for element in x[1]])
joinRDD= result_rdd.join(result)

final_RDD_temp = joinRDD.map(lambda x:(x[0].split("-")[1], x[0].split("-")[0], x[1]))

final_RDD = final_RDD_temp.sortBy(lambda x: x[0])

for elem in final_RDD.collect():
    print(elem)


joinRDD.saveAsTextFile(output_path)
#-----------------------------------------------------------------------------------------------------------------------------------#

#output_strings_RDD = joinRDD.map(lambda x: "%s\t%f" % (x[0], x[1]))

#sorted_RDD = user_id_mean_quality_RDD.sortBy(lambda x: x[1], ascending=False)