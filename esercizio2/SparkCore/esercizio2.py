#!/usr/bin/env python3

"""spark application"""
from pyspark.sql import SparkSession
import sys
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

start_time = time.time()

def make_quality(helpfulness_numerator, helpfulness_denominator):
    try:
        helpfulness_numerator = int(helpfulness_numerator)
        helpfulness_denominator = int(helpfulness_denominator)
        if helpfulness_denominator != 0:
            utility = helpfulness_numerator / helpfulness_denominator
        else:
            utility = 0
    except ValueError:
        utility = 0

    return utility

input_filepath = "file:///home/paolods/Desktop/ProgettoBigData/Dataset/Reviews-Parsed.csv"
output_filepath = "file:///home/paolods/Desktop/ProgettoBigData/esercizio2/SparkCore/dir0"

# initialize SparkSession with the proper configuration
spark = SparkSession.builder.appName("Spark Core Quality").getOrCreate()

# read the input file and obtain an RDD with a record for each line
lines_RDD = spark.sparkContext.textFile(input_filepath)

# get a new RDD where each record is a python list (fields)
# obtained by parsing a record (csv string) of lines_RDD
fields_RDD = lines_RDD.map(lambda line: line.strip().split(","))

user_id_quality_RDD = fields_RDD.map(lambda fields: (fields[2], (make_quality(fields[4], fields[5]), 1)))

tmp_RDD = user_id_quality_RDD.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))

user_id_mean_quality_RDD = tmp_RDD.map(lambda tmp: (tmp[0], tmp[1][0]/tmp[1][1]))

sorted_RDD = user_id_mean_quality_RDD.sortBy(lambda x: x[1], ascending=False)

output_strings_RDD = sorted_RDD.map(lambda x: "%s\t%f" % (x[0], x[1]))

output_strings_RDD.saveAsTextFile(output_filepath)

# sorted_RDD.foreach(print)

end_time = time.time()
execution_time = end_time - start_time

logger.info("TEMPO spark core: %s s", execution_time)

spark.stop()
