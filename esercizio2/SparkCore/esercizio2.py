#!/usr/bin/env python3

"""spark application"""
import argparse
from pyspark.sql import SparkSession

def make_quality(helpfulness_numerator, helpfulness_denominator):
    try:
        helpfulness_numerator = int(helpfulness_numerator)
        helpfulness_denominator = int(helpfulness_denominator)
        if helpfulness_denominator != 0:
            utility = helpfulness_numerator / helpfulness_denominator
        else:
            # division by zero
            # continue
            utility = 0
    except ValueError:
	    # if something was not a number ignore/discard this line
        # continue
        utility = 0

    return utility
    
def parse_line(line):
    fields = line.strip().split(",")
    return (fields[2], make_quality(fields[4], fields[5]))
    
def compute_mean(count_sum, value):
    count, sum = count_sum
    return (count + 1, sum + value)

def combine_means(mean1, mean2):
    count1, sum1 = mean1
    count2, sum2 = mean2
    return (count1 + count2, sum1 + sum2)

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession with the proper configuration
spark = SparkSession.builder.appName("Spark Quality").getOrCreate()

# read the input file and obtain an RDD with a record for each line
lines_RDD = spark.sparkContext.textFile(input_filepath)

# get a new RDD where each record is a python list (fields)
# obtained by parsing a record (csv string) of lines_RDD
fields_RDD = lines_RDD.map(parse_line)

user_id_mean_quality_RDD = fields_RDD \
    .aggregateByKey((0, 0), compute_mean, combine_means) \
    .mapValues(lambda v: v[1]/v[0]) \
    .sortBy(lambda x: x[1], ascending=False)

# Transform the user_id_mean_quality_sorted_RDD into an RDD of strings
output_strings_RDD = user_id_mean_quality_RDD.map(f=lambda pair: "%s\t%f" % (pair[0], pair[1]))

# this final action saves the RDD of strings as a new folder in the FS
output_strings_RDD.saveAsTextFile(output_filepath)









