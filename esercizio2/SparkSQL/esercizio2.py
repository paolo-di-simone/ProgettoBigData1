#!/usr/bin/env python3
"""spark application"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession with the proper configuration
spark = SparkSession.builder.appName("Spark Quality").getOrCreate()

df = spark.read.load(input_filepath, format="csv", sep=",", inferSchema="true", header="true")

df = df.withColumn("Quality", col("HelpfulnessNumerator") / col("HelpfulnessDenominator"))

df = df.na.fill(0, subset=["Quality"])

df = df.groupBy("UserId").mean("Quality")

df = df.withColumnRenamed("avg(Quality)", "MeanQuality")

df = df.orderBy(df["MeanQuality"].desc())

df.show()

df.write.save(output_filepath, format="csv", delimiter="\t")

spark.stop()
