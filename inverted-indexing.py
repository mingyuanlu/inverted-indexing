import sys
import os
from pyspark.sql import Row
from pyspark.sql import SparkSession, SQLContext, Row
import configparser
from pyspark.sql.functions import udf, col, explode, avg, count, max, min, collect_list
from pyspark.sql.types import StringType, ArrayType, FloatType, IntegerType
import numpy as np
import string

def cleanWords(words):
    words = words.encode('utf-8','ignore')
    words = words.decode('utf-8','ignore')
    words = words.lower()
    cleaned_words = words.translate(str.maketrans('', '', string.punctuation))
    return cleaned_words

def processFile(filename, sc, rddList):
    text_file = sc.textFile(filename)
    words_rdd = text_file.flatMap(lambda line: cleanWords(line).split())\
                    .filter(lambda x: len(x) > 0)
    words_rdd = words_rdd.map(lambda x: (x, os.path.basename(filename))).distinct()

    rddList.append(words_rdd)

def main(sc, filenames):

    listOfRDD = []

    for fname in filenames:
        print(fname)
        fname = '/user/'+os.path.basename(fname)
        processFile('hdfs://ec2-18-204-83-229.compute-1.amazonaws.com:9000'+fname, sc, listOfRDD)
 
    print(listOfRDD)
    concatRDD = sc.union(listOfRDD) \
    .map(lambda x: (x[0], [x[1]])) \
    .reduceByKey(lambda x, y: x+y)

    word_id = concatRDD.map(lambda x: x[0]).zipWithIndex()
    word_map  = word_id.collectAsMap()

    wordIdConcatRDD = concatRDD.map(lambda x: (word_map[x[0]], [x[1]]))
    
    
    print(wordIdConcatRDD.take(100))



if __name__ == '__main__':
    """
    Setting up Spark session and Spark context, AWS access key
    """

    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.aws/credentials'))
    access_id = config.get('default', "aws_access_key_id")
    access_key = config.get('default', "aws_secret_access_key")
    spark = SparkSession.builder \
        .appName("inverse-indexing") \
        .getOrCreate()

    sc=spark.sparkContext
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", access_id)
    hadoop_conf.set("fs.s3a.secret.key", access_key)


    main(sc, sys.argv[1:])
