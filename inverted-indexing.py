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
        processFile('hdfs://ec2-18-204-83-229.compute-1.amazonaws.com:9000/'+fname, sc, listOfRDD)
 
    print(listOfRDD)
    concatRDD = sc.union(listOfRDD) \
    .map(lambda x: (x[0], [x[1]])) \
    .reduceByKey(lambda x, y: x+y) \
    .map(lambda x: (x[0], sorted(x[1])))
    

    word_id = concatRDD.map(lambda x: x[0]).zipWithIndex()
    word_map  = word_id.collectAsMap()

    wordIdConcatRDD = concatRDD.map(lambda x: (word_map[x[0]], x[1])) \
                      .sortByKey() 

    

    #Transform to Dataframe
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(wordIdConcatRDD) 
            
    df.withColumn("doc_id", df["_2"].cast("string")) \
      .select("_1", "doc_id") \
      .write.csv(mode='overwrite',path='./output/reverse_index.txt')


if __name__ == '__main__':
    """
    Setting up Spark session and Spark context, AWS access key
    """

    spark = SparkSession.builder \
        .appName("inverse-indexing") \
        .getOrCreate()

    sc=spark.sparkContext

    main(sc, sys.argv[1:])
