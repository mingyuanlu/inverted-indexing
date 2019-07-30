import sys
import os
from pyspark.sql import Row
from pyspark.sql import SparkSession, SQLContext, Row
import configparser
from pyspark.sql.functions import udf, col, explode, avg, count, max, min, collect_list
from pyspark.sql.types import StringType, ArrayType, FloatType, IntegerType
import numpy as np
import string


'''
def cleanWords(word):
    return word.encode('utf','ignore').lower().translate(str.maketrans('', '', string.punctuation))
'''
def cleanWords(words):
    #print(words)
    words = words.encode('utf-8','ignore')
    words = words.decode('utf-8','ignore')
    words = words.lower()
    #print('64 %s')%(words)
    cleaned_words = words.translate(str.maketrans('', '', string.punctuation))
    #print('66 %s') % (cleaned_words)
    return cleaned_words

def processFile(filename, sc, rddList):
    text_file = sc.textFile(filename)
    words_rdd = text_file.flatMap(lambda line: cleanWords(line).split())\
                    .filter(lambda x: len(x) > 0)
    words_rdd = words_rdd.map(lambda x: (x, os.path.basename(filename))).distinct()
    #words_rdd = words_rdd.map(lambda x: (x[0], [x[1]]))

    rddList.append(words_rdd)

def main(sc, filenames):

    #Obtainb list of top new src used for filtering
    #src_file = os.environ['SRC_LIST_FILE']
    #src_list = f.read_src_file(src_file)
    #filenames = ['file1.txt', 'file2.txt', ...]
    #outFileName = 'concant.txt'
    #totalTermDoc = []
    #with open(outFileName, 'w') as outfile:
    listOfRDD = []

    for fname in filenames:
        print(fname)
        #fname = '/user/'+os.path.basename(fname)
        #with open(fname) as infile:
        #infileRDD = sc.textFile('hdfs://ec2-18-204-83-229.compute-1.amazonaws.com:9000/user/0')
        #infileRDD = sc.textFile('hdfs://ec2-18-204-83-229.compute-1.amazonaws.com:9000'+fname)
      
        #infileRDD.map(lambda x: cleanWord(x).split(' ')) \
        #processFile('hdfs://ec2-18-204-83-229.compute-1.amazonaws.com:9000/user/0', sc, listOfRDD)
        processFile('hdfs://ec2-18-204-83-229.compute-1.amazonaws.com:9000/'+fname, sc, listOfRDD)
 
        '''
        infileRDD.map(lambda x: x.lower()) \
        .map(lambda x: x.encode('utf','ignore')) \
        .map(lambda x: x.split(' ')) \
        .filter(lambda x: len(x)>0) \
        .map(lambda x: (x, fname))
        print(infileRDD.collect())
        listOfRDD.append(infileRDD)
        '''
        #totalTermDoc.append( getTermDoc(infile)
            #outfile.write(infile.read())
    #concatText = f.read_file(outFileName)

    #concatTextRDD = sc.broadcast(concatText)
    print(listOfRDD)
    concatRDD = sc.union(listOfRDD) \
    .map(lambda x: (x[0], [x[1]])) \
    .reduceByKey(lambda x, y: x+y) \
    .map(lambda x: (x[0], sorted(x[1])))
    
    #concatRDD = concatRDD.map(lambda x: 
    #concatRDD.collect()
    #print(concatRDD.take(10))

    word_id = concatRDD.map(lambda x: x[0]).zipWithIndex()
    word_map  = word_id.collectAsMap()

    wordIdConcatRDD = concatRDD.map(lambda x: (word_map[x[0]], x[1])) \
                      .sortByKey() 
    #print(wordIdConcatRDD.take(100))

    #reducedRDD = wordIdConcatRDD.reduceByKey(lambda x, y: x+y)
    
    print(wordIdConcatRDD.take(100))
    # print(reducedRDD.take(100))

    #Transform to Dataframe
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(wordIdConcatRDD) 
            
    #print(df.head())
    df.withColumn("doc_id", df["_2"].cast("string")) \
      .select("_1", "doc_id") \
      .write.csv(mode='overwrite',path='./output/reverse_index.txt')

    '''
    concatTextRDD.map(lambda x: x.encode("utf", "ignore")) \
                 .map(lambda x: x.split(' ')) \
                 .filter(lambda x: not(x == ''))


    #Read "mentions" table from GDELT S3 bucket. Transform into RDD
    mentionRDD = sc.textFile('s3a://gdelt-open-data/v2/mentions/201807200000*.mentions.csv')
    mentionRDD = mentionRDD.map(lambda x: x.encode("utf", "ignore"))
    mentionRDD.cache()
    mentionRDD  = mentionRDD.map(lambda x : x.split('\t'))
    mentionRowRDD = mentionRDD.map(lambda x : Row(event_id = x[0],
                                        mention_id = x[5],
                                        mention_doc_tone = float(x[13]),
                                        mention_time_date = transform_to_timestamptz(x[2]),
                                        event_time_date = x[1],
                                        mention_src_name = x[4]))


    #Read 'GKG" table from GDELT S3 bucket. Transform into RDD
    gkgRDD = sc.textFile('s3a://gdelt-open-data/v2/gkg/201807200000*.gkg.csv')
    gkgRDD = gkgRDD.map(lambda x: x.encode("utf", "ignore"))
    gkgRDD.cache()
    gkgRDD = gkgRDD.map(lambda x: x.split('\t'))
    gkgRowRDD = gkgRDD.map(lambda x : Row(src_common_name = x[3],
                                        doc_id = x[4],
                                        themes = x[7].split(';')[:-1]
                                        ))
    '''

    '''
    sqlContext = SQLContext(sc)

    #Transform RDDs to dataframes
    mentionDF = sqlContext.createDataFrame(mentionRowRDD)
    gkgDF     = sqlContext.createDataFrame(gkgRowRDD)

    sqlContext.registerDataFrameAsTable(mentionDF, 'temp1')
    sqlContext.registerDataFrameAsTable(gkgDF, 'temp2')


    df1 = mentionDF.alias('df1')
    df2 = gkgDF.alias('df2')

    #Themes and tones information are stored in two different tables
    joinedDF = df1.join(df2, df1.mention_id == df2.doc_id, "inner").select('df1.*'
                                                , 'df2.src_common_name','df2.themes')

    #Each document could contain multiple themes. Explode on the themes and make a new column
    explodedDF = joinedDF.select('event_id', 'mention_id', 'mention_doc_tone'
                                                , 'mention_time_date', 'event_time_date'
                                                , 'mention_src_name', 'src_common_name'
                                                , explode(joinedDF.themes).alias("theme"))



    hist_data_udf = udf(hist_data, ArrayType(IntegerType()))
    get_quantile_udf = udf(get_quantile, ArrayType(FloatType()))

    #Compute statistics for each theme at a time
    testDF = explodedDF.groupBy('theme', 'mention_time_date').agg(
            count('*').alias('num_mentions'),
            avg('mention_doc_tone').alias('avg'),
            collect_list('mention_doc_tone').alias('tones')
            )

    #Histogram and compute  quantiles for tones
    histDF = testDF.withColumn("bin_vals", hist_data_udf('tones')) \
                   .withColumn("quantiles", get_quantile_udf('tones'))

    histDF.drop('tones')
    #histDF.show()
    finalDF = histDF.select('theme', 'num_mentions', 'avg', 'quantiles', 'bin_vals', col('mention_time_date').alias('time'))
    finalDF.show()


    #Preparing to write to TimescaleDB
    db_properties = {}
    config = configparser.ConfigParser()
    config.read("db_properties.ini")
    db_prop = config['postgresql']
    db_url = db_prop['url']
    db_properties['username'] = db_prop['username']
    db_properties['password'] = db_prop['password']
    db_properties['url'] = db_prop['url']
    db_properties['driver'] = db_prop['driver']

    #Write to table
    finalDF.write.format("jdbc").options(
    url=db_properties['url'],
    dbtable='bubblebreaker_schema.tones_table',
    user='postgres',
    password='postgres',
    stringtype="unspecified"
    ).mode('append').save()
    '''

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

    #argList = []
    #for i in sys.argv[1:]:

    main(sc, sys.argv[1:])
