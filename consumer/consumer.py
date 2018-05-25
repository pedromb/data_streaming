import json
import os

# Spark imports
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark.conf import SparkConf

# Google API imports
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types

from conf import consumer_conf

## Set configurations
aws_region = consumer_conf["aws_region"]
kinesis_stream = consumer_conf["kinesis_stream"]
kinesis_endpoint = consumer_conf["kinesis_endpoint"]
kinesis_app_name = consumer_conf["kinesis_app_name"]

## Where the application should start from
kinesis_initial_position = InitialPositionInStream.LATEST

## Every 15 seconds set a checkpoint to know that the data has been processed
kinesis_checkpoint_interval = 15

## Interval between data ingestion
spark_batch_interval = 15

## To retrieve SQL context on executors
def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

## The data arrives as json strings so we convert to python objects
def convert_json(item):
    return json.loads(item)

## Runs the model to detect the topic of the submission
## This part is missing, to know more about the topic modelling part go to
## github.com/pedromb/pyDMM
def assign_topic(submission):
    return Row(**submission)

## Runs sentiment analysis to retrieve sentiment score
def sentiment_analysis(comment):
    # Create google api client
    client = language.LanguageServiceClient()
    text = comment['text']
    document = types.Document(
        content=text,
        type=enums.Document.Type.PLAIN_TEXT)
    sentiment = client.analyze_sentiment(document).document_sentiment
    comment["sentiment_score"] = sentiment.score
    comment["sentiment_magnitude"] = sentiment.magnitude
    return Row(**comment)

## Process each data point
def process(my_time, rdd):
    count = rdd.count()
    print("=================={}=================== data count {}".format(my_time, count))
    sqlContext = getSqlContextInstance(rdd.context)
    if count > 0:
        submission_rdd = rdd.filter(lambda kv: kv['type'] == 'submission')
        comments_rdd = rdd.filter(lambda kv: kv['type'] == 'comment')
        submission_count = submission_rdd.count()
        comments_count = comments_rdd.count()
        if submission_count > 0:
            submission_rdd = submission_rdd.map(assign_topic)
            submission_df = sqlContext.createDataFrame(submission_rdd)
            submission_df \
                .write \
                .format("com.mongodb.spark.sql.DefaultSource") \
                .mode("append") \
                .option("database", "reddit-stream") \
                .option("collection", "submissions") \
                .save()
        if comments_count > 0:
            comments_rdd = comments_rdd.map(sentiment_analysis)
            comments_df = sqlContext.createDataFrame(comments_rdd)
            comments_df \
                .write \
                .format("com.mongodb.spark.sql.DefaultSource") \
                .mode("append") \
                .option("database", "reddit-stream") \
                .option("collection", "comments") \
                .save()

if __name__ == "__main__":
    try:
        ## Spark Configurations
        conf = SparkConf()
        conf.setAppName(kinesis_app_name)
        conf.setExecutorEnv(pairs=[
            ("AWS_ACCESS_KEY", consumer_conf["AWS_ACCESS_KEY"]),
            ("AWS_SECRET_KEY", consumer_conf["AWS_SECRET_KEY"]),
            ("AWS_ACCESS_KEY_ID", consumer_conf["AWS_ACCESS_KEY_ID"]),
            ("AWS_SECRET_ACCESS_KEY", consumer_conf["AWS_SECRET_ACCESS_KEY"]),
            ("GOOGLE_APPLICATION_CREDENTIALS", os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        ])
        conf.set("spark.mongodb.output.uri", consumer_conf["MONGO_CONNECTION_STRING"])

        spark_session = SparkSession.builder.config(conf=conf).getOrCreate()
        spark_context = spark_session.sparkContext

        ## Streaming context
        spark_streaming_context = StreamingContext(
            spark_context, spark_batch_interval)
        
        sql_context = SQLContext(spark_context)
        #gsdmm = spark_context.broadcast(model)

        ## Create Kinesis Stream
        kinesis_stream = KinesisUtils.createStream(
            spark_streaming_context, kinesis_app_name, kinesis_stream, kinesis_endpoint,
            aws_region, kinesis_initial_position, kinesis_checkpoint_interval
        )
        
        ## Convert strings to objects
        myrdd = kinesis_stream.map(convert_json)
        
        ## Process entry data point
        myrdd.foreachRDD(process)

        ## Start process and awaits
        spark_streaming_context.start()
        spark_streaming_context.awaitTermination()
        spark_streaming_context.stop()
    except Exception as e:
        print(e)
        pass


