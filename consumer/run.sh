# To run the Spark Streaming job
spark-submit --deploy-mode client --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.3.0,org.mongodb.spark:mongo-spark-connector_2.11:2.2.2 consumer.py
