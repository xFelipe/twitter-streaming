from os import environ as env
from pyspark.sql import SparkSession, functions as f
from dotenv import load_dotenv

load_dotenv()

HOST = env['HOST']
PORT = int(env['PORT'])
AUTH_TOKEN = env['AUTH_TOKEN']
EXCLIDED_WORDS = ['para', 'não', 'mais', 'está', 'dura', 'cara','quem', 'como', '&gt;']


spark = SparkSession.builder.appName('TwitterStreaming').getOrCreate()

lines = spark.readStream\
    .format('socket')\
    .option('host', HOST)\
    .option('port', PORT)\
    .load()

words = lines.select(
    f.explode(
        f.split(lines.value, ' ')
    ).alias('word')
)\
    .filter(f.length(f.col('word')) > 3)\
    .filter(f.col('word').isin(EXCLIDED_WORDS) == False)

word_counts = words.groupBy('word').count().sort(f.col("count").desc())

# query = lines.writeStream\
query = word_counts.writeStream\
    .outputMode('complete')\
    .format('console')\
    .start()

query.awaitTermination()
