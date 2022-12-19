from os import environ as env, makedirs
import shutil
from pyspark.sql import SparkSession, functions as f
from dotenv import load_dotenv

load_dotenv()

HOST = env['HOST']
PORT = int(env['PORT'])
AUTH_TOKEN = env['AUTH_TOKEN']
EXCLIDED_WORDS = ['para', 'não', 'mais', 'está', 'dura', 'cara','quem', 'como', '&gt;']


for item in ('./check', './csv'):
    try:
        shutil.rmtree(item)
    except OSError as err:
        print(f'Aviso: {err.strerror}')

spark = SparkSession.builder.appName('TwitterStreaming').getOrCreate()

tweets = spark.readStream\
    .format('socket')\
    .option('host', HOST)\
    .option('port', PORT)\
    .load()

words = tweets.select(
    f.explode(
        f.split(tweets.value, ' ')
    ).alias('word')
)\
    .filter(f.length(f.col('word')) > 3)\
    .filter(f.col('word').isin(EXCLIDED_WORDS) == False)

FILE_PATH = './datalake/csv'

makedirs(FILE_PATH, exist_ok=True)
query = words.writeStream\
    .outputMode('append')\
    .option('encoding', 'utf-8')\
    .format('csv')\
    .option('path', FILE_PATH)\
    .option('checkpointLocation', './check')\
    .start()

query.awaitTermination()
