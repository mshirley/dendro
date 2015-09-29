from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import ujson as json

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

sc = SparkContext(appName="dendro")

#sqlContext = SQLContext(sc)
#df = sqlContext.read.json("/home/mshirley/src/dendro/data/test.json")

ssc = StreamingContext(sc, 1)

zkQuorum = "localhost"
topic = "dendro"
kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
lines = kvs.map(lambda x: x[1])

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        sqlContext = getSqlContextInstance(rdd.context)
        
        lineDf = sqlContext.jsonRDD(rdd)
        #print lineDf.select(lineDf["balance"]).count()
        print lineDf.count()
        
        #lineDf.write.parquet('/home/mshirley/src/dendro/data/test1_parquet')
        
        #print lineDf.first()
        #lineDf.registerTempTable("lines")
        
        #lineDf = sqlContext.sql("select * from lines")
        #print lineDf.show()
        #print type(lineDf)

        # Register as table
        #wordsDataFrame.registerTempTable("words")

        # Do word count on table using SQL and print it
        #wordCountsDataFrame = sqlContext.sql("select word, count(*) as total from words group by word")
        #wordCountsDataFrame.show()
    except:
        pass

lines.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
