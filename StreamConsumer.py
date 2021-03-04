from kafka import KafkaConsumer
import json
from textblob import TextBlob
from elasticsearch import Elasticsearch
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import SentimentAnalyzer


# def load_json(line):
#     return json.loads(line)

def store_elastic(line):
    es = Elasticsearch()
    es.index(index="coronavirus",
             doc_type="test-type",
             body={
                 "filtered_message": str(line[0]),
                 "sentiment": line[1],
                 "hashtag": "#coronavirus"
             })


def analyze(time, rdd):
    #rdd.foreach(lambda rd: print(rd))
    #jr = rdd.map(load_json)
    # if line[1] else {}
    #dict_data =
    tr = rdd.map(lambda line: TextBlob(line))

    #(line["text"]))
    #tweet = )
    # print(tweet)
    atw = tr.map(lambda line: SentimentAnalyzer.analyze(line))
    # add text and sentiment info to elasticsearch

    # atw.map(lambda line:

    #"author": line["user"]["screen_name"],
    #"date": line["created_at"],
    #"message": line["text"],
    #

    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    print('\n')
    #atw.collect()
    #val = rdd.value()
    atw.foreach(lambda line : print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$  sentence : ' + str(line[0]) + ' label : ' + line[1]))
    #atw.saveAsTextFile('senti.txt')
    #rdd.map(lambda line: print(line))
    print('\n')
    #df = rdd.toDF()
    #df.show()
    #print('RDD: &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& ::::: ')
    #print(val)
    print('\n')
    print('###############################################################################################################')
    print('\n')

    atw.foreach(store_elastic)


def main():
    '''
    main function initiates a kafka consumer, initialize the tweetdata database.
    Consumer consumes tweets from producer extracts features, cleanses the tweet text,
    calculates sentiments and loads the data into postgres database
    '''
    # set-up a Kafka consumer

    sc = SparkContext("local[2]", "NetworkWordCount")
    ssc = StreamingContext(sc, 30)
    topic = 'coronavirus'

   # sa = SentimentAnalyzer()

    consumer = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': 'localhost:9092'})

    #consumer.map(lambda line: analyze(line))

    # consumer.pprint()

    rdds = consumer.map(lambda x : x[1])

    rdds.foreachRDD(analyze)

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()