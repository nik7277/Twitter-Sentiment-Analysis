from textblob import TextBlob
import nltk
nltk.download('stopwords')
nltk.download('punkt')
from nltk.corpus import stopwords


stop_words = set(stopwords.words('english'))

def analyze(tweet):

    sub_stopwords = set(tweet.words) - stop_words

    filteredstring = ' '
    for word in sub_stopwords:
        filteredstring = filteredstring + word + ' '

    sentence = TextBlob(filteredstring).correct()

    if(sentence.sentiment.polarity>0.2 and sentence.sentiment.subjectivity <0.7):
        label = 'positive'

    elif(sentence.sentiment.polarity<-0.2 and sentence.sentiment.subjectivity <0.7):
        label = 'negative'
    else:
        label = 'neutral'

    return (sentence,label)

        #  topic = 'wwe'
        #
        #  consumer = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': 'localhost:9092'})
        #
        #  for msg in consumer:
        #     dict_data = json.loads(msg.value)
        #     tweet = TextBlob(dict_data["text"])
        #     print(tweet)
        #     # add text and sentiment info to elasticsearch
        #     es.index(index="weet",
        #              doc_type="test-type",
        #              body={"author": dict_data["user"]["screen_name"],
        #                    "date": dict_data["created_at"],
        #                    "message": dict_data["text"]})
        #     print('\n')
