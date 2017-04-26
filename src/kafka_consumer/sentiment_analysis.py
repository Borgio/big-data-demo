from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark import SparkFiles
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import string
import json
import itertools

import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer

nltk.download('punkt')
PUNCTUATION = set(string.punctuation)
STOPWORDS = set(stopwords.words('english'))

def tokenize(text):
    tokens = word_tokenize(text)
    lowercased = [t.lower() for t in tokens]
    no_punctuation = []
    for word in lowercased:
        punct_removed = ''.join([letter for letter in word if not letter in PUNCTUATION])
        no_punctuation.append(punct_removed)
    no_stopwords = [w for w in no_punctuation if not w in STOPWORDS]
    return [w for w in no_stopwords if w]

def tokenize_tweet(tweet):
	return (tweet, tokenize(tweet))

def score_tokens(tokens, word_list):
	if len(tokens) == 0:
		return 0
	matches = [t for t in tokens if t in word_list]
	score = float(len(matches)) / float(len(tokens))
	return score

def score_tweet(pair, positive_words, negative_words):
	tweet = pair[0]
	tokens = pair[1]
	pos_score = score_tokens(tokens, positive_words)
	neg_score = score_tokens(tokens, negative_words)
	if pos_score == neg_score:
		return (tweet, 0)
	elif pos_score > neg_score:
		return (tweet, pos_score)
	else:
		return (tweet, -1.0 * neg_score)

def score_tweets(iterator):
    positive_words = [line.strip() for line in open(SparkFiles.get("pos-words.txt"))]
    negative_words = [line.strip() for line in open(SparkFiles.get("neg-words.txt"))]
    return itertools.imap(lambda x: score_tweet(x, positive_words, negative_words), iterator)

#def ziptogether(Dstream):
#    Zipped=Dstream.zip(Topic)
#    return Zipped

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")

    sc.addFile("/home/zhenchang/Downloads/pos-words.txt")
    sc.addFile("/home/zhenchang/Downloads/neg-words.txt")

    ssc = StreamingContext(sc, 5)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})

    # parse json structure
    vs = kvs.map(lambda entry : entry[1]).map(json.loads)

    # filter out comments without 'body' and extract 'subreddit' as topic
    Topic = vs.filter(lambda x: 'body' in x).map(lambda x: x['subreddit'])

    # score the comment
    Scored_PostedComment = vs.filter(lambda x: 'body' in x).map(lambda x: x['body'])\
        .map(tokenize_tweet).mapPartitions(score_tweets)

    # extract the score
    Score = Scored_PostedComment.map(lambda x: x[1])

    #Scored_PostedComment.pprint()

    #Topic.pprint()
    Score.pprint()
    aaa=Score.union(Topic) # no error reported, but no effect either
    # aaa.pprint()

    #bbb=Score.transform(lambda rdd: rdd.append(Topic))
    #bbb.pprint()

    # zip back with topic-level
    #Paired = Score.foreachRDD(lambda rdd: rdd.foreach(zip(Topic).SortByKey(False))
    #Paired.pprint()
    #(Topic).SortByKey(False)
    #Paired.take(5)

    ssc.start()
    ssc.awaitTermination()