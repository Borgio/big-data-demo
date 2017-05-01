from hdfs import InsecureClient
from kafka import KafkaConsumer
import json

client = InsecureClient('http://192.168.186.135:50070', user='training')
print(client.list('/'))
client.makedirs('/user/hdfs/reddit-comments/google/')
kafka_consumer = KafkaConsumer("comments", bootstrap_servers='7.11.230.242:9092')
for msg in kafka_consumer:
	print('saving to hdfs...')
	comment=json.loads(msg.value)
	client.write('/user/hdfs/reddit-comments/google/' + comment['id'] + '.json', data=msg.value, overwrite=True)
	print('saved.')
