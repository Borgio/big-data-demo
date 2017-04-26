from hdfs import InsecureClient
from kafka import KafkaConsumer
import json

client = InsecureClient('http://192.168.186.135:50070', user='training')
print(client.list('/'))
kafka_consumer = KafkaConsumer("comments", bootstrap_servers='127.0.0.1:9092')
client.write('test' + '.json', data='test123')
for msg in kafka_consumer:
    print(msg.value)
    model = json.loads(str(msg.value, 'utf-8'))
    client.write('/reddit-comments/' + model['id'] + '.json', data=model)