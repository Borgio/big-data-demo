from kafka import KafkaConsumer

kafka_server = "localhost:2181"

if __name__ == "__main__":
    consumer = KafkaConsumer('topic', bootstrap_servers=kafka_server)