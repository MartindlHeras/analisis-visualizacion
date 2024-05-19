import kafka

if __name__ == '__main__':
    consumer = kafka.KafkaConsumer('first_topic', bootstrap_servers=['localhost:9092'])
    for msg in consumer:
        print(msg)