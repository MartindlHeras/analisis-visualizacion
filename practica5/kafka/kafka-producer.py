import kafka

if __name__ == '__main__':
    producer = kafka.KafkaProducer(bootstrap_servers=['localhost:9092'], key_serializer=str.encode, value_serializer=str.encode)
    future = producer.send('first_topic', key='key', value='value')
    producer.flush()

    record_metadata = future.get(timeout=10)

    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)