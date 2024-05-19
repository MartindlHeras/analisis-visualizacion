import kafka
import time
import random

if __name__ == '__main__':
    producer = kafka.KafkaProducer(bootstrap_servers=['localhost:9092'], key_serializer=str.encode, value_serializer=str.encode)
    i = 0
    while True:
        time.sleep(1)
        i += 1
        future = producer.send('first_topic', key='key', value=str(20 + i + random.gauss(0,10)))
    producer.flush()

    record_metadata = future.get(timeout=10)

    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)