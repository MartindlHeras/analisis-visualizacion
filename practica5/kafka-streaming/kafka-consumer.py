import kafka
import statistics

if __name__ == '__main__':
    consumer = kafka.KafkaConsumer('first_topic', bootstrap_servers=['localhost:9092'])
    messages = {}
    sensor_metrics = []
    t0 = 0
    try:
        for msg in consumer:
            if t0 == 0:
                t0 = msg.timestamp
            messages[msg.timestamp] = float(msg.value)
            t0 = min(t0, msg.timestamp)
            if (msg.timestamp - t0) > 10000:
                average = sum(messages.values())/len(messages.values())
                sensor_metrics.append((average, statistics.variance(messages.values())))
                print(f'Average of sensor: {average:.2f}')
                messages = {}
                t0 = msg.timestamp
    except KeyboardInterrupt:
        with open('sensor.txt', 'a') as fp:
            i = 0
            for iteration in sensor_metrics:
                fp.write(f'{i}, {iteration[0]}, {iteration[1]}\n')
                i += 1