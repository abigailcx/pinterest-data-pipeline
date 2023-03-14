from kafka import KafkaConsumer
import json

topic = 'PinterestTopic1'
bootstrap_servers = 'localhost:9092'
value_deserializer = lambda m: json.loads(m.decode('ascii'))
batch_consumer = KafkaConsumer(topic, 
                               bootstrap_servers=bootstrap_servers, 
                               value_deserializer=value_deserializer)

for message in batch_consumer:
    print(message.topic)
    print(message.key)
    print(message.value)
