from kafka import KafkaConsumer
import boto3
import json

pinterest_s3_bucket = 'pinterest-data-40dba6ba-1777-41cd-82ea-0ca36620f0b4'

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

    # send data from kafka consumer to S3 bucket
    # create S3 resource
    s3 = boto3.client('s3')
    # define tmp filepath on local machine to store json object
    filename = f"pin_{message.value['index']}"
    filepath = f"/Users/agc/AiCore/pinterest-data-pipeline/tmp/{filename}"
    # convert message in batch consumer to json file
    with open(filepath, "w") as f:
        print(f"file: {f}")
        json.dump(message, f)
    # my_bucket = s3.Bucket('pinterest-data-40dba6ba-1777-41cd-82ea-0ca36620f0b4')
    response = s3.upload_file(filepath, 
                             pinterest_s3_bucket, 
                             f"pins/{filename}")
    
    # result = s3.head_object(Bucket=pinterest_s3_bucket, Key=filepath)
    # print(result)
