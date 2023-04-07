from kafka import KafkaConsumer
import boto3
import json

topic = 'PinterestTopic1'
bootstrap_servers = 'localhost:9092'
value_deserializer = lambda m: json.loads(m)
batch_consumer = KafkaConsumer(topic, 
                               bootstrap_servers=bootstrap_servers, 
                               value_deserializer=value_deserializer)


# create S3 resource
s3_resource = boto3.resource('s3')
pinterest_s3_bucket = 'pinterest-data-40dba6ba-1777-41cd-82ea-0ca36620f0b4'
my_bucket = s3_resource.Bucket(pinterest_s3_bucket)

for message in batch_consumer:
    print(message.topic)
    print(message.key)
    print(message.value)

    # send data from kafka consumer to S3 bucket
    pin_filename = f"pin_{message.value['index']}.json"
    json_object = json.dumps(message.value)
    my_bucket.put_object(Key=pin_filename, Body=json_object)
    
    # # define tmp filepath on local machine to store json object
    # filename = f"pin_{message.value['index']}.json"
    # filepath = f"/Users/agc/AiCore/pinterest-data-pipeline/tmp/{filename}"
    # # convert message in batch consumer to json file
    # with open(filepath, "w") as f:
    #     print(f"file: {f}")
    #     json.dump(message, f)
    
    # response = s3.upload_file(filepath, 
    #                          pinterest_s3_bucket, 
    #                          f"posts/{filename}")
    
    # result = s3.head_object(Bucket=pinterest_s3_bucket, Key=filepath)
    # print(result)
