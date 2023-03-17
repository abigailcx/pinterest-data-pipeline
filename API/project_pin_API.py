from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import json
from json import dumps
from kafka import KafkaProducer

app = FastAPI()



class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str


@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: json.dumps(m).encode('ascii'))
    future = producer.send('PinterestTopic1', data)
    record_metadata = future.get(timeout=10)
    # Successful result returns assigned partition and offset
    print (record_metadata.topic)
    print (record_metadata.partition)
    print (record_metadata.offset)


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8001)
