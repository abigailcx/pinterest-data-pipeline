import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from kafka import KafkaProducer
from kafka.errors import KafkaError

random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        with engine.connect() as conn:
            selected_row = conn.execute(text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1"))
            # .mappings() is a wrapper for a Result that returns dict values rather than row values
            result = dict(selected_row.mappings().all()[0])
            producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: json.dumps(m).encode('ascii'))
            future = producer.send('PinterestTopic1', result)

            record_metadata = future.get(timeout=10)

            # Successful result returns assigned partition and offset
            print (record_metadata.topic)
            print (record_metadata.partition)
            print (record_metadata.offset)
            # requests.post("http://localhost:9092/pin/", json=result)
            # print(result)

# def run_infinite_post_data_loop():
#     while True:
#         sleep(random.randrange(0, 2))
#         random_row = random.randint(0, 11000)
#         engine = new_connector.create_db_connector()
#         with engine.connect() as conn:
#             sql_statement = f"SELECT * FROM pinterest_data LIMIT {random_row}, 1"
#             selected_row = conn.execute(text(sql_statement))
#             print(f"selected row: {selected_row}")
#             for row in selected_row:
#                 print(f"row: {row}")
#                 result = dict(row)
#                 requests.post("http://localhost:8000/pin/", json=result)
#                 print(result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


