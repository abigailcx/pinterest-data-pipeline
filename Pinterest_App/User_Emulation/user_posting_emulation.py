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
        print(f"random row: {random_row}")
        engine = new_connector.create_db_connector()
        with engine.connect() as conn:
            selected_row = conn.execute(text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1"))
            # .mappings() is a wrapper for a Result that returns dict values rather than row values
            result = dict(selected_row.mappings().all()[0])
            requests.post("http://localhost:8001/pin/", json=result)
            print(result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


