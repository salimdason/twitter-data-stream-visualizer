import tweepy
from pykafka import KafkaClient
import json
import os
import auth

"""Auth credentials"""
API_KEY=os.environ.get('API_KEY')
API_SECRET_KEY=os.environ.get('API_SECRET_KEY')
ACCESS_TOKEN=os.environ.get('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET=os.environ.get('ACCESS_TOKEN_SECRET')


def get_kafka_client():
    return KafkaClient(hosts='127.0.0.1:9092')


class MyListener(tweepy.Stream):
    def on_data(self, data):
        try:
            print(data)
            message = json.loads(data)
            if message['place'] is not None:
                client = get_kafka_client()
                topic = client.topics['dasonstream']
                producer = topic.get_sync_producer
                producer.produce(data.encode('ascii'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


if __name__ == "__main__":
    stream = MyListener(auth.API_KEY, auth.API_SECRET_KEY, auth.ACCESS_TOKEN, auth.ACCESS_TOKEN_SECRET)
    # stream.filter(locations=[-180,-90,180,90])
    stream.filter(track=['maguire'])
