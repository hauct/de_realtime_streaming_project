from datetime import datetime
import requests
import json
import time
import logging
from kafka import KafkaProducer

def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res):
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(res['location']['street']['number'])} {res['location']['street']['name']}, " \
                    f"{res['location']['city']}, {res['location']['state']}, {res['location']['country']}"
    data['post_code'] = res['location']['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

def stream_data():
    res = get_data()
    res = format(res)

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)

    producer.send('user_created', json.dumps(res).encode('utf-8'))

stream_data()