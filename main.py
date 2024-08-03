
# This script will fetch the current price of IBM stock using the Alpha Vantage API and publish it to a Kafka topic 
from kafka import KafkaProducer
import requests
import json
import time

ALPHA_VANTAGE_API_KEY = '3FHND2TZ5CVIWGIN'
SYMBOL = 'IBM'

def get_data():
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=3FHND2TZ5CVIWGIN'
    r = requests.get(url)
    data = r.json()
    return data
if __name__ == '__main__':
    while True:
        stock_data = get_data()
        if 'Time Series (5min)' in stock_data:
            print(f"Current price of IBM stock: {stock_data['Time Series (5min)'][list(stock_data['Time Series (5min)'].keys())[-1]]['4. close']}")
        else:
            print("Failed to fetch stock data.")
        time.sleep(60)

from confluent_kafka import Producer, Producer

conf = {
    'bootstrap.servers':'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
    'security.protocol':'SASL_SSL',
    'sasl.mechanisms':'PLAIN',
    'sasl.username':'FS2XJUVQM72JZRS3',
    'sasl.password':'wToHN73jRLbvDHIs4EzD4+O4u4IZ8vbrZQZehQ05EOqKagjgjx+GaglB6A5K5nA3'
    
    }

producer=Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_stock_data(data):
    producer.produce('TIME_SERIES_INTRADAY', key=SYMBOL, value=json.dumps(data), callback=delivery_report)
    producer.flush()

if __name__ == '__main__':
    while True:
        stock_data = get_data()
        produce_stock_data(stock_data)
        time.sleep(60)









