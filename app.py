import websocket
import os
import json
import logging
from kafka import KafkaProducer, producer, KafkaClient
from kafka.admin import KafkaAdminClient, NewTopic

logging.basicConfig(level=logging.NOTSET)
logger = logging.getLogger("stream-app")

topic = 'fin-stream-topic'
bootstrap_server = 'localhost:9092'


def setup_kafka_topic():
    client = KafkaClient(bootstrap_servers=bootstrap_server)
    future = client.cluster.request_update()
    client.poll(future=future)
    metadata = client.cluster
    print(metadata.topics())
    # create topic if not exist
    if topic not in metadata.topics():
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_server,
            api_version=(3, 0, 0),
            client_id='client'
        )
        topic_list = [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)


def setup_kafka_producers():
    producer = KafkaProducer(bootstrap_servers=[bootstrap_server],
                             api_version=(3, 0, 0),
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    return producer


def on_message(ws, message):
    producer.send(topic, message)
    info = f'MSG: {message} sent to TOPIC: {topic}'
    logging.info(info)


def on_error(ws, error):
    print(error)


def on_close(ws):
    print("### closed ###")


def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
    ws.send('{"type":"subscribe","symbol":"IC MARKETS:1"}')


if __name__ == "__main__":
    # setup kafka
    setup_kafka_topic()
    producer = setup_kafka_producers()

    # run app
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=" + os.environ['FINNHUB_API_TOKEN'],
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()
