import asyncio
import json
import websockets
from confluent_kafka import Producer

# Note: We use localhost:19092 because that's what your docker ps shows!
conf = {'bootstrap.servers': 'localhost:19092'}
p = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Moola Sent: {msg.topic()} [{msg.partition()}]')

async def stream_prices():
    uri = "wss://ws-feed.exchange.coinbase.com"
    async with websockets.connect(uri) as websocket:
        subscribe_msg = {
            "type": "subscribe",
            "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]
        }
        await websocket.send(json.dumps(subscribe_msg))

        while True:
            data = await websocket.recv()
            data_json = json.loads(data)
            if 'price' in data_json:
                price = data_json['price']
                print(f"Live BTC Price: ${price}")
                # Push to Redpanda
                p.produce('market-topic', value=json.dumps(data_json), callback=delivery_report)
                p.poll(0)

try:
    asyncio.run(stream_prices())
except KeyboardInterrupt:
    print("Stopping stream...")