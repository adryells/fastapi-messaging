import json

import uvicorn
from fastapi import FastAPI, BackgroundTasks
from confluent_kafka import Producer, Consumer, KafkaError

app = FastAPI()

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "test_topic"

producer = Producer({"bootstrap.servers": KAFKA_BROKER})

consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "fastapi-group",
    "auto.offset.reset": "earliest"
})
consumer.subscribe([KAFKA_TOPIC])

@app.post("/publish/")
async def publish_message(message: dict):
    try:
        producer.produce(
            KAFKA_TOPIC,
            value=json.dumps(message).encode("utf-8")
        )
        producer.flush()
        return {"data": message}
    except Exception as e:
        return {"data": f"error{str(e)}"}


@app.get("/consume/")
async def consume_messages(background_tasks: BackgroundTasks):
    def consume_task():
        messages = []

        while True:
            message = consumer.poll(1) # waitttt 1s
            if message is None:
                break

            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Error: {message.error()}")
                continue

            message_value = json.loads(message.value().decode("utf-8"))
            messages.append(message_value)
            print(f"Received message: {message_value}")
        return messages

    background_tasks.add_task(consume_task)
    return {"status": "Started!"}


uvicorn.run(app)