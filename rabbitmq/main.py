import uvicorn
from fastapi import FastAPI, BackgroundTasks
import pika

app = FastAPI()

RABBITMQ_HOST = "localhost"
QUEUE_NAME = "fila_pika"

def publish_message(message: str):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=message,
        properties=pika.BasicProperties(delivery_mode=2), # Caso se pergunte no futuro "pq 2?" é pras mensagens serem duráveis
    )
    print(f"Published message: {message}")
    connection.close()

def consume_messages():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    def callback(ch, method, properties, body):
        print(f"Received message: {body.decode()}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    print("Waiting messages...")
    channel.start_consuming()

@app.post("/send-message/")
async def send_message(message: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(publish_message, message)
    return {"message": f"Sent message: {message}"}

@app.on_event("startup")
async def start_consumer():
    import threading
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()


uvicorn.run(app)