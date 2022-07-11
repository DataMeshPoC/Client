from threading import Thread
from queue import Queue

from kafka import KafkaProducer
from kafka import KafkaConsumer

# Define the input and output topics
topic_name_input = "PolicyDraftList"
topic_name_output = "PolicyUWResult"

data = Queue()

def bytes_to_int(bytes):
    result = 0
    for b in bytes:
        result = result * 256 + int(b)
    return result


consumer = KafkaConsumer(
     topic_name_input,
     bootstrap_servers=['pkc-epwny.eastus.azure.confluent.cloud:9092'],
     group_id='my-group',
     value_deserializer=lambda x: bytes_to_int(x)
    )

producer1 = KafkaProducer(
    bootstrap_servers=['pkc-epwny.eastus.azure.confluent.cloud:9092'], value_serializer=lambda x: bytes(x))

def read_topic_data():
    print("received")
    for message in consumer:
        print(message)
        data.put(message.value)

def send_data_to_topic():
    while True:
        print("starting write thread")
        # sorted_data = sorted(data)
        # for d in sorted_data:
        #     producer1.send(topic_name_output, value=d)
        # producer1.flush()
        # time.sleep(10) # wait for 10 seconds before starting next iteration
        producer1.send(topic_name_output, value=data.get())
        producer1.flush()

# Use threads to concurrently read & write to topics
if __name__ == "__main__":
    read_thread = Thread(target=read_topic_data)
    read_thread.start()
    write_thread = Thread(target=send_data_to_topic)
    write_thread.start()
