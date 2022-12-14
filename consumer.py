import database
import s3
import socket
import json
import os
from confluent_kafka import Consumer

def process_message(msg):
    data = json.loads(msg.value().decode('utf-8'))
    filename = data["filename"]
    
    print(f"Consumer processing {filename} data")
    with open(filename, "w") as f:
        f.write(json.dumps(data, indent=2))

    print(f"Consumer uploading processed data of {filename} to S3")
    s3.upload_processed_file(filename)

    print(f"Producer updating {filename} processing status to PROCESSED")
    database.update_processing_status_of_filename(filename, "PROCESSED")

    print(f"Consumer deleting local processed data file of {filename}\n")
    os.remove(filename)

conf = {'bootstrap.servers': "localhost:9092",
        'group.id': socket.gethostname(),
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)
consumer.subscribe(["files_data"])

keep_running = False
failed_attempts = 0

print("Consumer awaiting for messages in KAFKA queue")

while True:
    msg = consumer.poll(timeout=0.1)

    if msg:
        failed_attempts = 0
        print("\nConsumer fetched message from KAFKA queue")
        process_message(msg)
        print("Consumer awaiting for messages in KAFKA queue")

    else:
        failed_attempts+=1

    if failed_attempts==100 and not keep_running:
        print("Consumer failed to fetch messages 100 times in a row and keep_running=False, so exiting Consumer")
        exit()