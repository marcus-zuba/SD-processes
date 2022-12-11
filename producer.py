import database
import s3
import os
import json
import socket
import time
from confluent_kafka import Producer

def make_json(filename):
    data = {}
    data["filename"] = filename
    data["data"] = []
    with open(filename, 'r') as f:
        for row in f:
            row = row.split(' ')
            row_dic = {} 
            row_dic["id"] = int(row[0])
            row_dic["att1"] = row[1]
            row_dic["att2"] = int(row[2])
            row_dic["att3"] = int(row[3])
            row_dic["att4"] = int(row[4])
            data["data"].append(row_dic)
    return json.dumps(data, indent=2)

def process_file(filename):
    print(f"Producer handling {filename}")
    
    print(f"Producer downloading {filename} from S3 Bucket")
    s3.download_file_to_process(filename)
    
    print(f"Producer processing {filename}")
    data_json = make_json(filename)

    print(f"Producer publishing {filename} data json to KAFKA")
    producer.produce('files_data', value=data_json.encode('utf-8'))
    producer.flush()

    print(f"Producer updating {filename} processing status to CONSUMER_QUEUE")
    database.update_processing_status_of_filename(filename, "CONSUMER_QUEUE")

    print(f"Producer deleting {filename} downloaded from S3\n")
    os.remove(filename)


conf = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}

producer = Producer(conf)

keep_running = False

while True:
    filename = database.get_filename_with_processing_status("PRODUCER_QUEUE")

    if filename:
        process_file(filename)

    elif not keep_running:
        print("No files with processing status PRODUCER_QUEUE found and keep_running=False, so exiting Producer")
        break

    else:
        print("Producer waiting 5 seconds for more files to process")
        time.sleep(5)