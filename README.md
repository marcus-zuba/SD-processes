# SD-processes
Repository to include code of processes team from SD subject. 

## Running
Install dependencies
```
pip install -r requirements.txt
```

Setup kafka:
```
docker-compose up -d
```

Setup local database:
```
python database.py
```

⚠️ **Warning**: For running producer and consumer below, you need to have AWS Credentials setup in your local machine. Learn how to do that [here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration).

Run producer:
```
python producer.py
```

Run consumer:
```
python consumer.py
```


