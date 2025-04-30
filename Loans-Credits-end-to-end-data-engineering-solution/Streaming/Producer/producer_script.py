from kafka import KafkaProducer
import pandas as pd
import json
import time

# Load your CSV file
df = pd.read_csv("loan_2019_20.csv", low_memory=False)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'loan_data'

# Send only the first 10 rows as JSON messages
for _, row in df.head(1000).iterrows():
    message = row.to_dict()
    producer.send(topic_name, value=message)
    time.sleep(5)  # Optional delay to simulate streaming

producer.flush()
producer.close()
