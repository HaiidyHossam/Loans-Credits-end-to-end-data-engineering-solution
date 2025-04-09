import json
import time
import math
from kafka import KafkaProducer
import pandas as pd

csv_path = "/home/haidy/final_selected_data.csv"  
df = pd.read_csv(csv_path)

#nan to none
def replace_nan_with_none(obj):
    if isinstance(obj, dict):
        return {k: replace_nan_with_none(v) for k, v in obj.items()}
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    else:
        return obj

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "loan_topic_v2"

for _, row in df.iterrows():
    raw_message = row.to_dict()
    clean_message = replace_nan_with_none(raw_message)
    producer.send(topic, value=clean_message)
    print("✔️ Sent:", clean_message)
    time.sleep(2)
