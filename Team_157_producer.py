#!/usr/bin/env python3

from kafka import KafkaProducer
import csv
import time
import json

# Configuration
BROKER_IP = '172.25.33.31:9092'  # Replace with actual broker IP
DATASET_FILE = 'dataset.csv'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[BROKER_IP],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Producer connected to broker at {BROKER_IP}")
print(f"Reading dataset from {DATASET_FILE}")

# Read and publish data
try:
    with open(DATASET_FILE, 'r') as file:
        reader = csv.DictReader(file)
        for row_num, row in enumerate(reader, start=1):
            # Prepare message payload
            message = {
                'ts': row['ts'],
                'server_id': row['server_id'],
                'cpu_pct': float(row['cpu_pct']),
                'mem_pct': float(row['mem_pct']),
                'net_in': float(row['net_in']),
                'net_out': float(row['net_out']),
                'disk_io': float(row['disk_io'])
            }

            # Send to individual topics
            producer.send('topic-cpu', value={
                'ts': message['ts'],
                'server_id': message['server_id'],
                'cpu_pct': message['cpu_pct']
            })
            producer.send('topic-mem', value={
                'ts': message['ts'],
                'server_id': message['server_id'],
                'mem_pct': message['mem_pct']
            })
            producer.send('topic-net', value={
                'ts': message['ts'],
                'server_id': message['server_id'],
                'net_in': message['net_in'],
                'net_out': message['net_out']
            })
            producer.send('topic-disk', value={
                'ts': message['ts'],
                'server_id': message['server_id'],
                'disk_io': message['disk_io']
            })

            # Log progress
            if row_num % 100 == 0:
                print(f"Published {row_num} records...")

            # Simulate real-time streaming (adjust sleep time as needed)
            #time.sleep(0.1) 

    print(f"\nTotal records published: {row_num}")
    print("Flushing producer...")
    producer.flush()
    print("Producer finished successfully!")

except FileNotFoundError:
    print(f"ERROR: {DATASET_FILE} not found!")
except Exception as e:
    print(f"ERROR: {e}")
finally:
    producer.close()
