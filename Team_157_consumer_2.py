#!/usr/bin/env python3
from kafka import KafkaConsumer
import json
import csv

# Configuration
BROKER_IP = '172.25.33.31:9092'  # Replace with actual broker IP
TOPICS = ['topic-net', 'topic-disk']
GROUP_ID = 'consumer2-group'

# Output files
NET_FILE = 'net_data.csv'
DISK_FILE = 'disk_data.csv'

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[BROKER_IP],
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print(f"Consumer 2 connected to broker at {BROKER_IP}")
print(f"Subscribed to topics: {TOPICS}")
print("Waiting for messages...")

# Prepare CSV files
net_file = open(NET_FILE, 'w', newline='')
disk_file = open(DISK_FILE, 'w', newline='')

net_writer = csv.writer(net_file)
disk_writer = csv.writer(disk_file)

# Write headers
net_writer.writerow(['ts', 'server_id', 'net_in', 'net_out'])
disk_writer.writerow(['ts', 'server_id', 'disk_io'])

message_count = 0

try:
    for message in consumer:
        topic = message.topic
        value = message.value

        if topic == 'topic-net':
            net_writer.writerow([
                value['ts'],
                value['server_id'],
                value['net_in'],
                value['net_out']
            ])
            net_file.flush()

        elif topic == 'topic-disk':
            disk_writer.writerow([
                value['ts'],
                value['server_id'],
                value['disk_io']
            ])
            disk_file.flush()

        message_count += 1
        if message_count % 100 == 0:
            print(f"Received {message_count} messages...")

except KeyboardInterrupt:
    print("\nConsumer interrupted by user")

finally:
    net_file.close()
    disk_file.close()
    consumer.close()
    print(f"Total messages received: {message_count}")
    print(f"Data saved to {NET_FILE} and {DISK_FILE}")

