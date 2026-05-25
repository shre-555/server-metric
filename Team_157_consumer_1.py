#!/usr/bin/env python3
from kafka import KafkaConsumer
import json
import csv
import signal
import sys

# Configuration
BROKER_IP = '172.25.33.31:9092'  # Replace with actual IP
TOPICS = ['topic-cpu', 'topic-mem']
GROUP_ID = 'consumer1-group'

# Output files
CPU_FILE = 'cpu_data.csv'
MEM_FILE = 'mem_data.csv'

# Global file handles for graceful shutdown
cpu_file = None
mem_file = None
consumer = None

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print("\n\nShutting down gracefully...")
    if cpu_file:
        cpu_file.close()
    if mem_file:
        mem_file.close()
    if consumer:
        consumer.close()
    print(f"Data saved to {CPU_FILE} and {MEM_FILE}")
    sys.exit(0)

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[BROKER_IP],
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,  # Commit every 5 seconds
    session_timeout_ms=30000,
    max_poll_records=500  # Process in batches
)

print(f"Consumer 1 connected to broker at {BROKER_IP}")
print(f"Subscribed to topics: {TOPICS}")
print("Waiting for messages... (Press Ctrl+C to stop)")

# Prepare CSV files
cpu_file = open(CPU_FILE, 'w', newline='')
mem_file = open(MEM_FILE, 'w', newline='')

cpu_writer = csv.writer(cpu_file)
mem_writer = csv.writer(mem_file)

# Write headers
cpu_writer.writerow(['ts', 'server_id', 'cpu_pct'])
mem_writer.writerow(['ts', 'server_id', 'mem_pct'])

message_count = 0
cpu_count = 0
mem_count = 0

try:
    for message in consumer:
        topic = message.topic
        value = message.value
        
        if topic == 'topic-cpu':
            cpu_writer.writerow([
                value['ts'],
                value['server_id'],
                value['cpu_pct']
            ])
            cpu_count += 1
            if cpu_count % 100 == 0:
                cpu_file.flush()
        
        elif topic == 'topic-mem':
            mem_writer.writerow([
                value['ts'],
                value['server_id'],
                value['mem_pct']
            ])
            mem_count += 1
            if mem_count % 100 == 0:
                mem_file.flush()
        
        message_count += 1
        if message_count % 1000 == 0:
            print(f"Received {message_count} messages (CPU: {cpu_count}, MEM: {mem_count})...")

except Exception as e:
    print(f"\nError: {e}")
finally:
    cpu_file.close()
    mem_file.close()
    consumer.close()
    print(f"\nTotal messages received: {message_count}")
    print(f"CPU messages: {cpu_count}")
    print(f"MEM messages: {mem_count}")
    print(f"Data saved to {CPU_FILE} and {MEM_FILE}")

