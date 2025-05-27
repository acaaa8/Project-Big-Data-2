from kafka import KafkaConsumer
import json
import csv
import os
import time

# Konfigurasi
TOPIC = 'youtube-comments'
BOOTSTRAP_SERVERS = ['localhost:9092']
BATCH_SIZE = 100  # Jumlah data per batch
MAX_BATCHES = 3   # Hanya simpan 3 batch CSV
OUTPUT_DIR = 'batches'

# Setup Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Siapkan direktori output
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

print("Kafka Consumer started. Menyimpan hingga 3 batch...")

batch_data = []
batch_count = 0

try:
    for message in consumer:
        batch_data.append(message.value)

        if len(batch_data) >= BATCH_SIZE:
            batch_count += 1
            output_file = os.path.join(OUTPUT_DIR, f"batch{batch_count}.csv")

            with open(output_file, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=batch_data[0].keys())
                writer.writeheader()
                writer.writerows(batch_data)

            print(f"Batch {batch_count} disimpan: {output_file}")

            batch_data = []

            if batch_count >= MAX_BATCHES:
                print("Telah mencapai maksimal 3 batch. Consumer berhenti.")
                break

except KeyboardInterrupt:
    print("Consumer dihentikan manual.")

consumer.close()
