import pandas as pd
import json
import time
import random
from kafka import KafkaProducer

# Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Baca dataset
df = pd.read_csv('youtube_comments_cleaned.csv')

# Hilangkan baris yang memiliki nilai kosong agar tidak error saat dikirim
df.dropna(subset=['CommentID', 'CommentText', 'VideoID', 'Sentiment', 'Likes', 'Replies'], inplace=True)

# Looping untuk mengirim data
for _, row in df.iterrows():
    data = {
        'CommentID': row['CommentID'],
        'CommentText': row['CommentText'],
        'VideoID': row['VideoID'],
        'Sentiment': row['Sentiment'],
        'Likes': str(row['Likes']),
        'Replies': str(row['Replies'])
    }

    try:
        producer.send('youtube-comments', value=data)
        producer.flush()  # Pastikan dikirim langsung
        print("Sent:", json.dumps(data, ensure_ascii=False))
        time.sleep(random.uniform(0.5, 2))  # Simulasi streaming
    except Exception as e:
        print("Failed to send:", e)

# Tutup producer setelah selesai
producer.close()
