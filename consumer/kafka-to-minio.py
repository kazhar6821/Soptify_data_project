import json
import os
from kafka import KafkaConsumer
from datetime import datetime
import boto3
from dotenv import load_dotenv

# ---------- Load environment variables ----------
# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
# Look for .env in the parent directory (C:\Users\kzmha\no)
parent_dir = os.path.dirname(script_dir)
env_path = os.path.join(parent_dir, '.env')

# Try to load from parent directory, if not found try current directory
if os.path.exists(env_path):
    load_dotenv(env_path)
    print(f"✅ Loaded .env from: {env_path}")
else:
    # Try current directory as fallback
    env_path = os.path.join(script_dir, '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"✅ Loaded .env from: {env_path}")
    else:
        print(f"⚠️  No .env file found. Searched in:")
        print(f"   - {parent_dir}")
        print(f"   - {script_dir}")

# ---------- Configuration ----------
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))

# ---------- Validate required environment variables ----------
required_vars = {
    "MINIO_BUCKET": MINIO_BUCKET,
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "KAFKA_TOPIC": KAFKA_TOPIC,
    "KAFKA_BOOTSTRAP_SERVER": KAFKA_BOOTSTRAP_SERVER,
    "KAFKA_GROUP_ID": KAFKA_GROUP_ID
}

missing_vars = [var for var, value in required_vars.items() if value is None]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# ---------- Connect to MinIO ----------
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Ensure bucket exists (idempotent)
try:
    s3.head_bucket(Bucket=MINIO_BUCKET)
    print(f"Bucket {MINIO_BUCKET} already exists.")
except Exception:
    s3.create_bucket(Bucket=MINIO_BUCKET)
    print(f"Created bucket {MINIO_BUCKET}.")

# ---------- Kafka Consumer Setup ----------
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=KAFKA_GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print(f"🎧 Listening for events on Kafka topic '{KAFKA_TOPIC}'...")

batch = []
for message in consumer:
    event = message.value
    batch.append(event)
    
    if len(batch) >= BATCH_SIZE:
        now = datetime.utcnow()
        date_path = now.strftime("date=%Y-%m-%d/hour=%H")
        file_name = f"spotify_events_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
        file_path = f"bronze/{date_path}/{file_name}"
        
        json_data = "\n".join([json.dumps(e) for e in batch])
        
        s3.put_object(
            Bucket=MINIO_BUCKET,
            Key=file_path,
            Body=json_data.encode("utf-8")
        )
        
        print(f"✅ Uploaded {len(batch)} events to MinIO: {file_path}")
        batch = []