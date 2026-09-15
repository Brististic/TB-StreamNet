import json
import time
from datetime import datetime, timezone
import random
import uuid
from kafka import KafkaProducer

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 2),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def stream_patient_events():
    producer = create_kafka_producer()
    topic = "patient-events"
    
    districts = ["Central-Zone", "North-Hub", "East-District", "South-Subdivision", "West-Transit"]
    symptoms_pool = ["persistent_cough", "fever", "night_sweats", "weight_loss", "hemoptysis"]
    
    print(f"[*] Starting Patient Telemetry stream to Kafka topic: '{topic}'...")
    
    while True:
        try:
            current_iso_time = datetime.now(timezone.utc).isoformat()
            
            patient_record = {
                "patient_id": f"PT-{uuid.uuid4().hex[:6].upper()}",
                "district_id": random.choice(districts),
                "age": random.randint(18, 75),
                "symptoms": random.sample(symptoms_pool, k=random.randint(1, 3)),
                "smear_positive": random.choice([True, False, False]),
                "contact_count": random.randint(1, 8),
                "timestamp_utc": current_iso_time
            }
            
            producer.send(topic, value=patient_record)
            producer.flush()
            print(f"[{current_iso_time}] Dispatched Patient: {patient_record['patient_id']} | District: {patient_record['district_id']}")
            
            time.sleep(2)
            
        except Exception as e:
            print(f"[!] Patient stream error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    stream_patient_events()