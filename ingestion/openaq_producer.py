import json
import time
import requests
from kafka import KafkaProducer

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 2),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def fetch_and_stream_air_quality():
    producer = create_kafka_producer()
    topic = "environmental-stream"
    
    # OpenAQ v2 API endpoint for air quality readings
    api_url = "https://api.openaq.org/v2/latest?limit=10"
    
    print(f"[*] Starting OpenAQ live stream to Kafka topic: '{topic}'...")
    
    while True:
        try:
            response = requests.get(api_url, timeout=10)
            if response.status_code == 200:
                payload = response.json()
                results = payload.get("results", [])
                
                for item in results:
                    record = {
                        "location_id": item.get("location"),
                        "city": item.get("city", "Unknown"),
                        "country": item.get("country"),
                        "coordinates": item.get("coordinates"),
                        "measurements": item.get("measurements", []),
                        "timestamp": time.time()
                    }
                    producer.send(topic, value=record)
                    print(f"[+] Dispatched environmental payload for: {record['city']}")
                
                producer.flush()
            else:
                print(f"[!] OpenAQ API returned status: {response.status_code}")
                
            time.sleep(15)  # Fetch every 15 seconds
            
        except Exception as e:
            print(f"[!] Streaming error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    fetch_and_stream_air_quality()