# TB StreamNet

TB StreamNet is a local streaming MVP for TB patient telemetry. It generates
synthetic patient events, transports them through Kafka, aggregates them with
Spark Structured Streaming, and stores a queryable contact-tracing graph in
Neo4j.

## Start the infrastructure

```powershell
docker compose -f docker\docker-compose.yml up -d
```

Activate the Python environment:

```powershell
.\.venv\Scripts\activate
```

Run the patient producer and Spark processor in separate terminals:

```powershell
python ingestion\patient_producer.py
python processing\spark_streaming.py
```

The processor stores `Patient`, `District`, and `Symptom` nodes with
`LOCATED_IN` and `HAS_SYMPTOM` relationships.

## Start the dashboard

```powershell
streamlit run dashboard\app.py
```

Open the URL printed by Streamlit, usually `http://localhost:8501`.

## Run tests

```powershell
python -m unittest discover -s tests
```

Neo4j is available at `http://localhost:7474` with the default credentials
`neo4j` / `password123`.
