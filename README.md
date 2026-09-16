# TB StreamNet

> A portfolio-ready real-time epidemiology intelligence dashboard built with
> Kafka, Spark Structured Streaming, Neo4j, and Streamlit.

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

## Deploy the portfolio dashboard

The dashboard supports two modes:

- **Demo mode:** runs immediately without a database and shows clearly labeled
  portfolio data.
- **Live mode:** connects to Neo4j AuraDB using Streamlit Cloud Secrets.

On Streamlit Community Cloud, deploy `dashboard/app.py` from this repository.
The dashboard has its own lightweight dependency file at
`dashboard/requirements.txt`, so Streamlit Cloud does not need to install the
full Spark and machine-learning pipeline just to serve the portfolio UI.
In the app settings, add these secrets:

```toml
NEO4J_URI = "neo4j+s://<your-aura-instance>.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "<your-aura-password>"
NEO4J_DATABASE = "neo4j"
```

Do not commit these values to the repository. The local Docker Neo4j instance
at `localhost:7687` is not reachable from Streamlit Cloud.

## Run tests

```powershell
python -m unittest discover -s tests
```

Neo4j is available at `http://localhost:7474` with the default credentials
`neo4j` / `password123`.
