import os

import pandas as pd
import streamlit as st
from neo4j import GraphDatabase
from streamlit.errors import StreamlitSecretNotFoundError


st.set_page_config(
    page_title="TB StreamNet | Epidemiology Intelligence",
    page_icon=":hospital:",
    layout="wide",
)


def setting(name, default=None):
    try:
        return st.secrets[name]
    except (KeyError, StreamlitSecretNotFoundError):
        return os.getenv(name, default)


def demo_data():
    districts = pd.DataFrame(
        [
            {"district": "Central-Zone", "event_count": 248, "updated_at": "Demo dataset"},
            {"district": "East-District", "event_count": 221, "updated_at": "Demo dataset"},
            {"district": "North-Hub", "event_count": 187, "updated_at": "Demo dataset"},
            {"district": "South-Subdivision", "event_count": 164, "updated_at": "Demo dataset"},
            {"district": "West-Transit", "event_count": 139, "updated_at": "Demo dataset"},
        ]
    )
    symptoms = pd.DataFrame(
        [
            {"symptom": "persistent_cough", "patients": 316},
            {"symptom": "fever", "patients": 241},
            {"symptom": "night_sweats", "patients": 177},
            {"symptom": "weight_loss", "patients": 129},
            {"symptom": "hemoptysis", "patients": 72},
        ]
    )
    summary = {"patients": 512, "positive_smears": 96, "districts": 5, "symptoms": 5}
    return districts, symptoms, summary


st.title("TB StreamNet")
st.caption("Real-time epidemiology intelligence for tuberculosis surveillance")
st.markdown(
    "Track patient telemetry from **Kafka → Spark Structured Streaming → Neo4j** "
    "with district-level and symptom-level signals."
)


@st.cache_resource
def neo4j_driver():
    return GraphDatabase.driver(
        setting("NEO4J_URI"),
        auth=(
            setting("NEO4J_USER", "neo4j"),
            setting("NEO4J_PASSWORD"),
        ),
    )


def query_dataframe(query, **parameters):
    records, _, _ = neo4j_driver().execute_query(
        query,
        parameters_=parameters,
        database_=setting("NEO4J_DATABASE", "neo4j"),
    )
    return pd.DataFrame([record.data() for record in records])


def load_live_data():
    district_data = query_dataframe(
        """
        MATCH (d:District)
        RETURN d.district_id AS district,
               d.patient_event_count AS event_count,
               d.updated_at AS updated_at
        ORDER BY event_count DESC
        """
    )
    symptom_data = query_dataframe(
        """
        MATCH (p:Patient)-[:HAS_SYMPTOM]->(s:Symptom)
        RETURN s.name AS symptom, count(DISTINCT p) AS patients
        ORDER BY patients DESC
        """
    )
    summary = query_dataframe(
        """
        OPTIONAL MATCH (p:Patient)
        WITH count(p) AS patients
        OPTIONAL MATCH (positive:Patient {smear_positive: true})
        WITH patients, count(positive) AS positive_smears
        OPTIONAL MATCH (d:District)
        WITH patients, positive_smears, count(d) AS districts
        OPTIONAL MATCH (s:Symptom)
        RETURN patients, positive_smears, districts, count(s) AS symptoms
        """
    ).iloc[0].to_dict()
    return district_data, symptom_data, summary


live_mode = bool(setting("NEO4J_URI") and setting("NEO4J_PASSWORD"))
if live_mode:
    try:
        district_data, patient_data, summary = load_live_data()
    except Exception as error:
        st.error(f"Connected to Neo4j, but the database query failed: {error}")
        st.stop()
else:
    district_data, patient_data, summary = demo_data()
    st.info(
        "Demo mode is active. Add NEO4J_URI and NEO4J_PASSWORD in Streamlit "
        "Secrets to connect this dashboard to Neo4j AuraDB."
    )

st.sidebar.header("System status")
st.sidebar.success("Live Neo4j connection" if live_mode else "Portfolio demo mode")
st.sidebar.markdown("**Pipeline**  \nKafka → Spark → Neo4j → Streamlit")

metric_columns = st.columns(4)
metric_columns[0].metric("Patient events", int(district_data["event_count"].sum()))
metric_columns[1].metric("Patients", int(summary["patients"]))
metric_columns[2].metric("Positive smears", int(summary["positive_smears"]))
metric_columns[3].metric("Districts monitored", int(summary["districts"]))

if district_data.empty:
    st.info("No patient events have been processed yet.")
else:
    st.subheader("District surveillance overview")
    st.bar_chart(district_data.set_index("district")["event_count"])
    st.dataframe(district_data, use_container_width=True, hide_index=True)

if not patient_data.empty:
    st.subheader("Symptom distribution")
    st.bar_chart(patient_data.set_index("symptom")["patients"])

st.caption(
    "This portfolio project uses synthetic telemetry for demonstration and "
    "is not a clinical diagnostic tool."
)

if st.button("Refresh data"):
    st.rerun()
