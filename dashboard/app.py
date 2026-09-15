import os

import pandas as pd
import streamlit as st
from neo4j import GraphDatabase


st.set_page_config(page_title="TB StreamNet", page_icon=":hospital:", layout="wide")
st.title("TB StreamNet")
st.caption("Live patient telemetry and district risk overview")


@st.cache_resource
def neo4j_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        auth=(
            os.getenv("NEO4J_USER", "neo4j"),
            os.getenv("NEO4J_PASSWORD", "password123"),
        ),
    )


def query_dataframe(query, **parameters):
    records, _, _ = neo4j_driver().execute_query(
        query,
        parameters_=parameters,
        database_=os.getenv("NEO4J_DATABASE", "neo4j"),
    )
    return pd.DataFrame([record.data() for record in records])


try:
    district_data = query_dataframe(
        """
        MATCH (d:District)
        RETURN d.district_id AS district,
               d.patient_event_count AS event_count,
               d.updated_at AS updated_at
        ORDER BY event_count DESC
        """
    )
    patient_data = query_dataframe(
        """
        MATCH (p:Patient)-[:HAS_SYMPTOM]->(s:Symptom)
        RETURN s.name AS symptom, count(DISTINCT p) AS patients
        ORDER BY patients DESC
        """
    )
except Exception as error:
    st.error(f"Neo4j is unavailable: {error}")
    st.stop()

if district_data.empty:
    st.info("No patient events have been processed yet.")
else:
    total_events = int(district_data["event_count"].fillna(0).sum())
    st.metric("Patient events", total_events)
    st.subheader("Events by district")
    st.bar_chart(district_data.set_index("district")["event_count"])
    st.dataframe(district_data, use_container_width=True, hide_index=True)

if not patient_data.empty:
    st.subheader("Patients by symptom")
    st.bar_chart(patient_data.set_index("symptom")["patients"])

if st.button("Refresh"):
    st.rerun()
