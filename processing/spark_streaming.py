import os
from datetime import datetime, timezone

from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType


def _neo4j_config():
    return (
        os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        os.getenv("NEO4J_USER", "neo4j"),
        os.getenv("NEO4J_PASSWORD", "password123"),
        os.getenv("NEO4J_DATABASE", "neo4j"),
    )


def write_batch_to_neo4j(batch_df, _batch_id):
    records = [
        {
            "patient_id": row["patient_id"],
            "district_id": row["district_id"],
            "age": row["age"],
            "symptoms": row["symptoms"] or [],
            "smear_positive": row["smear_positive"],
            "contact_count": row["contact_count"],
            "timestamp_utc": row["timestamp_utc"],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        for row in batch_df.collect()
    ]
    if not records:
        return

    district_ids = set()
    for record in records:
        district_ids.add(record["district_id"])

    uri, user, password, database = _neo4j_config()
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        driver.execute_query(
            """
            UNWIND $records AS record
            MERGE (patient:Patient {patient_id: record.patient_id})
            SET patient.age = record.age,
                patient.smear_positive = record.smear_positive,
                patient.contact_count = record.contact_count,
                patient.timestamp_utc = record.timestamp_utc
            MERGE (district:District {district_id: record.district_id})
            MERGE (patient)-[:LOCATED_IN]->(district)
            FOREACH (symptom IN record.symptoms |
                MERGE (symptom_node:Symptom {name: symptom})
                MERGE (patient)-[:HAS_SYMPTOM]->(symptom_node)
            )
            """,
            records=records,
            database_=database,
        )
        driver.execute_query(
            """
            MATCH (district:District)<-[:LOCATED_IN]-(patient:Patient)
            WHERE district.district_id IN $district_ids
            WITH district, count(patient) AS count
            SET district.patient_event_count = count,
                district.updated_at = $updated_at
            """,
            district_ids=list(district_ids),
            updated_at=records[-1]["updated_at"],
            database_=database,
        )

    batch_df.groupBy("district_id").count().orderBy("district_id").show(truncate=False)
    print(f"[*] Stored {len(records)} patients and updated {len(district_ids)} districts in Neo4j.")


def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    checkpoint_path = os.path.join(project_root, ".spark-checkpoint")
    local_hadoop_home = os.path.join(project_root, ".hadoop")
    local_hadoop_bin = os.path.join(local_hadoop_home, "bin")

    if os.path.isfile(os.path.join(local_hadoop_bin, "winutils.exe")):
        os.environ["HADOOP_HOME"] = local_hadoop_home
        path_entries = [
            entry
            for entry in os.environ["PATH"].split(os.pathsep)
            if os.path.normcase(os.path.normpath(entry))
            != os.path.normcase(os.path.normpath(r"C:\hadoop\bin"))
        ]
        os.environ["PATH"] = local_hadoop_bin + os.pathsep + os.pathsep.join(path_entries)

    # 1. Initialize Spark Session with Kafka Package
    spark = SparkSession.builder \
        .appName("TB-StreamNet-Consumer") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.extraJavaOptions", "-Dhadoop.native.lib=false") \
        .config("spark.executor.extraJavaOptions", "-Dhadoop.native.lib=false") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config(
            "spark.sql.streaming.checkpointFileManagerClass",
            "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager",
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Match schema to synthetic producer
    patient_schema = StructType([
        StructField("patient_id", StringType(), True),
        StructField("district_id", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("symptoms", ArrayType(StringType()), True),
        StructField("smear_positive", BooleanType(), True),
        StructField("contact_count", IntegerType(), True),
        StructField("timestamp_utc", StringType(), True)
    ])

    # 3. Read stream from Kafka broker
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "patient-events") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 4. Parse JSON
    parsed_df = df.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), patient_schema).alias("data")) \
        .select("data.*")

    parsed_df = parsed_df.filter(col("district_id").isNotNull())

    # 5. Store patients, symptoms, district relationships, and aggregate counts.
    query = parsed_df.writeStream \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .foreachBatch(write_batch_to_neo4j) \
        .start()

    print("[*] Spark Structured Streaming engine started. Waiting for incoming batches...")
    query.awaitTermination()

if __name__ == "__main__":
    main()