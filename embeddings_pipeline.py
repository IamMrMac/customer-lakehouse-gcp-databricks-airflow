# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Vector Embeddings for AI/LLM Applications
# MAGIC 
# MAGIC This notebook demonstrates how to:
# MAGIC - Extract text from clinical notes
# MAGIC - De-identify sensitive information
# MAGIC - Generate vector embeddings using sentence transformers
# MAGIC - Store vectors in Delta Lake and pgvector for RAG applications

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install sentence-transformers transformers presidio-analyzer presidio-anonymizer spacy
# MAGIC %pip install psycopg2-binary pgvector
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, pandas_udf, struct, lit, current_timestamp
)
from pyspark.sql.types import *
from delta.tables import DeltaTable

import pandas as pd
import numpy as np
from typing import List
import json

# Embedding model
from sentence_transformers import SentenceTransformer

# De-identification
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine

print("✓ Libraries imported successfully")

# Configuration
SILVER_TABLE = "silver.patient_encounters_clean"
GOLD_EMBEDDINGS_TABLE = "gold.patient_note_embeddings"
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"  # 384 dimensions
BATCH_SIZE = 32

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Embedding Model

# COMMAND ----------

# Load model (cache on driver)
print(f"Loading embedding model: {EMBEDDING_MODEL}")
model = SentenceTransformer(EMBEDDING_MODEL)
embedding_dim = model.get_sentence_embedding_dimension()

print(f"✓ Model loaded - Embedding dimension: {embedding_dim}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## De-identification Functions

# COMMAND ----------

def create_deidentifier():
    """Initialize de-identification engines"""
    analyzer = AnalyzerEngine()
    anonymizer = AnonymizerEngine()
    return analyzer, anonymizer


def deidentify_text(text: str, analyzer, anonymizer) -> str:
    """
    Remove PHI from text using Presidio
    Redacts: names, locations, dates, phone numbers, medical record numbers
    """
    if not text or pd.isna(text):
        return ""
    
    # Analyze for PII
    results = analyzer.analyze(
        text=text,
        entities=[
            "PERSON", "LOCATION", "DATE_TIME", 
            "PHONE_NUMBER", "EMAIL_ADDRESS", "MEDICAL_LICENSE"
        ],
        language="en"
    )
    
    # Anonymize
    anonymized = anonymizer.anonymize(text=text, analyzer_results=results)
    
    return anonymized.text


# Create de-identifier (singleton)
analyzer, anonymizer = create_deidentifier()

# Create UDF for de-identification
deidentify_udf = udf(
    lambda text: deidentify_text(text, analyzer, anonymizer),
    StringType()
)

print("✓ De-identification engine initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Prepare Data

# COMMAND ----------

# Read silver layer data
silver_df = spark.read.format("delta").table(SILVER_TABLE)

print(f"Loaded {silver_df.count():,} records from silver layer")

# Select relevant columns and de-identify
notes_df = silver_df.select(
    col("encounter_id"),
    col("patient_id"),
    col("note_text"),
    col("primary_diagnosis"),
    col("encounter_date"),
    col("visit_type"),
    col("department")
)

# Apply de-identification to note text
print("De-identifying clinical notes...")
deidentified_df = notes_df.withColumn(
    "note_text_deidentified",
    deidentify_udf(col("note_text"))
)

# Show sample
print("\nSample of de-identified notes:")
deidentified_df.select(
    "encounter_id",
    "note_text_deidentified",
    "primary_diagnosis"
).limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Embeddings with Pandas UDF

# COMMAND ----------

# Define schema for embeddings
embedding_schema = StructType([
    StructField("encounter_id", StringType(), False),
    StructField("embedding", ArrayType(FloatType()), False),
    StructField("embedding_model", StringType(), False),
    StructField("text_length", IntegerType(), False)
])


@pandas_udf(embedding_schema)
def generate_embeddings_udf(batch: pd.DataFrame) -> pd.DataFrame:
    """
    Pandas UDF to generate embeddings in batches
    This runs on executors, so model must be loaded here
    """
    # Load model on executor
    model = SentenceTransformer(EMBEDDING_MODEL)
    
    # Get texts
    texts = batch['note_text_deidentified'].tolist()
    encounter_ids = batch['encounter_id'].tolist()
    
    # Generate embeddings
    embeddings = model.encode(
        texts,
        batch_size=BATCH_SIZE,
        show_progress_bar=False,
        normalize_embeddings=True
    )
    
    # Create result dataframe
    result = pd.DataFrame({
        'encounter_id': encounter_ids,
        'embedding': [emb.tolist() for emb in embeddings],
        'embedding_model': [EMBEDDING_MODEL] * len(encounter_ids),
        'text_length': [len(text) for text in texts]
    })
    
    return result


# Generate embeddings
print("Generating embeddings (this may take a few minutes)...")

embeddings_df = deidentified_df.select(
    col("encounter_id"),
    col("note_text_deidentified")
).repartition(10).mapInPandas(generate_embeddings_udf, embedding_schema)

print(f"✓ Generated embeddings for {embeddings_df.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join with Original Data

# COMMAND ----------

# Join embeddings back with metadata
final_df = deidentified_df.join(
    embeddings_df,
    on="encounter_id",
    how="inner"
).select(
    col("encounter_id"),
    col("patient_id"),
    col("note_text_deidentified").alias("text_chunk"),
    col("embedding"),
    col("embedding_model"),
    col("text_length"),
    col("primary_diagnosis"),
    col("encounter_date"),
    col("visit_type"),
    col("department"),
    lit(current_timestamp()).alias("embedding_generated_at")
)

# Show sample
print("\nSample embeddings:")
final_df.select(
    "encounter_id",
    "text_length",
    "embedding_model",
    "primary_diagnosis"
).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Delta Lake (Gold Layer)

# COMMAND ----------

# Write to Delta Lake
print(f"Writing embeddings to {GOLD_EMBEDDINGS_TABLE}...")

final_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .partitionBy("encounter_date") \
    .saveAsTable(GOLD_EMBEDDINGS_TABLE)

print("✓ Embeddings saved to Delta Lake")

# Optimize table
spark.sql(f"OPTIMIZE {GOLD_EMBEDDINGS_TABLE}")
print("✓ Table optimized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export to pgvector for Fast Similarity Search

# COMMAND ----------

def export_to_pgvector(df, table_name="patient_embeddings"):
    """
    Export embeddings to PostgreSQL with pgvector extension
    This enables fast similarity search for RAG applications
    """
    import psycopg2
    from psycopg2.extras import execute_values
    
    # Connection details (from environment/secrets)
    conn_params = {
        'host': 'postgres',  # From docker-compose
        'port': 5432,
        'database': 'vectors',
        'user': 'postgres',
        'password': 'postgres'
    }
    
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Create extension if not exists
        cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        
        # Create table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                encounter_id TEXT PRIMARY KEY,
                patient_id TEXT,
                text_chunk TEXT,
                embedding vector({embedding_dim}),
                primary_diagnosis TEXT,
                encounter_date DATE,
                visit_type TEXT,
                department TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create index for similarity search
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS {table_name}_embedding_idx 
            ON {table_name} 
            USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100);
        """)
        
        conn.commit()
        print(f"✓ Created table {table_name} with vector index")
        
        # Collect data (be careful with memory for large datasets)
        data = df.limit(1000).collect()  # Limit for demo
        
        # Prepare data for insertion
        values = [
            (
                row.encounter_id,
                row.patient_id,
                row.text_chunk,
                row.embedding,
                row.primary_diagnosis,
                row.encounter_date,
                row.visit_type,
                row.department
            )
            for row in data
        ]
        
        # Insert data
        execute_values(
            cursor,
            f"""
            INSERT INTO {table_name} 
            (encounter_id, patient_id, text_chunk, embedding, 
             primary_diagnosis, encounter_date, visit_type, department)
            VALUES %s
            ON CONFLICT (encounter_id) DO UPDATE SET
                embedding = EXCLUDED.embedding,
                text_chunk = EXCLUDED.text_chunk;
            """,
            values
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✓ Exported {len(values)} embeddings to pgvector")
        
    except Exception as e:
        print(f"Error exporting to pgvector: {e}")
        print("This is expected if PostgreSQL is not configured")


# Export to pgvector
export_to_pgvector(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example: Similarity Search Query

# COMMAND ----------

# Example SQL for similarity search in pgvector:
similarity_search_sql = """
-- Find similar patient notes
SELECT 
    encounter_id,
    text_chunk,
    primary_diagnosis,
    1 - (embedding <=> query_embedding) AS similarity_score
FROM patient_embeddings
WHERE 1 - (embedding <=> query_embedding) > 0.7  -- 70% similarity threshold
ORDER BY embedding <=> query_embedding
LIMIT 10;
"""

print("Example pgvector similarity search query:")
print(similarity_search_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Summary Statistics

# COMMAND ----------

# Embedding statistics
stats = final_df.agg({
    'text_length': 'avg',
    'encounter_id': 'count'
}).collect()[0]

summary = {
    'total_embeddings': stats['count(encounter_id)'],
    'avg_text_length': stats['avg(text_length)'],
    'embedding_dimension': embedding_dim,
    'embedding_model': EMBEDDING_MODEL,
    'gold_table': GOLD_EMBEDDINGS_TABLE
}

print("\n" + "=" * 60)
print("EMBEDDINGS PIPELINE SUMMARY")
print("=" * 60)
for key, value in summary.items():
    print(f"{key}: {value}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC **For RAG Applications:**
# MAGIC 1. Query embeddings from pgvector using similarity search
# MAGIC 2. Retrieve top-k most similar documents
# MAGIC 3. Pass to LLM as context
# MAGIC 
# MAGIC **For Analytics:**
# MAGIC 1. Cluster embeddings to find patient cohorts
# MAGIC 2. Visualize embedding space with t-SNE/UMAP
# MAGIC 3. Train classifiers on embeddings
# MAGIC 
# MAGIC **Example RAG Query:**
# MAGIC ```python
# MAGIC query = "Patient with chest pain and shortness of breath"
# MAGIC query_embedding = model.encode(query)
# MAGIC # Search pgvector for similar cases
# MAGIC # Use results as context for LLM
# MAGIC ```

print("\n✓ Embeddings pipeline complete!")