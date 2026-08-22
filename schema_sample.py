import os
from pyspark.sql import SparkSession

# 1. Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("Inspect_MusicBrainz_Schema")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 2. Correctly resolve PROJECT_ROOT (one folder up from glue/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
json_file_path = os.path.join(PROJECT_ROOT, "airflow", "data", "raw", "inspect_raw.json").replace("\\", "/")

# 3. Read and print schema
print(f"\n--- Loading schema from: {json_file_path} ---")

try:
    df = spark.read.option("multiline", "true").json(json_file_path)
    
    print("\n=== RAW JSON INFERRED SCHEMA ===")
    df.printSchema()
    
    print("\n=== RECORD COUNT ===")
    print(f"Total rows: {df.count()}\n")

finally:
    spark.stop()