# pipeline.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from google.cloud import bigquery

# 1. Iniciar Spark
spark = SparkSession.builder \
    .appName("LATAM Telecom ETL Pipeline") \
    .getOrCreate()

# 2. EXTRACT — Leer CSV
df = spark.read.csv("data/telecom_data.csv", header=True, inferSchema=True)
print(f"Records extracted: {df.count()}")

# Fix: castear TotalCharges a Double (viene como string con espacios)
df = df.withColumn("TotalCharges",
    F.col("TotalCharges").cast(DoubleType()))

# 3. TRANSFORM — Limpiar y enriquecer
df_transformed = df \
    .filter(F.col("TotalCharges").isNotNull()) \
    .withColumn("revenue_per_month",
        F.col("TotalCharges") / F.col("tenure")) \
    .withColumn("is_high_value",
        F.when(F.col("MonthlyCharges") > 70, True).otherwise(False)) \
    .withColumn("churn_risk",
        F.when(
            (F.col("tenure") < 12) & (F.col("Contract") == "Month-to-month"),
            "HIGH"
        ).when(F.col("tenure") < 24, "MEDIUM")
        .otherwise("LOW")
    )

# 4. Data Quality Check
total = df.count()
nulls = df.filter(F.col("TotalCharges").isNull()).count()
clean = total - nulls
print(f"Data quality: {clean}/{total} records passed validation ({round(clean/total*100, 2)}%)")

# 5. LOAD — Guardar a Parquet (intermedio)
df_transformed.write.mode("overwrite").parquet("output/transformed/")
print("Parquet saved successfully.")

# 6. LOAD — BigQuery
df_pandas = df_transformed.toPandas()

client = bigquery.Client.from_service_account_json("service_account.json")
table_id = "mdlr-504420.telecom_analytics.churn_analysis"

job = client.load_table_from_dataframe(df_pandas, table_id)
job.result()
print(f"✅ Loaded {len(df_pandas)} rows to BigQuery → {table_id}")