# data_marts/dm_coverage.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def build_dm_coverage(df):
    """
    Data Mart: Service Coverage Analysis
    Analiza adopción de servicios por segmento de usuario.
    """
    dm = df \
        .filter(F.col("TotalCharges").isNotNull()) \
        .withColumn("has_streaming",
            F.when(
                (F.col("StreamingTV") == "Yes") | (F.col("StreamingMovies") == "Yes"), 1
            ).otherwise(0)
        ) \
        .withColumn("has_security",
            F.when(
                (F.col("OnlineSecurity") == "Yes") | (F.col("OnlineBackup") == "Yes"), 1
            ).otherwise(0)
        ) \
        .withColumn("has_support",
            F.when(
                (F.col("TechSupport") == "Yes") | (F.col("DeviceProtection") == "Yes"), 1
            ).otherwise(0)
        ) \
        .groupBy("InternetService", "Contract") \
        .agg(
            F.count("*").alias("total_users"),
            F.sum("has_streaming").alias("streaming_users"),
            F.sum("has_security").alias("security_users"),
            F.sum("has_support").alias("support_users"),
            F.round(F.avg("MonthlyCharges"), 2).alias("avg_monthly_charges")
        ) \
        .withColumn("streaming_adoption_pct",
            F.round(F.col("streaming_users") / F.col("total_users") * 100, 2)
        ) \
        .withColumn("security_adoption_pct",
            F.round(F.col("security_users") / F.col("total_users") * 100, 2)
        ) \
        .orderBy(F.col("total_users").desc())

    return dm

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("DM Coverage") \
        .getOrCreate()

    df = spark.read.csv("data/telecom_data.csv", header=True, inferSchema=True)
    df = df.withColumn("TotalCharges", F.expr("try_cast(trim(TotalCharges) as double)"))

    dm_coverage = build_dm_coverage(df)
    dm_coverage.show(truncate=False)

    dm_coverage.write.mode("overwrite").parquet("output/data_marts/dm_coverage/")
    print("✅ dm_coverage saved.")