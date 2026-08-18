# data_marts/dm_churn.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def build_dm_churn(df):
    """
    Data Mart: Churn Analysis
    Segmenta usuarios por riesgo de churn con métricas clave.
    """
    dm = df \
        .filter(F.col("TotalCharges").isNotNull()) \
        .withColumn("churn_risk",
            F.when(
                (F.col("tenure") < 12) & (F.col("Contract") == "Month-to-month"), "HIGH"
            ).when(F.col("tenure") < 24, "MEDIUM")
            .otherwise("LOW")
        ) \
        .groupBy("churn_risk", "Contract") \
        .agg(
            F.count("*").alias("total_users"),
            F.avg("MonthlyCharges").alias("avg_monthly_charges"),
            F.avg("tenure").alias("avg_tenure_months"),
            F.sum(F.when(F.col("Churn") == "Yes", 1).otherwise(0)).alias("churned_users")
        ) \
        .withColumn("churn_rate",
            F.round(F.col("churned_users") / F.col("total_users") * 100, 2)
        ) \
        .orderBy("churn_risk")

    return dm


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("DM Churn") \
        .getOrCreate()

    df = spark.read.csv("data/telecom_data.csv", header=True, inferSchema=True)
    df = df.withColumn("TotalCharges", F.expr("try_cast(trim(TotalCharges) as double)"))

    dm_churn = build_dm_churn(df)
    dm_churn.show(truncate=False)

    dm_churn.write.mode("overwrite").parquet("output/data_marts/dm_churn/")
    print("✅ dm_churn saved.")