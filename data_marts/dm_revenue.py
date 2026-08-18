# data_marts/dm_revenue.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def build_dm_revenue(df):
    """
    Data Mart: Revenue Analysis
    Analiza ingresos por segmento de cliente y tipo de contrato.
    """
    dm = df \
        .filter(F.col("TotalCharges").isNotNull()) \
        .withColumn("revenue_per_month",
            F.round(F.col("TotalCharges") / F.col("tenure"), 2)
        ) \
        .withColumn("customer_segment",
            F.when(F.col("MonthlyCharges") > 70, "High Value")
            .when(F.col("MonthlyCharges") > 40, "Mid Value")
            .otherwise("Low Value")
        ) \
        .groupBy("customer_segment", "Contract", "PaymentMethod") \
        .agg(
            F.count("*").alias("total_customers"),
            F.round(F.avg("MonthlyCharges"), 2).alias("avg_monthly_charges"),
            F.round(F.sum("TotalCharges"), 2).alias("total_revenue"),
            F.round(F.avg("revenue_per_month"), 2).alias("avg_revenue_per_month")
        ) \
        .orderBy(F.col("total_revenue").desc())

    return dm

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("DM Revenue") \
        .getOrCreate()

    df = spark.read.csv("data/telecom_data.csv", header=True, inferSchema=True)
    df = df.withColumn("TotalCharges", F.expr("try_cast(trim(TotalCharges) as double)"))

    dm_revenue = build_dm_revenue(df)
    dm_revenue.show(truncate=False)

    dm_revenue.write.mode("overwrite").parquet("output/data_marts/dm_revenue/")
    print("✅ dm_revenue saved.")