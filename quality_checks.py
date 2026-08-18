import pandera as pa
import pandas as pd
from pandera import Column, DataFrameSchema, Check

schema = DataFrameSchema({
    "customerID":       Column(str,    nullable=False, unique=True),
    "tenure":           Column(float,  Check.between(0, 120), nullable=False),
    "MonthlyCharges":   Column(float,  Check.between(0, 500), nullable=False),
    "TotalCharges":     Column(float,  Check.between(0, 100000), nullable=True),
    "Contract":         Column(str,    Check.isin(["Month-to-month", "One year", "Two year"])),
    "Churn":            Column(str,    Check.isin(["Yes", "No"])),
    "churn_risk":       Column(str,    Check.isin(["HIGH", "MEDIUM", "LOW"])),
    "is_high_value":    Column(bool,   nullable=False),
    "revenue_per_month":Column(float,  nullable=True),
})

def run_quality_checks(df_pandas: pd.DataFrame) -> pd.DataFrame:
    print("\n📊 Running data quality checks with Pandera...")
    validated = schema.validate(df_pandas)
    print("✅ All quality checks passed!\n")
    return validated