# LATAM Telecom ETL Pipeline 🚀

A production-grade PySpark ETL pipeline that extracts, transforms, validates,
and loads telecom churn data into Google BigQuery — plus 3 downstream data
marts and a BigQuery ML churn prediction model.

Built as part of a data engineering portfolio targeting large-scale data
infrastructure roles.

---

## 🏗️ Architecture

CSV (7,043 rows)
→ PySpark Extract & Clean
→ Feature Engineering (churn_risk, is_high_value, revenue_per_month)
→ Pandera Data Quality Checks (9 rules)
→ Parquet (local intermediate)
→ BigQuery (7,032 rows)
→ Data Marts (dm_churn, dm_revenue, dm_coverage)
→ BigQuery ML (Logistic Regression — ROC AUC: 0.837)

---

## ⚙️ Tech Stack

| Layer | Tool |
|---|---|
| Processing | Apache Spark / PySpark |
| Data Quality | Pandera |
| Data Warehouse | Google BigQuery |
| Machine Learning | BigQuery ML (Logistic Regression) |
| Cloud | GCP (Google Cloud Platform) |
| Language | Python 3.14 |
| Format | Parquet (intermediate) |

---

## 📁 Project Structure
pyspark-telecom-pipeline/

├── pipeline.py # Main ETL pipeline

├── quality_checks.py # Pandera schema & validation rules

├── data_marts/

│ ├── dm_churn.py # Churn risk segmentation

│ ├── dm_revenue.py # Revenue by customer segment

│ └── dm_coverage.py # Service adoption analysis

├── notebooks/

│ └── churn_prediction_bqml.ipynb # BigQuery ML training & evaluation

├── requirements.txt # Dependencies

├── data/ # Raw CSV input

└── output/ # Parquet output (intermediate + marts)

---

## 🔍 Data Quality Rules (Pandera)

| Column | Rule |
|---|---|
| `customerID` | Not null, unique |
| `tenure` | Between 0–120 months |
| `MonthlyCharges` | Between $0–$500 |
| `TotalCharges` | Between $0–$100,000 |
| `Contract` | One of: Month-to-month, One year, Two year |
| `Churn` | One of: Yes, No |
| `churn_risk` | One of: HIGH, MEDIUM, LOW |
| `is_high_value` | Not null boolean |
| `revenue_per_month` | Float, nullable |

---

## 🔧 Feature Engineering

- **`churn_risk`** — HIGH / MEDIUM / LOW based on tenure and contract type
- **`is_high_value`** — customers with MonthlyCharges > $70
- **`revenue_per_month`** — TotalCharges / tenure ratio

---

## 📦 Data Marts

Three purpose-built marts, each aggregated with PySpark and written to Parquet:

| Mart | Purpose | Key Metrics |
|---|---|---|
| **`dm_churn`** | Segment users by churn risk | `churn_rate`, `avg_tenure_months`, `avg_monthly_charges` by risk tier & contract |
| **`dm_revenue`** | Revenue by customer segment | `total_revenue`, `avg_revenue_per_month` by value segment, contract & payment method |
| **`dm_coverage`** | Service adoption analysis | Streaming / security / support adoption % by internet service & contract |

Run individually with `python data_marts/dm_churn.py` (or `dm_revenue.py` / `dm_coverage.py`).

---

## 🤖 Machine Learning — BigQuery ML

- **Model:** Logistic Regression (`CREATE MODEL ... OPTIONS(model_type='LOGISTIC_REG')`)
- **Target:** `churn_label` (binary, derived from `Churn`)
- **Class balancing:** `auto_class_weights = TRUE`
- **Result:** **ROC AUC 0.837**
- Full workflow (dataset creation → training table → model training → evaluation) in `notebooks/churn_prediction_bqml.ipynb`

---

## 🚀 How to Run
bash
1. Clone the repo
git clone https://github.com/meza62831/pyspark-telecom-pipeline.git

cd pyspark-telecom-pipeline

2. Create virtual environment
python -m venv venv

source venv/bin/activate

3. Install dependencies
pip install -r requirements.txt

4. Add your GCP service account
Place your service_account.json in the root directory
5. Run the ETL pipeline
python pipeline.py

6. (Optional) Build the data marts
python data_marts/dm_churn.py

python data_marts/dm_revenue.py

python data_marts/dm_coverage.py

7. (Optional) Train the churn model
Open notebooks/churn_prediction_bqml.ipynb
---

## 📊 Results

- ✅ **7,032 records** loaded to BigQuery
- ✅ **9 data quality rules** enforced via Pandera
- ✅ **3 engineered features** added
- ✅ **3 data marts** built (churn, revenue, coverage)
- ✅ **BigQuery ML model** trained — ROC AUC: **0.837**
- ✅ Output table: `mdlr-504420.telecom_analytics.churn_analysis`

---

## 👤 Author

**Michael Alexis Meza Flores**
Data & Software Engineer | Huixquilucan, México
[LinkedIn](https://www.linkedin.com/in/michael-meza-flores) · [GitHub](https://github.com/meza62831)