`

# LATAM Telecom ETL Pipeline 🚀

A production-grade PySpark ETL pipeline that extracts, transforms, validates,
and loads telecom churn data into Google BigQuery for executive analytics.

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

---

## ⚙️ Tech Stack

| Layer | Tool |
|---|---|
| Processing | Apache Spark / PySpark |
| Data Quality | Pandera |
| Data Warehouse | Google BigQuery |
| Cloud | GCP (Google Cloud Platform) |
| Language | Python 3.14 |
| Format | Parquet (intermediate) |

---

## 📁 Project Structure
pyspark-telecom-pipeline/

├── pipeline.py # Main ETL pipeline

├── quality_checks.py # Pandera schema & validation rules

├── requirements.txt # Dependencies

├── data/ # Raw CSV input

└── output/ # Parquet output (intermediate)

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
5. Run the pipeline
python pipeline.py

---

## 📊 Results

- ✅ **7,032 records** loaded to BigQuery
- ✅ **9 data quality rules** enforced via Pandera
- ✅ **3 engineered features** added
- ✅ Output table: `mdlr-504420.telecom_analytics.churn_analysis`

---

## 👤 Author

**Michael Alexis Meza Flores**
Data & Software Engineer | Huixquilucan, México
[LinkedIn](https://www.linkedin.com/in/michael-meza-flores) · [GitHub](https://github.com/meza62831)
`