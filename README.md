# 📊 Marketing Data ETL Pipeline

## 🚀 Overview
This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline built using Python. The pipeline processes raw, messy marketing data and transforms it into structured datasets suitable for analytics and reporting.

---

## 🧩 Problem
The raw dataset contains:
- Inconsistent delimiters (`;` instead of `,`)
- Corrupted rows and malformed records
- Missing values
- Mixed data formats (e.g., numeric values stored as strings like "2,7")

This simulates real-world data challenges where raw data is often unreliable and requires preprocessing before analysis.

---

## ⚙️ Solution
The pipeline performs the following steps:

### 1. Extract
- Reads raw CSV data using Pandas
- Handles malformed rows using robust parsing techniques

### 2. Transform
- Cleans and standardizes column names
- Converts string-based numeric values into proper numeric format
- Handles missing values in both numeric and categorical columns
- Creates derived metrics such as **engagement_ratio**

### 3. Data Validation
- Ensures no null values remain
- Checks for duplicate records
- Validates that numeric fields contain only non-negative values

### 4. Data Modeling
- Implements a **star schema**:
  - **Fact Table:** campaign_performance
  - **Dimension Tables:** users, campaigns

### 5. Load
- Loads processed data into a SQLite database
- Exports clean datasets as CSV files for further analysis

---

## 🏗️ Tech Stack
- Python
- Pandas
- NumPy
- SQLite

---

## 📂 Project Structure
```
marketing_pipeline_final.py
marketing_data.csv
marketing.db
campaign_performance.csv
users.csv
campaigns.csv
```

## 📈 Key Features
- Handles messy, real-world data formats
- Implements data quality checks
- Demonstrates data modeling (fact/dimension tables)
- Simulates a production-style data pipeline

---

## 🧠 Learnings
- Data cleaning is the most critical step in any pipeline
- Real-world datasets often require robust parsing strategies
- Data validation prevents downstream issues in analytics workflows

---

## 🔮 Future Improvements
- Automate pipeline with scheduling (Airflow)
- Store data in cloud data warehouse (AWS Redshift / Snowflake)
- Add dashboard visualization (Tableau / Power BI)

---

## 👤 Author
Harshitha Babu
