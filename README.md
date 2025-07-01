# ETL Extract  – Full and Incremental Extraction

**Student Name:** Rosamistica  
**Student ID:** 670699  
**Course:** Data Science Methodology  

---

## 📘 Overview

This project demonstrates how to implement both **Full Extraction** and **Incremental Extraction** processes in an ETL (Extract, Transform, Load) pipeline using a realistic synthetic dataset of user activity logs.

The goal is to simulate a real-world scenario where data engineers need to extract new data periodically and avoid unnecessary duplication by tracking changes using timestamps.

---

## 🎯 Objectives

- Understand the differences between **Full Extraction** and **Incremental Extraction**
- Implement both extraction types in a Python Jupyter Notebook
- Simulate a data source and maintain a tracking mechanism
- Practice version control and GitHub collaboration

---

## 🛠 Tools & Technologies

| Tool                 | Purpose                        |
|----------------------|--------------------------------|
| Python 3.x           | Programming language           |
| Pandas               | Data manipulation              |
| Jupyter Lab/Notebook | Interactive development        |
| Git & GitHub         | Version control & collaboration|
| CSV                  | Dataset storage                |

---

## 📁 Project Structure

ETL_Extract_Rosamistica_670699/
├── etl_extract.ipynb # Main ETL notebook
├── user_activity_logs.csv # Simulated activity log dataset
├── last_extraction.txt # Timestamp tracker for incremental extraction
├── .gitignore # Specifies files to exclude from Git
├── README.md # Project documentation

---

## 🗃 Dataset Description

- **File Name:** `user_activity_logs.csv`
- **Records:** 50+
- **Simulated Date Range:** April to May 2025
- **Attributes:**
  - `user_id`: Numeric ID for each user
  - `username`: Randomly generated usernames
  - `activity`: User activity (e.g., login, logout, purchase, comment)
  - `timestamp`: When the activity occurred
  - `last_updated`: When the record was last modified

The dataset was generated using Python and the `random` and `datetime` modules to simulate a natural flow of user events over time.

---

## 🔄 Full Extraction

In this mode, the entire dataset is pulled regardless of whether it was changed or not.

- ✅ Simple to implement
- ❌ Inefficient for large datasets

### Code Summary:
```python
df = pd.read_csv("user_activity_logs.csv", parse_dates=["last_updated"])
print(f"Pulled {len(df)} rows via full extraction.")
## Lab 5: Load

In this final stage of the ETL pipeline, we loaded the transformed datasets into a structured format using SQLite.

### Loading Method
- Used Python `sqlite3` to write CSVs into local `.db` files

### Output Files
- `loaded_data/full_data.db` (table: `full_data`)
- `loaded_data/incremental_data.db` (table: `incremental_data`)

### Sample SQL Query for Verification
```sql
SELECT * FROM full_data LIMIT 5;
