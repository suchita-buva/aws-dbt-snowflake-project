# 🏡 Airbnb Data Engineering Pipeline (Snowflake + dbt + AWS)

## 📌 Overview

This project demonstrates an end-to-end ELT data pipeline built using AWS S3, Snowflake, and dbt.
The pipeline ingests raw Airbnb dataset, performs transformations using dbt, and creates analytics-ready data models.

---

## ⚙️ Tech Stack

* **Cloud Storage:** AWS S3
* **Data Warehouse:** Snowflake
* **Transformation Tool:** dbt
* **Language:** SQL
* **Version Control:** Git

---

## 🏗️ Architecture

```
AWS S3 → Snowflake Stage → dbt Transformations → Analytics Tables
```

---

## 🔄 Data Pipeline Flow

1. Raw Airbnb data is stored in **AWS S3**
2. Data is loaded into **Snowflake staging tables** using external/internal stages
3. **dbt models** transform raw data into structured layers:

   * Staging Layer
   * Intermediate Layer
   * Mart Layer
4. Final tables are optimized for **analytics and reporting**

---

## 📊 Data Modeling

* Implemented **Star Schema**
* Fact and dimension tables created for business analysis
* Modular and reusable dbt models

---

## ✅ Data Quality Checks

* Implemented dbt tests:

  * `not_null`
  * `unique`
  * `relationships`
* Ensures reliability and consistency of data

---

## ⚡ Key Features

* End-to-end ELT pipeline
* Modular transformations using dbt
* Scalable Snowflake architecture
* Data quality validation using dbt tests
* Optimized SQL transformations for performance

---

## 🚀 Future Enhancements

* Add orchestration using Airflow
* Implement incremental models in dbt
* Add data visualization layer (Power BI / Tableau)

---

## 📸 Project Screenshots (Add Here)
### Project Structure
![Project Structure](images/Project_structure.png)

### Sample Transformation (SQL Model)
![SQL Model](images/SQL-Model.png)

---

## 🔗 GitHub Repository

https://github.com/suchita-buva/aws-dbt-snowflake-project
