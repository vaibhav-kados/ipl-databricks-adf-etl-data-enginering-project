# 🏏 IPL Data Analysis Project – Azure Data Factory & Databricks (Medallion Architecture)

This project demonstrates an **end-to-end Data Engineering pipeline** using **Azure Data Factory (ADF)**, **Azure Databricks**, and **Azure Data Lake Storage Gen2 (ADLS Gen2)**.  
It is based on the **Medallion Architecture (Bronze → Silver → Gold)** and processes **IPL cricket data (matches & deliveries)** to build analytical insights.

---

## 🚀 Project Architecture

The project follows the Medallion architecture with three layers:

1. **Bronze Layer** – Stores raw data from the source system in Delta format.  
2. **Silver Layer** – Performs cleaning, transformations, and incremental loads.  
3. **Gold Layer** – Stores aggregated and business-ready data (e.g., Batsman performance summary).

📌 **Architecture Diagram**  
![Architecture Diagram](architecture/ipl_de_architecture.png)  

---

## 🔄 Data Flow

1. **ADF Master Pipeline**  
   - Triggers automatically when new data lands in the **bronze container** in ADLS Gen2.  
   - Uses **Get Metadata** and **If Condition** activity to check whether **both files (`matches` & `deliveries`)** are present.  
   - If yes → Extracts the file names and passes them as **parameters** to a Databricks job.  

   📌 **Screenshot:** ADF Master Pipeline  
   _Attach here_: `images/1.adf_master_pipeline.png`

2. **Databricks Job Workflow**
   - **Step 1: Create External Location**  
     Configures access to ADLS Gen2.  
     📌 Screenshot: `images/databricks_external_location.png`

   - **Step 2: Create Catalog & Schemas**  
     Creates:
     - Catalog: `ipl_data_analysis_catalog`  
     - Schemas: `bronze`, `silver`, `gold`  
     📌 Screenshot: `images/databricks_catalog_schema.png`

   - **Step 3: Create Bronze Tables**  
     Raw files (`matches`, `deliveries`) are stored as **Delta tables** in the bronze layer.  
     Metadata columns added: `data_source`, `ingestion_date`  
     📌 Screenshot: `images/databricks_bronze_tables.png`

   - **Step 4: Create Silver Tables**  
     - Incremental load applied  
       - `matches` → Incremental on `match_id`  
       - `deliveries` → Incremental on (`match_id`, `inning`, `over`, `ball`)  
     - Null handling, cleaning & schema enforcement performed.  
     📌 Screenshot: `images/databricks_silver_tables.png`

   - **Step 5: Create Gold Tables**  
     - Analytical tables prepared (example: **Batsman Performance Summary**)  
       - Total runs  
       - Balls faced  
       - Strike rate & other metrics  
     📌 Screenshot: `images/databricks_gold_tables.png`

---

## 🗂 Project Structure

ipl-databricks-adf-etl-data-enginering-project/
│
├── notebooks/ # Databricks notebooks (ETL code)
├── pipelines/ # ADF pipeline JSON exports
├── images/ # Screenshots (architecture, pipeline, notebooks, dashboards)
├── README.md # Project documentation

---

## ⚙️ Technologies Used

- **Azure Data Factory (ADF)** – Orchestration & pipeline automation  
- **Azure Databricks** – Data processing & transformation  
- **ADLS Gen2** – Storage for raw & processed data  
- **Delta Lake** – For incremental loads & ACID transactions  
- **PySpark / SQL** – Transformations & aggregations  

---

## 📊 Final Output (Gold Layer)

Example: **Batsman Performance Summary Table**  
- `batsman_name`  
- `total_runs`  
- `balls_faced`  
- `strike_rate`  
- `4s` / `6s` count  
- `matches_played`  

📌 Screenshot: Gold Table Preview  
_Attach here_: `images/batsman_summary.png`

---

## 📷 Screenshots to Attach

1. `images/architecture.png` – Overall Architecture  
2. `images/adf_master_pipeline.png` – ADF Master Pipeline  
3. `images/databricks_external_location.png` – External Location Setup  
4. `images/databricks_catalog_schema.png` – Catalog & Schema Creation  
5. `images/databricks_bronze_tables.png` – Bronze Tables in Databricks  
6. `images/databricks_silver_tables.png` – Silver Tables in Databricks  
7. `images/databricks_gold_tables.png` – Gold Tables in Databricks  
8. `images/batsman_summary.png` – Gold Table Example (Batsman Performance)

---

## 📌 How to Run This Project

1. Upload raw `matches.csv` and `deliveries.csv` files to **bronze container** in ADLS Gen2.  
2. ADF Master Pipeline triggers automatically → validates files → invokes Databricks job.  
3. Databricks notebooks process data into **Bronze → Silver → Gold** layers.  
4. Query Gold tables for analytics using **Databricks SQL** / **Power BI**.

---

## 🏆 Key Learnings

- Implemented **Medallion Architecture** in Azure.  
- Built **incremental pipelines** using Delta Lake.  
- Automated **data ingestion → transformation → analytics** workflow with ADF + Databricks.  
- Designed reusable & scalable architecture for future datasets.  

---

## 👨‍💻 Author

**Vaibhav Kados**  
- Azure Data Engineer | Data Enthusiast  
- [LinkedIn](https://www.linkedin.com) | [GitHub](https://github.com)
