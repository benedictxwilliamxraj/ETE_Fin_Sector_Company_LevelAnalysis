# 📊 Financial KPI Dashboard (2020–2025)

**Automated Analytics Pipeline with Airflow, Oracle Cloud, and Power BI**

---

## 🚀 Overview

A robust, end-to-end financial analytics solution that automates the ingestion, transformation, modeling, and visualization of company performance data across 2020–2025. Built for scale, security, and decision-ready insights.

---

## 🔧 What I Did

- **Extracted data** from Compustat, CRSP, and IBES (~500K rows).
- **Cleansed, joined, and orchestrated pipelines** in Oracle Cloud using Apache Airflow (Dockerized).
- **Designed a Snowflake Schema** with dimensions: `Company`, `Sector`, `Year` and corresponding fact tables.
- **Calculated 15 KPIs**: ROE, ROA, margins, growth rates, valuation ratios, etc.
- **Built composite scores** to rank top companies and sectors annually.
- **Automated monthly data refreshes** and enforced RBAC:
  - Engineers manage raw and intermediate layers.
  - Analysts access production views only.
- **Connected Power BI** to the production warehouse to deliver an interactive screening tool.

---

## 💡 Key Learnings

- **Large Volume Handling**: Partitioned tables and optimized batch loads to ingest ~500K records quickly.
- **Reliable Orchestration**: Used Dockerized Airflow for dependable job scheduling and dependency tracking.
- **Security and Governance**: Implemented Role-Based Access Control to separate engineering and analyst environments.

---

## 📈 Results

- Filter by **sector** and **year** to surface highest-scoring companies.
- Identify **top sectors** and standout companies annually.
- Dashboards **update automatically** for up-to-date decision-making.

---

## 🖼️ Visuals

### 📌 Dashboard Sample  
![Dashboard Screenshot](images/dashboard_sample.png) 

### 🛠️ Architecture Diagram  
![Architecture Diagram](images/architecture_diagram.png)

---

## ⚙️ Tech Stack

| Component          | Technology                |
|-------------------|---------------------------|
| Data Sources       | Compustat, CRSP, IBES     |
| Data Warehouse     | Oracle Cloud              |
| Orchestration      | Apache Airflow (Docker)   |
| Data Modeling      | Snowflake Schema          |
| Access Control     | Role-Based Access Control |
| Business Intelligence | Power BI              |

---

## 📂 Repository Structure

```bash
.
├── dags/                      # Airflow DAGs
├── docker/                    # Docker configs for Airflow
├── sql/                       # SQL scripts for staging, transformation, and modeling
├── images/                    # Dashboard and architecture images
├── powerbi/                   # PBIX files and configs
└── README.md
