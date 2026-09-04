# SIC-Ecomm-Dimensional-Modeling

This repository contains our graduation project for the Samsung Innovation Campus (Round 8) Big Data Track. Our team is implementing **Project 3.2: Customers and Products Dimensional Modeling** using the Brazilian E-Commerce Public Dataset by Olist.

## Repository Structure
```text
SIC-Ecomm-Dimensional-Modeling/
├── .gitignore
├── Docs/
│   └── Document.docx
├── Hive/
│   ├── Hive DDL
│   ├── Image .jpeg
│   └── analytical_queries
├── MySQL/
│   ├── 01_create_source_tables.sql
│   └── 02_bulk_insert.sql
├── Spark/
│   ├── Cleaning&Dim_Product.ipynb
│   ├── Customer_Dim_RFM.ipynb
│   └── Ingestion.ipynb
└── README.md
```

## Step 1: Source System Simulation & Raw Data Loading

In a real-world data engineering architecture, the source relational database represents the raw, operational state of the application. To accurately simulate this environment before applying any ETL transformations, we set up a local MariaDB instance to act as our transactional source system.

### 1. The Dataset
We utilized the [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), specifically targeting five core operational files to build our tables:
* `olist_customers_dataset.csv`
* `olist_products_dataset.csv`
* `olist_orders_dataset.csv`
* `olist_order_items_dataset.csv`
* `olist_order_payments_dataset.csv`

### 2. Database Initialization
We created the `olist_raw` database and defined the schema to perfectly mirror the incoming raw CSV files. To initialize the database locally, execute the DDL script from the terminal:
```bash
mysql -u root -p < MySQL/01_create_source_tables.sql
```

### 3. Bulk Data Ingestion
To efficiently load hundreds of thousands of records, we bypassed standard `INSERT` statements in favor of MySQL's `LOAD DATA LOCAL INFILE` command. The raw data is ingested into the MariaDB system by executing our bulk insert script:
```bash
mysql -u root -p olist_raw --local-infile=1 < MySQL/02_bulk_insert.sql
```

*(Note: This prepares the raw operational database. All data cleaning, enrichment, and metric calculations—such as RFM scores—are handled downstream in the PySpark processing layer.)*
