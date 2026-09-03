# SIC-Ecomm-Dimensional-Modeling

This repository contains our graduation project for the Samsung Innovation Campus (Round 8) Big Data Track. Our team is implementing **Project 3.2: Customers and Products Dimensional Modeling** using the Brazilian E-Commerce Public Dataset by Olist.

## Step 1: Source System Simulation & Raw Data Loading
**Responsible Team Member:** Ahmed Helmy

In a real-world data engineering architecture, the source relational database represents the raw, operational state of the application. To accurately simulate this environment before applying any ETL transformations, we set up a local MariaDB instance to act as our transactional source system.

### 1. The Dataset
We utilized the [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), specifically targeting five core operational files to build our tables:
* `olist_customers_dataset.csv`
* `olist_products_dataset.csv`
* `olist_orders_dataset.csv`
* `olist_order_items_dataset.csv`
* `olist_order_payments_dataset.csv`

### 2. Database Initialization
We created the `olist_raw` database and defined the schema to perfectly mirror the incoming raw CSV files. 

To initialize the database locally, execute the DDL script from the terminal:
```bash
mysql -u root -p < MySQL/01_create_source_tables.sql
