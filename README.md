# ETL-System-for-Sales-Customers-Product-Inventory-Management

# 📦 Retail Data Warehouse ETL Project (SQL Server)

## 📌 Project Overview
This project implements an **end-to-end ETL (Extract, Transform, Load) pipeline** for a retail business using **SQL Server**.  
The system ingests **daily CSV files** for Products, Customers, and Sales, processes them through staging tables, applies transformations and validations, and loads them into a **Data Warehouse schema** with dimensions, fact tables, inventory tracking, and audit logging.

---

## 🏗️ Architecture Overview

**Source → Staging → Transformation → Data Warehouse → Audit & Archival**

### Data Flow:
1. **Extract**
   - CSV files (`Products`, `Customers`, `Sales`) are loaded into **staging tables**
     - `stg_products`
     - `stg_customers`
     - `stg_sales`

2. **Transform**
   - Data cleansing & validation
   - Slowly Changing Dimension (SCD Type 2) handling
   - Aggregations (total quantity sold, total amount spent)
   - Inventory calculation (BOH & EOH)
   - Currency conversion (USD → NPR)

3. **Load**
   - Dimension tables:
     - `dim_product`
     - `dim_customer`
   - Fact table:
     - `fact_sales`
   - Inventory table:
     - `inventory_daily`

4. **Audit & Control**
   - `etl_audit` table tracks:
     - Package name
     - Run start & end time
     - Status (Success / Failure)
     - Error messages

---

## 🗂️ Database Objects

### 🔹 Staging Tables
- `stg_products`
- `stg_customers`
- `stg_sales`

Used only for raw data ingestion.

---

### 🔹 Dimension Tables
#### `dim_product`
- Maintains **product history** using **SCD Type 2**
- Tracks:
  - Price changes
  - Category/name updates
  - Active & inactive records

#### `dim_customer`
- Maintains **customer history**
- Tracks:
  - Name, email, city changes
  - Active & inactive versions

---

### 🔹 Fact Table
#### `fact_sales`
- Stores transactional sales data
- Linked to product and customer dimensions

---

### 🔹 Inventory Table
#### `inventory_daily`
- Calculates **daily inventory**
- Logic:
  - BOH (Beginning on Hand)
  - EOH (Ending on Hand = BOH − Sales)
  - Default BOH = 100 if no previous record
- Incremental based on **Last Run Date**

---

## 🔁 Incremental Load Logic
- Uses **Last Run Date** to process only **new sales**
- Prevents reprocessing of old data
- Supports Day-1, Day-2, Day-3 incremental runs

---

## ⚙️ Stored Procedures

### 📌 `dim_product_insert`
- Implements **SCD Type 2**
- Deactivates old records when product details change
- Inserts new version of product

### 📌 `dim_customer_insert`
- Handles customer history
- Deactivates old records on attribute change

### 📌 `inventory_daily`
- Calculates inventory per day
- Based on last successful run date

---

## 🧮 User-Defined Functions
- `dbo.total_qun_sold` → Calculates total quantity sold per product
- `dbo.convertUSDtoNPR` → Converts USD to NPR

---

## ✅ Data Validation Rules
- Null checks on key columns
- Data type consistency
- Duplicate handling
- Referential consistency before loading facts

---

## 📁 File Handling Rules

### ✔ On Successful Execution:
- Source CSV files are **moved to the `Archive` folder**

### ❌ On Failure:
- Source files are **copied to the `Error` folder**
- Error details are logged in `etl_audit`

---

## 📊 ETL vs ELT
| ETL | ELT |
|----|----|
| Transform before loading | Load first, transform later |
| Used in this project | ❌ |
| Best for large warehouses | ✔ |

This project follows **ETL architecture**.

---

## 🧠 Key Concepts Covered
- ETL pipeline design
- Data Warehousing fundamentals
- Slowly Changing Dimensions (Type 2)
- Incremental data loading
- Inventory management
- Audit & error handling
- SQL Server stored procedures & functions

---

## 🛠️ Technologies Used
- SQL Server
- T-SQL
- CSV Flat Files
- ETL Concepts (SSIS-style logic)

---



## ⭐ How to Use
1. Load CSV files into staging tables
2. Run dimension load procedures
3. Load fact table
4. Execute inventory procedure
5. Verify audit logs
6. Archive or error-handle files

---

