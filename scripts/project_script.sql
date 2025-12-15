/*==========================================================
  CREATE DATABASE
  Purpose: Create a separate database for ETL & DWH project
==========================================================*/
CREATE DATABASE Project;
GO

USE Project;
GO

/*==========================================================
  STAGING TABLES
  Purpose: Temporary tables to store raw data 
           loaded from source files (CSV/Flat files)
==========================================================*/

/* Product staging table */
CREATE TABLE stg_products(
  product_id INT,
  name VARCHAR(255),
  category VARCHAR(100),
  price INT,
  Updatedat DATETIME2,              -- last updated timestamp from source
  load_file_name VARCHAR(255),      -- source file name
  LoadedDateTime DATETIME2          -- ETL load timestamp
);

/* Sales staging table */
CREATE TABLE stg_sales (
  SalesID BIGINT,
  ProductID INT,
  CustomerID INT,
  Quantity INT,
  Price INT,
  TotalAmount BIGINT,
  SaleDate DATE,
  load_file_name VARCHAR(255),      -- source file name
  LoadedDateTime DATETIME2          -- ETL load timestamp
);

/* Customer staging table */
CREATE TABLE stg_customers (
  customer_id INT,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  email VARCHAR(255),
  city VARCHAR(100),
  updated_at DATETIME2,             -- last updated timestamp from source
  loaded_file_name VARCHAR(255),    -- source file name
  LoadedDateTime DATETIME2          -- ETL load timestamp
);

/*==========================================================
  FACT & DIMENSION TABLES
==========================================================*/

/* Daily inventory snapshot table */
CREATE TABLE inventory_daily (
  inventory_date DATE,
  product_id INT,
  boh INT,  -- Beginning On Hand inventory
  eoh INT   -- Ending On Hand inventory
);

/* Fact table storing sales transactions */
CREATE TABLE fact_sales (
  product_sk INT IDENTITY(1,1) PRIMARY KEY,
  sale_id BIGINT,
  product_id INT,
  customer_id INT,
  qty INT,
  Price INT,
  Total_amt BIGINT,
  sale_date DATE,
  LoadedDateTime DATETIME2
);

/* Product dimension (Slowly Changing Dimension – Type 2) */
CREATE TABLE dim_product (
  product_sk INT IDENTITY(1,1) PRIMARY KEY,
  product_id INT,
  name VARCHAR(255),
  category VARCHAR(100),
  price INT,
  total_qty_sold BIGINT,            -- derived metric
  is_active BIT,                    -- current active record flag
  start_date DATETIME,              -- SCD start date
  end_date DATETIME NULL,           -- SCD end date
  LastUpdatedDateTime DATETIME2
);

/* Customer dimension (Slowly Changing Dimension – Type 2) */
CREATE TABLE dim_customer(
  customer_sk INT IDENTITY(1,1) PRIMARY KEY,
  customer_id INT,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  email VARCHAR(255),
  city VARCHAR(100),
  total_amt_spent BIGINT,           -- derived metric
  is_active BIT,
  start_date DATETIME,
  end_date DATETIME NULL,
  LastUpdatedDateTime DATETIME2
);

/*==========================================================
  ETL AUDIT TABLE
  Purpose: Track ETL execution status
==========================================================*/
CREATE TABLE etl_audit (
  audit_id INT IDENTITY(1,1) PRIMARY KEY,
  package_name VARCHAR(200),
  run_start DATETIME,
  run_end DATETIME,
  status VARCHAR(50),
  error_message VARCHAR(MAX)
);

/* Sample audit record */
INSERT INTO etl_audit 
VALUES ('Package1','2024-01-04','2024-01-04','Success',NULL);

------------------------------------------------------------


/*==========================================================
  METADATA QUERIES
  Purpose: View column names and data types
==========================================================*/
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'stg_customers';

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'stg_products';

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'stg_sales';

------------------------------------------------------------

/*==========================================================
  STORED PROCEDURE: inventory_daily
  Purpose:
   - Calculate daily inventory
   - BOH = yesterday's EOH or default 100
   - EOH = BOH - quantity sold
==========================================================*/
CREATE PROCEDURE inventory_daily
    @lastrundate DATE
AS
BEGIN

    ;WITH CTE_A AS (
        -- Get sales after last run date
        SELECT 
            s.SaleDate,
            p.product_id,
            SUM(s.Quantity) AS total_quantity_sold
        FROM dim_product p
        LEFT JOIN stg_sales s
            ON p.product_id = s.ProductID
        WHERE s.SaleDate > @lastrundate
          AND p.is_active = 1
        GROUP BY p.product_id, s.SaleDate
    ),
    CTE_B AS (
        -- Determine BOH (previous day EOH or default 100)
        SELECT
            COALESCE(sn.SaleDate, DATEADD(DAY,1,inv.inventory_date)) AS SaleDate,
            sn.product_id,
            COALESCE(inv.eoh, 100) AS updated_boh,
            COALESCE(sn.total_quantity_sold, 0) AS total_quantity_sold
        FROM CTE_A sn
        LEFT JOIN inventory_daily inv
            ON sn.product_id = inv.product_id
           AND inv.inventory_date = DATEADD(DAY,-1,sn.SaleDate)
    ),
    CTE_C AS (
        -- Calculate Ending On Hand inventory
        SELECT
            SaleDate,
            product_id,
            updated_boh,
            (updated_boh - total_quantity_sold) AS eoh
        FROM CTE_B
    )

    -- Insert calculated inventory data
    INSERT INTO inventory_daily (product_id, inventory_date, boh, eoh)
    SELECT product_id, SaleDate, updated_boh, eoh
    FROM CTE_C;

END;
GO

------------------------------------------------------------

/*==========================================================
  STORED PROCEDURE: dim_product_insert
  Purpose: Maintain Product Dimension (SCD Type 2)
==========================================================*/
CREATE PROCEDURE dim_product_insert
AS
BEGIN

    MERGE dim_product AS tgt
    USING (
        SELECT product_id, name, category, price, Updatedat 
        FROM stg_products
    ) AS src
    ON tgt.product_id = src.product_id
       AND tgt.is_active = 1

    -- If attribute change, expire old record
    WHEN MATCHED AND (
        src.name <> tgt.name OR
        src.category <> tgt.category OR
        src.price <> tgt.price
    )
    THEN
        UPDATE SET
            tgt.is_active = 0,
            tgt.end_date = GETDATE(),
            tgt.LastUpdatedDateTime = GETDATE()

    -- Insert new product
    WHEN NOT MATCHED
    THEN
        INSERT (product_id, name, category, price, is_active, start_date, end_date, LastUpdatedDateTime)
        VALUES (src.product_id, src.name, src.category, src.price, 1, src.Updatedat, NULL, GETDATE())

    -- Mark deleted products as inactive
    WHEN NOT MATCHED BY SOURCE AND tgt.is_active = 1
    THEN
        UPDATE SET
            tgt.is_active = 0,
            tgt.end_date = GETDATE(),
            tgt.LastUpdatedDateTime = GETDATE();

END;
GO

------------------------------------------------------------

/*==========================================================
  STORED PROCEDURE: dim_customer_insert
  Purpose: Maintain Customer Dimension (SCD Type 2)
==========================================================*/
CREATE PROCEDURE dim_customer_insert
AS
BEGIN

    MERGE dim_customer t
    USING (
        SELECT customer_id, first_name, last_name, email, city, updated_at
        FROM stg_customers
    ) s
    ON t.customer_id = s.customer_id
       AND t.is_active = 1

    -- If customer attributes change
    WHEN MATCHED AND (
        s.first_name <> t.first_name OR
        s.last_name <> t.last_name OR
        s.email <> t.email OR
        s.city <> t.city
    )
    THEN
        UPDATE SET
            t.is_active = 0,
            t.end_date = GETDATE(),
            t.LastUpdatedDateTime = GETDATE()

    -- Insert new customer
    WHEN NOT MATCHED BY TARGET
    THEN
        INSERT (customer_id, first_name, last_name, email, city, is_active, start_date, end_date, LastUpdatedDateTime)
        VALUES (s.customer_id, s.first_name, s.last_name, s.email, s.city, 1, s.updated_at, NULL, GETDATE())

    -- Handle deleted customers
    WHEN NOT MATCHED BY SOURCE AND t.is_active = 1
    THEN
        UPDATE SET
            t.is_active = 0,
            t.end_date = GETDATE(),
            t.LastUpdatedDateTime = GETDATE();

END;
GO

------------------------------------------------------------

/*==========================================================
  UTILITY FUNCTION
  Purpose: Convert USD to NPR
==========================================================*/
CREATE FUNCTION dbo.convertUSDtoNPR(@USD BIGINT)
RETURNS BIGINT
AS
BEGIN
    DECLARE @NPR BIGINT;
    SET @NPR = @USD * 133;
    RETURN @NPR;
END;
GO


/*==========================================================
  SCALAR FUNCTION
  Purpose: Calculate total quantity sold for a product
==========================================================*/
CREATE FUNCTION dbo.total_qun_sold(@product_id INT)
RETURNS BIGINT
AS
BEGIN
    DECLARE @total BIGINT;

    SELECT @total = SUM(qty)
    FROM fact_sales
    WHERE product_id = @product_id;

    RETURN ISNULL(@total,0);
END;
GO

/* Update total quantity sold in active product records */
UPDATE dim_product
SET total_qty_sold = dbo.total_qun_sold(product_id)
WHERE is_active = 1;

------------------------------------------------------------

