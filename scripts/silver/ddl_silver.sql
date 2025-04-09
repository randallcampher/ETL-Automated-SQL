/*
====================================================================================================
DDL SCRIPT: CREATE SILVER TABLES
====================================================================================================

Purpose:
    This script checks for the existence of key tables in the 'silver' schema and drops them 
    if they exist. It then recreates these tables to store cleaned and conformed data sourced from 
    CRM and ERP systems, ready for integration or analysis in the data warehouse pipeline.

Tables Created:
    - silver.crm_cust_info        : Customer master data from CRM
    - silver.crm_prd_info         : Product master data from CRM
    - silver.crm_sales_details    : Sales transaction data from CRM
    - silver.erp_cust_az12        : Customer demographics from ERP
    - silver.erp_loc_a101         : Customer location data from ERP
    - silver.erp_px_cat_g1v2      : Product category and maintenance details from ERP

WARNING:
	 ALL EXISTING DATA IN THESE TABLES WILL BE LOST
    - This script drops and recreates the tables.
    - Ensure that this operation is safe and that backups exist before running it in any environment
      containing critical data.
    - Dropping tables may affect downstream processes, reports, or users dependent on these tables.

====================================================================================================
*/

-- Drop and recreate CRM Customer Info Table
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gndr            VARCHAR(50),
    cst_create_date     DATE,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);

-- Drop and recreate CRM Product Info Table
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id             INT,
    prd_key            VARCHAR(50),
	cat_id			   VARCHAR(50),
    prd_nm             VARCHAR(50),
    prd_cost           INT,
    prd_line           VARCHAR(50),
    prd_start_dt       DATE,
    prd_end_dt         DATE,
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);

-- Drop and recreate CRM Sales Details Table
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
    sls_ord_num        VARCHAR(50),
    sls_prd_key        VARCHAR(50),
    sls_cust_id        INT,
    sls_order_dt       DATE,
    sls_ship_dt        DATE,
    sls_due_dt         DATE,
    sls_sales          INT,
    sls_quantity       INT,
    sls_price          INT,
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);

-- Drop and recreate ERP Customer Demographics Table
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 (
    cid                VARCHAR(50),
    bdate              DATE,
    gen                VARCHAR(50),
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);

-- Drop and recreate ERP Customer Location Table
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
    cid                VARCHAR(50),
    cntry              VARCHAR(50),
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);

-- Drop and recreate ERP Product Category Table
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
    id                 VARCHAR(50),
    cat                VARCHAR(50),
    subcat             VARCHAR(50),
    maintenance        VARCHAR(50),
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
