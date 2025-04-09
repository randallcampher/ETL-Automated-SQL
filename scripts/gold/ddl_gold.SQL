
/********************************************************************************************
* Layer          : Gold Layer
* Environment    : Production
* Purpose        : Defines dimensional and fact views for customer analytics and reporting
* Description    : 
*   This script creates the core star schema components in the Gold layer of the EDW.
*   It includes:
*     - dim_customers: Customer dimension with enriched demographic information.
*     - dim_products: Product dimension with category and cost details.
*     - fact_sales: Sales fact table linking products and customers.
* 
* Notes:
*   - Source data is pulled from the Silver layer tables.
*   - Historical records and irrelevant gender codes are filtered/normalized where applicable.
********************************************************************************************/

-- ========================================================================================
-- DROP AND CREATE: dim_customers
-- Description: Enriched customer dimension with country, gender resolution and birthdate.
-- ========================================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    cl.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE ca.gen
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date	
FROM silver.crm_cust_info ci
INNER JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 cl
    ON ci.cst_key = cl.cid;

-- Optional Validation
SELECT DISTINCT marital_status FROM gold.dim_customers;

-- ========================================================================================
-- DROP AND CREATE: dim_products
-- Description: Product dimension with product category, cost, and start date.
-- ========================================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO 

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pr.prd_start_dt, pr.prd_key) AS product_key,
    pr.prd_id AS product_id,
    pr.prd_key AS product_number,
    pr.prd_nm AS product_name,
    pr.cat_id AS category_id,
    ct.cat AS category,
    ct.subcat AS sub_category,
    ct.maintenance,	
    pr.prd_cost AS product_cost,
    pr.prd_line AS product_line,
    pr.prd_start_dt AS product_start_date
FROM silver.crm_prd_info pr
LEFT JOIN silver.erp_px_cat_g1v2 ct
    ON pr.cat_id = ct.id
WHERE pr.prd_end_dt IS NULL; -- Exclude historical products

-- Optional Validation
SELECT * FROM gold.dim_products;

-- ========================================================================================
-- DROP AND CREATE: fact_sales
-- Description: Sales fact table referencing both customer and product dimensions.
-- ========================================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;

-- Optional Validation
SELECT * FROM gold.fact_sales;

-- ========================================================================================
-- DATA INTEGRITY CHECK: Identify missing dimension joins in fact table
-- ========================================================================================
SELECT 
    *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers cu
    ON f.customer_key = cu.customer_key
LEFT JOIN gold.dim_products pr
    ON f.product_key = pr.product_key
WHERE f.customer_key IS NULL;

