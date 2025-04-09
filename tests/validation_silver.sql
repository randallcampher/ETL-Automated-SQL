/* =============================================================================
   Data Validation Script: Silver Schema
   -----------------------------------------------------------------------------
   Objective:
   This script validates and audits data across Silver Layer tables to ensure 
   integrity, cleanliness, and readiness for transformation in the Gold Layer.
   It includes checks for:
     - Duplicate or NULL primary keys
     - Unwanted spaces in text fields
     - Inconsistent or invalid codes
     - Negative or NULL numerical values
     - Incorrect date entries
     - Embedded transformation logic (commented)
   ============================================================================= */


/* =============================================================================
   Table Validation: silver.crm_cust_info
   ============================================================================= */

-- 1. Check for Duplicates & NULLs in Primary Key
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- 2. Check for Unwanted Spaces in Text Fields
SELECT cst_key FROM silver.crm_cust_info WHERE cst_key != TRIM(cst_key);
SELECT cst_firstname FROM silver.crm_cust_info WHERE cst_firstname != TRIM(cst_firstname);
SELECT cst_lastname FROM silver.crm_cust_info WHERE cst_lastname != TRIM(cst_lastname);
SELECT cst_marital_status FROM silver.crm_cust_info WHERE cst_marital_status != TRIM(cst_marital_status);
SELECT cst_gndr FROM silver.crm_cust_info WHERE cst_gndr != TRIM(cst_gndr);

-- 3. Review Standardised Abbreviated Columns
SELECT DISTINCT cst_marital_status
-- Transformation Preview:
-- CASE
--     WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
--     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
--     ELSE 'n/a'
-- END AS cst_marital_status_NEW

SELECT DISTINCT cst_gndr
-- Transformation Preview:
-- CASE
--     WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
--     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
--     ELSE 'n/a'
-- END AS cst_gndr_NEW


/* =============================================================================
   Table Validation: silver.crm_prd_info
   ============================================================================= */

-- 1. Check for Duplicates & NULLs in Primary Key
SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- 2. Check for Unwanted Spaces in Text Fields
SELECT	prd_key	FROM silver.crm_prd_info WHERE prd_key	!= TRIM(prd_key);
SELECT	prd_nm	FROM silver.crm_prd_info WHERE prd_nm	!= TRIM(prd_nm);

-- 3. Review Product Lines
SELECT DISTINCT prd_line FROM silver.crm_prd_info;

-- 4. Validate Cost Columns for Negatives & NULLs
SELECT prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost <= 0 OR prd_cost IS NULL;
-- Transformation Preview:
-- CASE WHEN prd_cost IS NULL THEN 0 ELSE prd_cost END AS prd_cost_NEW

-- 5. Validate Product Dates
SELECT * 
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

-- 6. Extract cat_id from prd_key
SELECT DISTINCT prd_key 
-- Transformation Preview:
-- REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
-- SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key_NEW


/* =============================================================================
   Table Validation: silver.crm_sales_details
   ============================================================================= */

-- 1. Check for NULLs & Unwanted Spaces
SELECT * 
FROM silver.crm_sales_details 
WHERE sls_ord_num != TRIM(sls_ord_num) OR sls_ord_num IS NULL;

SELECT * 
FROM silver.crm_sales_details 
WHERE sls_prd_key != TRIM(sls_prd_key) OR sls_prd_key IS NULL;

-- 2. Check for Invalid or Negative Customer IDs
SELECT sls_cust_id 
FROM silver.crm_sales_details 
WHERE sls_cust_id <= 0 OR sls_cust_id IS NULL;

-- 3. Validate Order, Shipping, and Due Dates
SELECT	sls_order_dt	FROM silver.crm_sales_details;
SELECT	sls_ship_dt		FROM silver.crm_sales_details;
SELECT	sls_due_dt		FROM silver.crm_sales_details;
-- Transformation Preview:
-- WHERE LEN(sls_order_dt) != 8 OR sls_order_dt <= 0

-- 4. Validate and Apply Business Logic to Sales Figures
SELECT 
	sls_quantity,
	sls_price,
	sls_sales
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
	OR sls_quantity <= 0 OR sls_price <= 0 OR sls_sales <= 0
	OR sls_quantity IS NULL OR sls_price IS NULL OR sls_sales IS NULL;
-- Transformation Preview:
-- CASE 
--     WHEN sls_sales != sls_quantity * ABS(sls_price) OR sls_sales IS NULL THEN sls_quantity * ABS(sls_price)
--     ELSE sls_sales
-- END AS sales_NEW,
-- CASE 
--     WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
--     ELSE sls_price
-- END AS sls_price_NEW


/* =============================================================================
   Table Validation: silver.erp_cust_az12
   ============================================================================= */

-- 1. Clean Customer IDs starting with 'NAS'
SELECT 
	cid,
	bdate,
	gen
FROM silver.erp_cust_az12;
-- Transformation Preview:
-- CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END AS cid_NEW

-- 2. Check for Future Dates in Birthdate
SELECT bdate 
FROM silver.erp_cust_az12 
WHERE bdate > GETDATE();
-- Transformation Preview:
-- CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate_NEW

-- 3. Standardise Gender Field
SELECT DISTINCT gen 
FROM silver.erp_cust_az12;
-- Transformation Preview:
-- CASE
--     WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
--     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
--     ELSE 'n/a'
-- END AS gen_NEW


/* =============================================================================
   Table Validation: silver.erp_loc_a101
   ============================================================================= */

-- 1. Standardise CID Format
SELECT cid 
FROM silver.erp_loc_a101;
-- Transformation Preview:
-- REPLACE(cid, '-', '')

-- 2. Review Country Codes
SELECT DISTINCT cntry 
FROM silver.erp_loc_a101;

-- 3. Sample Data Review
SELECT TOP 100 * 
FROM silver.erp_loc_a101;


/* =============================================================================
   Table Validation: silver.erp_px_cat_g1v2
   ============================================================================= */

-- 1. Check for Unwanted Spaces
SELECT * 
FROM silver.erp_px_cat_g1v2
WHERE id != TRIM(id) OR cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- 2. Review Unique Categories
SELECT DISTINCT cat			FROM silver.erp_px_cat_g1v2;
SELECT DISTINCT subcat		FROM silver.erp_px_cat_g1v2;
SELECT DISTINCT maintenance FROM silver.erp_px_cat_g1v2;

-- 3. Sample Data Review
SELECT * 
FROM silver.erp_px_cat_g1v2;
