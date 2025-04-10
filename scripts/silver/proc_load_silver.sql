/*
    ===============================================================================================
    STORED PROCEDURE: LOAD SILVER LAYER
    ===============================================================================================
    Purpose:  
    -- This stored procedure (silver.load_silver) loads transformed data from 'bronze' into 
       the 'silver' schema tables for downstream consumption.

    WARNING:  
    ALL EXISTING DATA IN TARGET TABLES WILL BE LOST!  
    -- This script **truncates** all target tables before inserting clean, transformed data.  
    -- Ensure validation of bronze layer data prior to execution.  

    Error Handling:  
	-- If any error occurs, an error message will be printed in the catch block.  
	-- Consider logging errors to an error table for better debugging in production.  

    Parameters:
    -- None

    Usage:
    -- EXEC silver.load_silver
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	-- ================================================
	-- Declare timing variables
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;
	-- ================================================

	BEGIN TRY
		-- ================================================
		-- Batch Load Start
		PRINT '===============================================================================================';
		PRINT 'LOADING SILVER LAYER';
		PRINT '===============================================================================================';
		SET @batch_start = GETDATE();

		-- ================================================
		-- 1. Load CRM Customer Info
		PRINT '>>>>> Truncating Table: silver.crm_cust_info';
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>>>>> Loading Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id, cst_key, cst_firstname, cst_lastname,
			cst_marital_status, cst_gndr, cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname),
			TRIM(cst_lastname),
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END,
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END,
			cst_create_date
		FROM (
			SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_num
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE row_num = 1;
		SET @end_time = GETDATE();
		PRINT '>>>>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

		-- ================================================
		-- 2. Load CRM Product Info
		PRINT '>>>>> Truncating Table: silver.crm_prd_info';
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT '>>>>> Loading Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id, prd_key, cat_id, prd_nm,
			prd_cost, prd_line, prd_start_dt, prd_end_dt
		)
		SELECT 
			prd_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)),
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
			prd_nm,
			ISNULL(prd_cost, 0),
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END,
			CAST(prd_start_dt AS DATE),
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_id) - 1 AS DATE)
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>>>>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

		-- ================================================
		-- 3. Load CRM Sales Details
		PRINT '>>>>> Truncating Table: silver.crm_sales_details';
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>>>>> Loading Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num, sls_prd_key, sls_cust_id,
			sls_order_dt, sls_ship_dt, sls_due_dt,
			sls_sales, sls_quantity, sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN LEN(sls_order_dt) != 8 OR sls_order_dt <= 0 THEN NULL ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END AS sls_order_dt,
			CASE WHEN LEN(sls_ship_dt) != 8 OR sls_ship_dt <= 0 THEN NULL ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END AS sls_ship_dt,
			CASE WHEN LEN(sls_due_dt) != 8 OR sls_due_dt <= 0 THEN NULL ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END AS sls_due_dt,
			CASE WHEN sls_sales != sls_quantity * ABS(sls_price) OR sls_sales IS NULL THEN sls_quantity * ABS(sls_price) ELSE sls_sales END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0) ELSE sls_price END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>>>>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

		-- ================================================
		-- 4. Load ERP Customer AZ12
		PRINT '>>>>> Truncating Table: silver.erp_cust_az12';
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT '>>>>> Loading Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END,
			CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>>>>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

		-- ================================================
		-- 5. Load ERP Location A101
		PRINT '>>>>> Truncating Table: silver.erp_loc_a101';
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>>>>> Loading Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT 
			REPLACE(cid, '-', ''),
			CASE
				WHEN TRIM(cntry) IN ('DE') THEN 'Germany'
				WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN NULL
				ELSE TRIM(cntry)
			END
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>>>>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

		-- ================================================
		-- 6. Load ERP Product Category
		PRINT '>>>>> Truncating Table: silver.erp_px_cat_g1v2';
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT '>>>>> Loading Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT id, cat, subcat, maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>>>>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

		-- ================================================
		-- Batch Load End
		SET @batch_end = GETDATE();
		PRINT '===============================================================================================';
		PRINT 'LOADING SILVER LAYER COMPLETED';
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS VARCHAR) + ' sec';
		PRINT '===============================================================================================';

	END TRY

	BEGIN CATCH
		PRINT 'Oops: Error Occurred During Loading Silver Layer';
		PRINT 'Error: ' + ERROR_MESSAGE();
	END CATCH
END;

