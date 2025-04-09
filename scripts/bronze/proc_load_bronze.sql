/*
	===============================================================================================
	STIRED PROCEDURE: LOAD BRONZE LAYER
	===============================================================================================
	Purpose:  
	-- This stored procedure (bronze.load_bronze) loads data from CSV source files into 	the 'bronze' schema tables using BULK INSERT. 
	-- It first truncates the tables to ensure fresh data is loaded.
	  
	WARNING:  
	ALL EXISTING DATA IN TARGET TABLES WILL BE LOST!   
	-- This script **truncates** all target tables before loading new data.  
	-- Ensure a backup exists if required before execution.  
	-- File paths are **hardcoded**, verify that they are correct before running.  
	-- If file paths are incorrect or missing, BULK INSERT will fail.  
	-- Ensure the SQL Server account has **read access** to the source files.  

	Error Handling:  
	-- If any error occurs, an error message will be printed in the catch block.  
	-- Consider logging errors to an error table for better debugging in production.  

	Parameters:
	-- None

	Usage:
	-- EXEC bronze.load_bronze
*/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;
	BEGIN TRY
		SET @batch_start = GETDATE();
		PRINT '==============================================================================================='
		PRINT 'LOADING BRONZE LAYER'
		PRINT '==============================================================================================='

		PRINT '-----------------------------------------------------------------------------------------------'
		PRINT 'a. Loading CRM Source Data'
		PRINT '-----------------------------------------------------------------------------------------------'
	
		SET @start_time = GETDATE();
		PRINT '>>>>> Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>>>>> Loading Data Into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\218014618\Videos\Data Science\SQL Data Warehouse Portfolio Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'

		SET @start_time = GETDATE();
		PRINT '>>>>>Truncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>>>>> Loading Data Into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\218014618\Videos\Data Science\SQL Data Warehouse Portfolio Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'

		SET @start_time = GETDATE();
		PRINT '>>>>>Truncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>>>>> Loading Data Into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\218014618\Videos\Data Science\SQL Data Warehouse Portfolio Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'

		PRINT '-----------------------------------------------------------------------------------------------'
		PRINT 'b. Loading ERP Source Data'
		PRINT '-----------------------------------------------------------------------------------------------'
	
	
		SET @start_time = GETDATE();
		PRINT '>>>>>Truncating Table: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>>>>> Loading Data Into: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\218014618\Videos\Data Science\SQL Data Warehouse Portfolio Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'


		SET @start_time = GETDATE();
		PRINT '>>>>>Truncating Table: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>>>>> Loading Data Into: bronze.erp_loc_a101 '
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\218014618\Videos\Data Science\SQL Data Warehouse Portfolio Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
	
		SET @start_time = GETDATE();
		PRINT '>>>>>Truncating Table: bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>>>>> Loading Data Into:  bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\218014618\Videos\Data Science\SQL Data Warehouse Portfolio Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
		SET @batch_end = GETDATE();
		PRINT '==============================================================================================='
		PRINT 'LOADING BRONZE LAYER COMPLETED'
		PRINT 'Load Duraton: ' + CAST(DATEDIFF(second, @batch_start, @batch_end) AS VARCHAR) + ' seconds'
		PRINT '==============================================================================================='

	END TRY
	BEGIN CATCH
		PRINT 'Oops: Error Occured During Loading Bronze Layer';
		PRINT 'Error:' + ERROR_MESSAGE();
	END CATCH
END
