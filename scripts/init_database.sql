/*
================================================================================================================

CREATE DATABASE AND SCHEMAS

================================================================================================================

	Script Purpose:
		1. Check if db already exists, drops if it does and recreated with schemas bronze, silver & gold.
		2. Forces the database into single-user mode, typically administrator.
		3. Forces all current connections to the database to be dropped immediately.
		4. Any ongoing transactions are rolled back (undone) to ensure that the command takes effect instantly.
		5. Drops db

	Warning:
		⚠️ ALL DATA WILL BE LOST PERMANENTLY! ⚠️  
		Ensure you have a backup before running this script.  
		This command will also forcibly disconnect all active users.  
		Use with caution in a production environment.  
*/

USE master;

-- Drop and recreate db 'BiycleDWH' if it exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'BiycleDWH')
	BEGIN
		ALTER DATABASE BiycleDWH SET SINGLE_USER WITH ROLLBACK IMMEDIATE
		DROP DATABASE BiycleDWH
	END;
GO

-- Create db 'BiycleDWH'
CREATE DATABASE BiycleDWH;
GO
USE BiycleDWH;
GO

-- Create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
