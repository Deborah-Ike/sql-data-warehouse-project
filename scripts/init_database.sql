/*
Create Database and Schemas
This script creates a new database named 'DataWarehouse'. The scripts sets up 3 schemas within the databases: 'bronze', 'silver', and 'gold'.
*/

USE master;
GO

-- Check if database exists, drop it if it exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE NAME = 'DataWarehouse')
BEGIN 
ALTER DATABASE DataWarehouse SET SINGLR_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END;
GO

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
