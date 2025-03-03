/*
========================================================================================================================
Quality checks
========================================================================================================================
Purpose:
This script performs various quality checks for data consistency, accuracy, and standardization across the 'silver' schemas.
It includes checks for:
-Null or duplicate primary keys
-Unwanted spaces in string field 
-Data standardization and consistency
-Invalid date ranges and orders
-Data consistency between related fields
========================================================================================================================
*/
/*******************************Customer information format**************************************************************/
INSERT INTO silver.crm_cust_info (
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' /* standardize by making symbols reader friendly */
     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a' /* handling missing values to n/a*/
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' /* standardize by making symbols reader friendly */
     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a' /* handling missing values to n/a*/
END cst_gndr,
cst_create_date
FROM (
     SELECT 
	 *,
     ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
     FROM bronze.crm_cust_info
	 WHERE cst_id IS NOT NULL
)t WHERE flag_last =1 /* remove duplicate of customer */

/***********************************************************************************************************************/
SELECT * 
FROM (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last =1 

/* Check nulls and duplicate*/
SELECT  cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

/* check unwanted space */
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

/* standardize and make consitent */
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info

/* END:  Customer information format**************************************************************/

/**START PRODUCT INFORMATION FORMAT **************************************************************/
IF OBJECT_ID ('silver.crm_prd_info' , 'U') IS NOT NULL
DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info ( 
prd_id INT,
cat_id NVARCHAR(50),
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost NVARCHAR(50),
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

INSERT INTO silver.crm_prd_info(
prd_id ,
cat_id,
prd_key,
prd_nm ,
prd_cost,
prd_line ,
prd_start_dt ,
prd_end_dt 
)
SELECT prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5),'-','_' ) AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key) ) AS prd_key,
    prd_nm,
    ISNULL(prd_cost,0) AS prd_cost, /* making null values 0, this is best for when we have to do calculations*/
    CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info



SELECT sls_prd_key FROM bronze.crm_sales_details

SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2

/* Check nulls and duplicate*/
SELECT  prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

/* check unwanted space */
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

/* check null or negative numbers */
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

/* standardize and make consitent */
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

/* check inavlid date order */
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

/* END:  Product information format**************************************************************/


/**START SALES INFORMATION FORMAT **************************************************************/
/* making sure all the product key in cr_sales_details table is also in the crm_prd_info */
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

/* */
INSERT INTO silver.crm_sales_details (
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price)

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_order_dt as VARCHAR) AS DATE) /*convert invalid date e.g 20139078 or 450912 etc to valid date e.g 2013-09-01 */
END AS sls_order_dt, /* we can not change from int to date this is whyb we first use varchar */
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_ship_dt as VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_due_dt as VARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price) /* changing invalid data e.g missing ot incorrect */
	ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity,0)  /* calcualte valid price */
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details

/**END CRM SALES INFORMATION FORMAT **************************************************************/

/**START ERP CUSTOMER FORMAT **************************************************************/
/* Clean our id data in erp_cust_az12 to amke the format similar to crm_cust_info */
INSERT INTO silver.erp_cust_az12 (cid,bdate, gen)

SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid)) /* remove nas infront of cust id if present*/
     ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
     ELSE bdate 
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen /* standardize gender values and handle unknown cases */
FROM bronze.erp_cust_az12

/*check if we have really old dates as the birthdate e.g more than 100 years */
/* check if we have birthdates that are in the future */
SELECT DISTINCT 
bdate 
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

/*Standardize the gender column */
SELECT DISTINCT gen
FROM silver.erp_cust_az12

SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

SELECT * FROM [silver].[crm_cust_info];

/**END ERP CUSTOMER FORMAT **************************************************************/

/**START ERP LOCATION FORMAT **************************************************************/
SELECT *
FROM bronze.erp_loc_a101

INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT DISTINCT
REPLACE(cid, '-', '') cid, /* remove '-' to standardize to be the same as the id in other tables*/
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101 /* WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info) */
/* checking that cid values are the same in bronze.erp_loc_a101 as silver.crm_cust_info */

SELECT 
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101

/* Data standardization & Consistency */
/*Check */
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry

SELECT DISTINCT 
cntry AS old_cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry
/**END ERP LOCATION FORMAT **************************************************************/

/**START ERP Px cat/ product category FORMAT **************************************************************/
INSERT INTO silver.erp_px_cat_g1v2 (
id,
cat,
subcat,
maintenance)

SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

/*check for unwanted space in cat or subcat or maintenance */
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

/* Data standardization & consistency*/
/*CHECK*/
SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2
/* ALL columns are fine, theres nothing to clean */

SELECT *
FROM silver.erp_px_cat_g1v2
