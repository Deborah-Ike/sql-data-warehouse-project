/* 
==========================================================================================================================
Purpose: This script creates views fir the gold layer in the data warehouse.
The gold layer represents the final dimension and fact tables.

Each view performs tranformations and combines data from the silver layer to produce a clean, enriched, and business-ready
dataset.
==========================================================================================================================
*/

/* Create dimension: gold.dim_customers

IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
/* Main */
CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, /* Surrogate key*/
 ci.cst_id AS customer_id,
 ci.cst_key AS customer_number,
 ci.cst_firstname AS first_name,
 ci.cst_lastname AS last_name,
 la.cntry AS country,
 ci.cst_marital_status AS marital_status,
 CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr /* CRM is the Master for gender Info */
 ELSE COALESCE(ca.gen, 'n/a')
 END AS gender,
  ca.bdate AS birthdate ,
 ci.cst_create_date AS create_date
 FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

/* Break down of steps */
SELECT cst_id, COUNT(*) FROM
 (SELECT 
 ci.cst_id,
 ci.cst_key,
 ci.cst_firstname,
 ci.cst_lastname,
 ci.cst_marital_status,
 ci.cst_gndr,
 ci.cst_create_date,
 ca.bdate,
 ca.gen,
 la.cntry
 FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
) t GROUP BY cst_id
HAVING COUNT(*) > 1 /* checking for duplicates */

/* what we notice is we have gender coming from crm and erp so here we solve the problem using data integration */
/* FIRST CHECK THAT BOTH COLUMNS AGREE FOR EACH CUSTOMER ID */
SELECT DISTINCT
 ci.cst_gndr,
 ca.gen
 FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER BY 1, 2

/*WE KNOW THAT THE CORRECT INFORMATION SHOULD BE IN THE CRM, HENCE THAT SHOULD OVERRIDE AS THE CORRECT GENDER**/
SELECT DISTINCT
 ci.cst_gndr,
 ca.gen,
 CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr /* CRM is the Master for gender Info */
 ELSE COALESCE(ca.gen, 'n/a')
 END AS new_gen
 FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER BY 1, 2

/* quality control */
SELECT DISTINCT 
gender
FROM gold.dim_customers

/****************************Product tables */

SELECT 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL /* FILTER OUT ALL HISTORICAL DATA*/

/* make sure the product key is unique */
SELECT prd_key, COUNT(*) FROM (
SELECT 
pn.prd_id,
pn.prd_key,
pn.prd_nm,
pn.cat_id,
pc.cat,
pc.subcat,
pc.maintenance,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL
)t GROUP BY prd_key
HAVING COUNT(*) > 1

/*rename columns*/
IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL

/* Foreign Key Integrity*/
SELECT * 
FROM gold.dim_products

/*********************** SALES *********************************/
IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
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
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quanity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

/* Foreign Key Integrity*/
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL
