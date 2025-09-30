/*
==================================================================================
CREATE DATABASE AND SCHEMAS - SALES_DWH PROJECT
==================================================================================

Script Purpose:
    This script provisions the required Snowflake objects to support the 
    Sales Data Warehouse project built using DBT, Snowflake, and S3.  

    It automates the creation of:
      - Roles and users (for DBT transformations)
      - Warehouse, database, and schemas (Bronze, Silver, Gold layers)
      - Grants/permissions for the `TRANSFORM` role
      - External stage for loading raw ERP and CRM files from S3

Key Steps:
    1. Role creation and assignment
    2. Warehouse creation
    3. User setup for DBT
    4. Database and schema setup
    5. Role grants and permissions
    6. Stage creation for raw data ingestion
    7. Data load preparation

WARNING:
    - Replace placeholder values (`<choose-a-username>`, `<your-s3-bucket-name>`, 
      `<your-aws-key-ID>`, `<your-aws-secret-key>`) with actual values.  
    - Running this script requires ACCOUNTADMIN or equivalent privileges.  
    - AWS credentials provided here must have access to the specified S3 bucket.  
    - Secure handling of secrets is required—prefer AWS IAM role integration over raw keys.  

==================================================================================
*/

-- Step 1: Use an admin role
USE ROLE ACCOUNTADMIN;

-- Step 2: Create the `transform` role and assign it to ACCOUNTADMIN
CREATE OR REPLACE ROLE TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Step 3: Create a default warehouse
CREATE OR REPLACE WAREHOUSE COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- Step 4: Create the `dbt` user and assign to the transform role
CREATE OR REPLACE USER dbt
  PASSWORD='<choose-a-password>'
  LOGIN_NAME='<choose-a-username>'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE=TRANSFORM
  DEFAULT_NAMESPACE='MOVIELENS.RAW'
  COMMENT='DBT user used for data transformation';
  
ALTER USER dbt SET TYPE = LEGACY_SERVICE;
GRANT ROLE TRANSFORM TO USER dbt;

-- Step 5: Create a database and schema for the Sales_dwh project
CREATE OR REPLACE DATABASE SALES_DWH;
CREATE OR REPLACE SCHEMA SALES_DWH.BRONZE;
CREATE OR REPLACE SCHEMA SALES_DWH.SILVER;
CREATE OR REPLACE SCHEMA SALES_DWH.GOLD;

-- Step 6: Grant permissions to the `transform` role
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;
GRANT ALL ON DATABASE SALES_DWH TO ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE SALES_DWH TO ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE SALES_DWH TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA SALES_DWH.BRONZE TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA  SALES_DWH.BRONZE TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA SALES_DWH.SILVER TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA  SALES_DWH.SILVER TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA SALES_DWH.GOLD TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA  SALES_DWH.GOLD TO ROLE TRANSFORM;


-- Step 6: Create stage to store data files before loading to tables
CREATE STAGE SALES_DWH.BRONZE.SALES_STAGE
  URL='s3://<your-s3-bucket-name>'
  DIRECTORY = (ENABLE = TRUE)
  CREDENTIALS=(AWS_KEY_ID='<your-aws-key-ID>' AWS_SECRET_KEY='<your-aws-secret-key>');

ALTER STAGE SALES_DWH.BRONZE.SALES_STAGE REFRESH;



-- Step 7: Create DDL to define Database Tables
/*
=======================================================================
 Create DDL to define Database Tables
=======================================================================
- Data Completeness and schema check,
- Data ingestion from S3 to Bronze Layer
=======================================================================
*/

CREATE OR REPLACE TABLE SALES_DWH.BRONZE.CRM_CUST_INFO(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);

CREATE OR REPLACE TABLE SALES_DWH.BRONZE.CRM_PRD_INFO(
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);

CREATE OR REPLACE TABLE SALES_DWH.BRONZE.CRM_SALES_DETAILS(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);


CREATE OR REPLACE TABLE SALES_DWH.BRONZE.ERP_CUST_AZ12(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);

CREATE OR REPLACE TABLE SALES_DWH.BRONZE.ERP_LOC_A101(
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);

CREATE OR REPLACE TABLE SALES_DWH.BRONZE.ERP_PX_CAT_G1V2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenace NVARCHAR(50)
);


-- Step 8: Create stored procedure to load raw data to bronze
/*
=======================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=======================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external stage.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY INTO` command to load data from stage to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC SALES_DWH.BRONZE.load_bronze_data();
=======================================================================
*/
CREATE OR REPLACE PROCEDURE SALES_DWH.BRONZE.load_bronze_data()
  RETURNS STRING NOT NULL
  LANGUAGE JAVASCRIPT
AS
$$  
    var steps = [
        {
            table: "SALES_DWH.BRONZE.CRM_CUST_INFO",
            copy: `COPY INTO SALES_DWH.BRONZE.CRM_CUST_INFO(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
                   FROM '@SALES_DWH.BRONZE.SALES_STAGE/cust_info.csv'
                   FILE_FORMAT = (type='CSV', field_delimiter=',', skip_header=1, field_optionally_enclosed_by='"');`
        },
        {
            table: "SALES_DWH.BRONZE.CRM_PRD_INFO",
            copy: `COPY INTO SALES_DWH.BRONZE.CRM_PRD_INFO(prd_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
                   FROM '@SALES_DWH.BRONZE.SALES_STAGE/prd_info.csv'
                   FILE_FORMAT = (type='CSV', field_delimiter=',', skip_header=1, field_optionally_enclosed_by='"');`
        },
        {
            table: "SALES_DWH.BRONZE.CRM_SALES_DETAILS",
            copy: `COPY INTO SALES_DWH.BRONZE.CRM_SALES_DETAILS(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
                   FROM '@SALES_DWH.BRONZE.SALES_STAGE/sales_details.csv'
                   FILE_FORMAT = (type='CSV', field_delimiter=',', skip_header=1, field_optionally_enclosed_by='"');`
        },
        {
            table: "SALES_DWH.BRONZE.ERP_CUST_AZ12",
            copy: `COPY INTO SALES_DWH.BRONZE.ERP_CUST_AZ12(cid,bdate,gen)
                   FROM '@SALES_DWH.BRONZE.SALES_STAGE/CUST_AZ12.csv'
                   FILE_FORMAT = (type='CSV', field_delimiter=',', skip_header=1, field_optionally_enclosed_by='"');`
        },
        {
            table: "SALES_DWH.BRONZE.ERP_LOC_A101",
            copy: `COPY INTO SALES_DWH.BRONZE.ERP_LOC_A101(cid,cntry)
                   FROM '@SALES_DWH.BRONZE.SALES_STAGE/LOC_A101.csv'
                   FILE_FORMAT = (type='CSV', field_delimiter=',', skip_header=1, field_optionally_enclosed_by='"');`
        },
        {
            table: "SALES_DWH.BRONZE.ERP_PX_CAT_G1V2",
            copy: `COPY INTO SALES_DWH.BRONZE.ERP_PX_CAT_G1V2(id,cat,subcat,maintenace)
                   FROM '@SALES_DWH.BRONZE.SALES_STAGE/PX_CAT_G1V2.csv'
                   FILE_FORMAT = (type='CSV', field_delimiter=',', skip_header=1, field_optionally_enclosed_by='"');`
        }
    ];
    
    var log = [];
    
    for (var i = 0; i < steps.length; i++) {
        try {
            // Step 1: Truncate the table
            snowflake.execute({sqlText: `TRUNCATE TABLE ${steps[i].table};`});
            log.push("✅ Truncated " + steps[i].table);
            
            // Step 2: Load data using COPY INTO
            snowflake.execute({sqlText: steps[i].copy});
            log.push("📥 Loaded data into " + steps[i].table);
        } catch (err) {
            return "❌ Error in step " + (i+1) + " for table " + steps[i].table + ": " + err; 
        }
    }

    return "🚀 Bronze layer load completed successfully.\nProgress Log:\n" + log.join("\n");
$$;


-- Step 8: Call the procedure to load data
CALL SALES_DWH.BRONZE.load_bronze_data();

-- Step 9: Confirm all data get loaded.
SELECT COUNT(*)
FROM SALES_DWH.BRONZE.CRM_CUST_INFO; -- 18,494 records found

SELECT COUNT(*)
FROM SALES_DWH.BRONZE.CRM_PRD_INFO; -- 397 records found

SELECT COUNT(*)
FROM SALES_DWH.BRONZE.CRM_SALES_DETAILS; -- 60,398 records found

SELECT COUNT(*)
FROM SALES_DWH.BRONZE.ERP_CUST_AZ12; -- 18,484 records found

SELECT COUNT(*)
FROM SALES_DWH.BRONZE.ERP_LOC_A101; -- 18,484 records found

SELECT COUNT(*)
FROM SALES_DWH.BRONZE.ERP_PX_CAT_G1V2; -- 37 records found




