CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME , @end_time DATETIME, @whole_start_time DATETIME;

    BEGIN TRY

        SET @whole_start_time = GETDATE();
        PRINT '==============================================================================================';
        PRINT '                               STARTING BRONZE LAYER LOAD                                     ';
        PRINT '==============================================================================================';

        -- ================================
        -- CRM Tables
        -- ================================
        PRINT '----------------------------------------------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '----------------------------------------------------------------------------------------------';

        -- crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into : bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time= GETDATE();
        PRINT '>> Data Load Completed : bronze.crm_cust_info in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        -- crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into : bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time= GETDATE();
        PRINT '>> Data Load Completed : bronze.crm_prd_info in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        -- crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into : bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time= GETDATE();
        PRINT '>> Data Load Completed : bronze.crm_sales_details in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        -- ================================
        -- ERP Tables
        -- ================================
        PRINT '----------------------------------------------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '----------------------------------------------------------------------------------------------';

        -- erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into : bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time= GETDATE();
        PRINT '>> Data Load Completed : bronze.erp_loc_a101 in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        -- erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into : bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time= GETDATE();
        PRINT '>> Data Load Completed : bronze.erp_cust_az12 in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        -- erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into : bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time= GETDATE();
        PRINT '>> Data Load Completed : bronze.erp_px_cat_g1v2 in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        SET @end_time= GETDATE();
        PRINT '==============================================================================================';
        PRINT '                               BRONZE LAYER LOAD COMPLETED                                    ';
        PRINT '==============================================================================================';
        PRINT '>> Data Load Completed : Bronze Layer Whole Batch ' 
              + CAST(DATEDIFF(SECOND, @whole_start_time, @end_time) AS NVARCHAR) + ' seconds';
    END TRY
    BEGIN CATCH
        PRINT '==============================================================================================';
        PRINT '                           ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT '----------------------------------------------------------------------------------------------';
        PRINT 'Error Number    : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Severity  : ' + CAST(ERROR_SEVERITY() AS NVARCHAR);
        PRINT 'Error State     : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'Error Line      : ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT 'Error Message   : ' + ERROR_MESSAGE();
        PRINT '==============================================================================================';
    END CATCH
END;
