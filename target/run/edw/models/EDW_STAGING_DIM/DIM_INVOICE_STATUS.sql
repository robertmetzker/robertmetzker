

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_INVOICE_STATUS  as
      (

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(INVOICE_STATUS_HKEY) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as Invoice_Status_HKey,
     first_value(MEDICAL_INVOICE_STATUS_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as Medical_Invoice_Status_Code,
     last_value(MEDICAL_INVOICE_STATUS_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as Medical_Invoice_Status_Desc
     FROM EDW_STAGING.DIM_INVOICE_STATUS_SCDALL_STEP2)

----ETL LAYER----
,
ETL1 as ( SELECT *,
	current_date as Load_Datetime,
	current_date as Update_Datetime,
	'CAM' as Primary_Source_System
		from SCD)

SELECT * FROM ETL1
      );
    