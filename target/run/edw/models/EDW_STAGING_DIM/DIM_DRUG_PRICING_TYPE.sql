

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_DRUG_PRICING_TYPE  as
      ( 

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(PRICE_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRICE_TYPE_CODE,
     last_value(PRICE_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRICE_TYPE_DESC
     FROM EDW_STAGING.DIM_DRUG_PRICING_TYPE_SCDALL_STEP2),
------------- ETL LAYER -----------
ETL AS (select 
        md5(cast(
    
    coalesce(cast(PRICE_TYPE_CODE as 
    varchar
), '')

 as 
    varchar
)) as PRICING_TYPE_HKEY
        ,UNIQUE_ID_KEY
        ,PRICE_TYPE_CODE
        ,PRICE_TYPE_DESC
        ,CURRENT_TIMESTAMP AS LOAD_DATETIME
        ,try_to_TIMESTAMP('Invalid')  AS UPDATE_DATETIME
        ,'RNP' AS PRIMARY_SOURCE_SYSTEM
 from SCD)
SELECT * FROM ETL
      );
    