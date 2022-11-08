 

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(PBM_PRICING_METHOD_HKEY) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PBM_PRICING_METHOD_HKEY,
     last_value(PRICE_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRICE_TYPE_CODE,
     last_value(PBM_PRICING_SOURCE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PBM_PRICING_SOURCE_CODE,
     last_value(PRICE_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRICE_TYPE_DESC,
     last_value(PBM_PRICING_SOURCE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PBM_PRICING_SOURCE_DESC
     FROM EDW_STAGING.DIM_PBM_PRICING_METHOD_SCDALL_STEP2)
-----ETL LAYER----
,
    ETL1 as (
        select PBM_PRICING_METHOD_HKEY,
               UNIQUE_ID_KEY,
               PRICE_TYPE_CODE,
               PBM_PRICING_SOURCE_CODE,
               PRICE_TYPE_DESC,
               PBM_PRICING_SOURCE_DESC,
               current_timestamp() as LOAD_DATETIME,
               try_to_timestamp('invalid') as UPDATE_DATETIME,
               'CAM' as PRIMARY_SOURCE_SYSTEM
        from SCD)

SELECT * FROM ETL1