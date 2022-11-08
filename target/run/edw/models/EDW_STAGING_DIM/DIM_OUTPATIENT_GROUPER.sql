

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_OUTPATIENT_GROUPER  as
      ( 

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(APC_STATUS_INDICATOR_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as APC_STATUS_INDICATOR_CODE,
     last_value(OPPS_RETURN_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OPPS_RETURN_CODE,
     last_value(OPPS_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OPPS_CODE,
     last_value(OPPS_PAYMENT_METHOD_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OPPS_PAYMENT_METHOD_CODE,
     last_value(APC_DISCOUNTING_FRACTION_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as APC_DISCOUNTING_FRACTION_CODE,
     last_value(APC_COMPOSITE_ADJUSTMENT_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as APC_COMPOSITE_ADJUSTMENT_CODE,
     last_value(APC_PAYMENT_INDICATOR_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as APC_PAYMENT_INDICATOR_CODE,
     last_value(APC_PACKAGING_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as APC_PACKAGING_CODE,
     last_value(APC_PAYMENT_ADJUSTMENT_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as APC_PAYMENT_ADJUSTMENT_CODE,
     last_value(APC_STATUS_INDICATOR_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as APC_STATUS_INDICATOR_DESC,
     last_value(OPPS_RETURN_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OPPS_RETURN_DESC,
     last_value(OPPS_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OPPS_DESC,
     last_value(OPPS_PAYMENT_METHOD_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OPPS_PAYMENT_METHOD_DESC,
     last_value(APC_DISCOUNTING_FRACTION_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as APC_DISCOUNTING_FRACTION_DESC,
     last_value(APC_COMPOSITE_ADJUSTMENT_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as APC_COMPOSITE_ADJUSTMENT_DESC,
     last_value(APC_PAYMENT_INDICATOR_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as APC_PAYMENT_INDICATOR_DESC,
     last_value(APC_PACKAGING_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as APC_PACKAGING_DESC,
     last_value(APC_PAYMENT_ADJUSTMENT_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as APC_PAYMENT_ADJUSTMENT_DESC
     FROM EDW_STAGING.DIM_OUTPATIENT_GROUPER_SCDALL_STEP2),

-----ETL LAYER-----
    ETL1 AS (
select md5(cast(
    
    coalesce(cast(APC_STATUS_INDICATOR_CODE as 
    varchar
), '') || '-' || coalesce(cast(OPPS_RETURN_CODE as 
    varchar
), '') || '-' || coalesce(cast(OPPS_CODE as 
    varchar
), '') || '-' || coalesce(cast(OPPS_PAYMENT_METHOD_CODE as 
    varchar
), '') || '-' || coalesce(cast(APC_DISCOUNTING_FRACTION_CODE as 
    varchar
), '') || '-' || coalesce(cast(APC_COMPOSITE_ADJUSTMENT_CODE as 
    varchar
), '') || '-' || coalesce(cast(APC_PAYMENT_INDICATOR_CODE as 
    varchar
), '') || '-' || coalesce(cast(APC_PACKAGING_CODE as 
    varchar
), '') || '-' || coalesce(cast(APC_PAYMENT_ADJUSTMENT_CODE as 
    varchar
), '')

 as 
    varchar
)) as GROUPER_HKEY 
            ,UNIQUE_ID_KEY
            , APC_STATUS_INDICATOR_CODE
            , OPPS_RETURN_CODE
            , OPPS_CODE
            , OPPS_PAYMENT_METHOD_CODE
            , APC_DISCOUNTING_FRACTION_CODE
            , APC_COMPOSITE_ADJUSTMENT_CODE
            , APC_PAYMENT_INDICATOR_CODE
            , APC_PACKAGING_CODE
            , APC_PAYMENT_ADJUSTMENT_CODE
            , CASE WHEN APC_STATUS_INDICATOR_CODE IS NOT NULL
                    AND APC_STATUS_INDICATOR_DESC IS NULL THEN 'INVALID' 
                    ELSE APC_STATUS_INDICATOR_DESC  END AS APC_STATUS_INDICATOR_DESC
            , CASE WHEN OPPS_RETURN_CODE IS NOT NULL
                    AND OPPS_RETURN_DESC IS NULL THEN 'INVALID' 
                    ELSE OPPS_RETURN_DESC 
                    END AS OPPS_RETURN_DESC
            , CASE WHEN OPPS_CODE IS NOT NULL
                    AND OPPS_DESC IS NULL THEN 'INVALID'
                     ELSE OPPS_DESC 
                    END AS OPPS_DESC
            , CASE WHEN OPPS_PAYMENT_METHOD_CODE IS NOT NULL
                    AND OPPS_PAYMENT_METHOD_DESC IS NULL THEN 'INVALID'
                    ELSE OPPS_PAYMENT_METHOD_DESC 
                    END AS OPPS_PAYMENT_METHOD_DESC
            , CASE WHEN APC_DISCOUNTING_FRACTION_CODE IS NOT NULL
                    AND APC_DISCOUNTING_FRACTION_DESC IS NULL THEN 'INVALID'
                     ELSE APC_DISCOUNTING_FRACTION_DESC 
                    END AS APC_DISCOUNTING_FRACTION_DESC
            , CASE WHEN APC_COMPOSITE_ADJUSTMENT_CODE IS NOT NULL
                    AND APC_COMPOSITE_ADJUSTMENT_DESC IS NULL THEN 'INVALID'
                     ELSE APC_COMPOSITE_ADJUSTMENT_DESC 
                    END AS APC_COMPOSITE_ADJUSTMENT_DESC
            , CASE WHEN APC_PAYMENT_INDICATOR_CODE IS NOT NULL
                    AND APC_PAYMENT_INDICATOR_DESC IS NULL THEN 'INVALID'
                     ELSE APC_PAYMENT_INDICATOR_DESC 
                    END AS APC_PAYMENT_INDICATOR_DESC
            , CASE WHEN APC_PACKAGING_CODE IS NOT NULL
                    AND APC_PACKAGING_DESC IS NULL THEN 'INVALID'
                     ELSE APC_PACKAGING_DESC 
                    END AS APC_PACKAGING_DESC
            , CASE WHEN APC_PAYMENT_ADJUSTMENT_CODE IS NOT NULL
                    AND APC_PAYMENT_ADJUSTMENT_DESC IS NULL THEN 'INVALID'
                     ELSE APC_PAYMENT_ADJUSTMENT_DESC 
                    END AS APC_PAYMENT_ADJUSTMENT_DESC
            , CURRENT_TIMESTAMP AS LOAD_DATETIME
            , try_to_timestamp('invalid') AS UPDATE_DATETIME
            , 'CAM' AS PRIMARY_SOURCE_SYSTEM 
            from SCD)

SELECT * FROM ETL1
      );
    