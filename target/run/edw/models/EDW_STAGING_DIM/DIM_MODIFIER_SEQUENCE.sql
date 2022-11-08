

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_MODIFIER_SEQUENCE  as
      ( 

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
          last_value(MODIFIER_SEQUENCE_1_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MODIFIER_SEQUENCE_1_CODE,
     last_value(MODIFIER_SEQUENCE_2_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MODIFIER_SEQUENCE_2_CODE,
     last_value(MODIFIER_SEQUENCE_3_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MODIFIER_SEQUENCE_3_CODE,
     last_value(MODIFIER_SEQUENCE_4_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MODIFIER_SEQUENCE_4_CODE,
     last_value(MODIFIER_SET_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MODIFIER_SET_CODE,
     last_value(CURRENT_MODIFIER_1_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CURRENT_MODIFIER_1_DESC,
     last_value(CURRENT_MODIFIER_2_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CURRENT_MODIFIER_2_DESC,
     last_value(CURRENT_MODIFIER_3_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CURRENT_MODIFIER_3_DESC,
     last_value(CURRENT_MODIFIER_4_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CURRENT_MODIFIER_4_DESC,
     last_value(MODIFIER_SET_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MODIFIER_SET_DESC

     FROM EDW_STAGING.DIM_MODIFIER_SEQUENCE_SCDALL_STEP2),
ETL AS (  SELECT md5(cast(
    
    coalesce(cast(MODIFIER_SEQUENCE_1_CODE as 
    varchar
), '') || '-' || coalesce(cast(MODIFIER_SEQUENCE_2_CODE as 
    varchar
), '') || '-' || coalesce(cast(MODIFIER_SEQUENCE_3_CODE as 
    varchar
), '') || '-' || coalesce(cast(MODIFIER_SEQUENCE_4_CODE as 
    varchar
), '') || '-' || coalesce(cast(MODIFIER_SET_CODE as 
    varchar
), '')

 as 
    varchar
)) as MODIFIER_SEQUENCE_CODE_HKEY
             ,UNIQUE_ID_KEY
             ,MODIFIER_SEQUENCE_1_CODE
             ,MODIFIER_SEQUENCE_2_CODE
             ,MODIFIER_SEQUENCE_3_CODE
             ,MODIFIER_SEQUENCE_4_CODE
             ,MODIFIER_SET_CODE
             ,CURRENT_MODIFIER_1_DESC
             ,CURRENT_MODIFIER_2_DESC
             ,CURRENT_MODIFIER_3_DESC
             ,CURRENT_MODIFIER_4_DESC
             ,MODIFIER_SET_DESC
             ,CURRENT_TIMESTAMP() AS LOAD_DATETIME
             ,NULL::TIMESTAMP AS UPDATE_DATETIME
             ,'CAM' AS PRIMARY_SOURCE_SYSTEM   FROM SCD                                            
)
select * from ETL
      );
    