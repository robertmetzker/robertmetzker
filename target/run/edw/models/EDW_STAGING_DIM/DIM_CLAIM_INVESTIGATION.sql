

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_CLAIM_INVESTIGATION  as
      ( 

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(CLAIM_ACP_STATUS_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_ACP_STATUS_IND, 
     last_value(ACP_MANUAL_INTERVENTION_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ACP_MANUAL_INTERVENTION_IND, 
     last_value(JURISDICTION_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as JURISDICTION_TYPE_CODE
	FROM EDW_STAGING.DIM_CLAIM_INVESTIGATION_SCDALL_STEP2),
ETL AS (
select Distinct md5(cast(
    
    coalesce(cast(CLAIM_ACP_STATUS_IND as 
    varchar
), '') || '-' || coalesce(cast(ACP_MANUAL_INTERVENTION_IND as 
    varchar
), '') || '-' || coalesce(cast(JURISDICTION_TYPE_CODE as 
    varchar
), '')

 as 
    varchar
)) AS CLAIM_INVESTIGATION_HKEY
    ,UNIQUE_ID_KEY
    ,CLAIM_ACP_STATUS_IND
    ,ACP_MANUAL_INTERVENTION_IND
    ,JURISDICTION_TYPE_CODE
    ,CURRENT_TIMESTAMP AS  LOAD_DATETIME
    ,TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
    ,'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
    from SCD
    qualify(ROW_NUMBER() over(PARTITION BY CLAIM_ACP_STATUS_IND, ACP_MANUAL_INTERVENTION_IND, JURISDICTION_TYPE_CODE order by UNIQUE_ID_KEY ) ) = 1
)

select * from ETL
      );
    