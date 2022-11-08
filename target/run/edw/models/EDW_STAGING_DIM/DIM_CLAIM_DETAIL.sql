

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_CLAIM_DETAIL  as
      (

WITH  SCD AS ( 
	SELECT  
     UNIQUE_ID_KEY , 
     last_value(FILING_SOURCE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FILING_SOURCE_DESC, 
     last_value(FILING_MEDIA_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FILING_MEDIA_DESC, 
     last_value(NATURE_OF_INJURY_CATEGORY) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NATURE_OF_INJURY_CATEGORY, 
     last_value(NATURE_OF_INJURY_TYPE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NATURE_OF_INJURY_TYPE, 
     last_value(FIREFIGHTER_CANCER_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FIREFIGHTER_CANCER_IND, 
     last_value(COVID_EXPOSURE_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as COVID_EXPOSURE_IND, 
     last_value(COVID_EMERGENCY_WORKER_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as COVID_EMERGENCY_WORKER_IND, 
     last_value(COVID_HEALTH_CARE_WORKER_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as COVID_HEALTH_CARE_WORKER_IND, 
     last_value(COMBINED_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as COMBINED_IND, 
     last_value(SB223_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SB223_IND, 
     last_value(EMPLOYER_PREMISES_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EMPLOYER_PREMISES_IND, 
     last_value(CATASTROPHIC_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CATASTROPHIC_IND, 
     last_value(K_PROGRAM_ENROLLMENT_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EMPLOYER_PAID_PROGRAM_ENROLLMENT_DESC,
     last_value(K_PROGRAM_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EMPLOYER_PAID_PROGRAM_TYPE, 
     last_value(K_PROGRAM_REASON_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EMPLOYER_PAID_PROGRAM_REASON
     
	FROM EDW_STAGING.DIM_CLAIM_DETAIL_SCDALL_STEP2),

ETL AS (
select DISTINCT
     md5(cast(
    
    coalesce(cast(FILING_SOURCE_DESC as 
    varchar
), '') || '-' || coalesce(cast(FILING_MEDIA_DESC as 
    varchar
), '') || '-' || coalesce(cast(NATURE_OF_INJURY_CATEGORY as 
    varchar
), '') || '-' || coalesce(cast(NATURE_OF_INJURY_TYPE as 
    varchar
), '') || '-' || coalesce(cast(FIREFIGHTER_CANCER_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_EXPOSURE_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_EMERGENCY_WORKER_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_HEALTH_CARE_WORKER_IND as 
    varchar
), '') || '-' || coalesce(cast(COMBINED_IND as 
    varchar
), '') || '-' || coalesce(cast(SB223_IND as 
    varchar
), '') || '-' || coalesce(cast(EMPLOYER_PREMISES_IND as 
    varchar
), '') || '-' || coalesce(cast(CATASTROPHIC_IND as 
    varchar
), '') || '-' || coalesce(cast(EMPLOYER_PAID_PROGRAM_ENROLLMENT_DESC as 
    varchar
), '') || '-' || coalesce(cast(EMPLOYER_PAID_PROGRAM_TYPE as 
    varchar
), '') || '-' || coalesce(cast(EMPLOYER_PAID_PROGRAM_REASON as 
    varchar
), '')

 as 
    varchar
)) AS CLAIM_DETAIL_HKEY 
    , * 
    , CURRENT_TIMESTAMP AS  LOAD_DATETIME
    , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
    , 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
    from SCD
    qualify(ROW_NUMBER() over(PARTITION BY  FILING_SOURCE_DESC, FILING_MEDIA_DESC, NATURE_OF_INJURY_CATEGORY, NATURE_OF_INJURY_TYPE, FIREFIGHTER_CANCER_IND, COVID_EXPOSURE_IND, COVID_EMERGENCY_WORKER_IND, COVID_HEALTH_CARE_WORKER_IND, COMBINED_IND, SB223_IND, EMPLOYER_PREMISES_IND, CATASTROPHIC_IND, EMPLOYER_PAID_PROGRAM_ENROLLMENT_DESC, EMPLOYER_PAID_PROGRAM_TYPE, EMPLOYER_PAID_PROGRAM_REASON order by UNIQUE_ID_KEY ) ) = 1
)

SELECT * FROM ETL
      );
    