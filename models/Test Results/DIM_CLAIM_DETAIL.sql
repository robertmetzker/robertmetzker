{{ config( 
post_hook = ("
ALTER TABLE EDW_STAGING_DIM.DIM_CLAIM_DETAIL ADD PRIMARY KEY (CLAIM_DETAIL_HKEY);
ALTER TABLE EDW_STAGING_DIM.DIM_CLAIM_DETAIL MODIFY CLAIM_DETAIL_HKEY SET NOT NULL;
ALTER TABLE EDW_STAGING_DIM.DIM_CLAIM_DETAIL MODIFY LOAD_DATETIME SET NOT NULL;
ALTER TABLE EDW_STAGING_DIM.DIM_CLAIM_DETAIL MODIFY PRIMARY_SOURCE_SYSTEM SET NOT NULL;
ALTER TABLE EDW_STAGING_DIM.DIM_CLAIM_DETAIL MODIFY PRIMARY_SOURCE_SYSTEM VARCHAR();
INSERT INTO EDW_STAGING_DIM.DIM_CLAIM_DETAIL (CLAIM_DETAIL_HKEY,  UNIQUE_ID_KEY, NATURE_OF_INJURY_CATEGORY, NATURE_OF_INJURY_TYPE, FILING_SOURCE_DESC, FILING_MEDIA_DESC, LOAD_DATETIME, PRIMARY_SOURCE_SYSTEM) SELECT MD5($1), MD5($2), $3, $4, $5, $6, $7,$8 FROM VALUES ('99999', '99999', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', SYSDATE(), 'MANUAL ENTRY');
")  
) }}

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY
	, last_value(CLAIM_DETAIL_HKEY) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_DETAIL_HKEY 
	, last_value(FILING_SOURCE_DESC) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FILING_SOURCE_DESC 
	, last_value(FILING_MEDIA_DESC) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FILING_MEDIA_DESC 
	, last_value(NATURE_OF_INJURY_CATEGORY) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NATURE_OF_INJURY_CATEGORY 
	, last_value(NATURE_OF_INJURY_TYPE) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NATURE_OF_INJURY_TYPE 
	, last_value(FIREFIGHTER_CANCER_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FIREFIGHTER_CANCER_IND 
	, last_value(COVID_EXPOSURE_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as COVID_EXPOSURE_IND 
	, last_value(COVID_EMERGENCY_WORKER_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as COVID_EMERGENCY_WORKER_IND 
	, last_value(COVID_HEALTH_CARE_WORKER_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as COVID_HEALTH_CARE_WORKER_IND 
	, last_value(COMBINED_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as COMBINED_IND 
	, last_value(SB223_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SB223_IND 
	, last_value(EMPLOYER_PREMISES_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EMPLOYER_PREMISES_IND 
	, last_value(CATASTROPHIC_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CATASTROPHIC_IND 
	, last_value(EMPLOYER_PAID_PROGRAM_ENROLLMENT_DESC) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EMPLOYER_PAID_PROGRAM_ENROLLMENT_DESC 
	, last_value(EMPLOYER_PAID_PROGRAM_TYPE) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EMPLOYER_PAID_PROGRAM_TYPE 
	, last_value(EMPLOYER_PAID_PROGRAM_REASON) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EMPLOYER_PAID_PROGRAM_REASON 
	, last_value(FILING_SOURCE_DESC) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FILING_SOURCE_DESC 
	, last_value(FILING_MEDIA_DESC) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FILING_MEDIA_DESC 
	, last_value(NATURE_OF_INJURY_CATEGORY) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NATURE_OF_INJURY_CATEGORY 
	, last_value(NATURE_OF_INJURY_TYPE) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NATURE_OF_INJURY_TYPE 
	, last_value(FIREFIGHTER_CANCER_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FIREFIGHTER_CANCER_IND 
	, last_value(COVID_EXPOSURE_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as COVID_EXPOSURE_IND 
	, last_value(COVID_EMERGENCY_WORKER_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as COVID_EMERGENCY_WORKER_IND 
	, last_value(COVID_HEALTH_CARE_WORKER_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as COVID_HEALTH_CARE_WORKER_IND 
	, last_value(COMBINED_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as COMBINED_IND 
	, last_value(SB223_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SB223_IND 
	, last_value(EMPLOYER_PREMISES_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EMPLOYER_PREMISES_IND 
	, last_value(CATASTROPHIC_IND) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CATASTROPHIC_IND 
	, last_value(EMPLOYER_PAID_PROGRAM_ENROLLMENT_DESC) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EMPLOYER_PAID_PROGRAM_ENROLLMENT_DESC 
	, last_value(EMPLOYER_PAID_PROGRAM_TYPE) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EMPLOYER_PAID_PROGRAM_TYPE 
	, last_value(EMPLOYER_PAID_PROGRAM_REASON) over 
            (partition by UNIQUE_ID_KEY 
              order by dbt_updated_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EMPLOYER_PAID_PROGRAM_REASON
	    , DBT_VALID_FROM AS EFFECTIVE_TIMESTAMP
    	, DBT_VALID_TO   AS END_TIMESTAMP
    	,
    	  CASE WHEN CAST(DBT_VALID_FROM AS DATE) = '1901-01-01' then cast( DBT_VALID_FROM AS DATE )
    	    WHEN cast( DBT_VALID_FROM AS DATE ) <> '1901-01-01' THEN dateadd(day,1,CAST(DBT_VALID_FROM AS DATE))
    	  else cast( DBT_VALID_FROM AS DATE ) end as EFFECTIVE_DATE
    	, cast( DBT_VALID_TO AS DATE ) as END_DATE
    	, CASE WHEN DBT_VALID_TO IS NULL THEN 'Y' ELSE 'N' END AS CURRENT_INDICATOR
     
	FROM {{ ref('DIM_CLAIM_DETAIL_SCDALL_STEP2') }})

select * from SCD
