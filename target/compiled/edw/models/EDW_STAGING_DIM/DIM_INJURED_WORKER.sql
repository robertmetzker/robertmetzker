 

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
		last_value(CUSTOMER_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CUSTOMER_NUMBER,
        last_value(FORMATTED_TAX_ID_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FORMATTED_TAX_ID_NUMBER,     
	-- last_value(COURTESY_TITLE_NAME) over 
    --     (partition by UNIQUE_ID_KEY 
    --     order by dbt_updated_at 
    --         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    --         ) as COURTESY_TITLE_NAME, 
    --  last_value(FIRST_NAME) over 
    --     (partition by UNIQUE_ID_KEY 
    --     order by dbt_updated_at 
    --         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    --         ) as FIRST_NAME, 
     last_value(GENDER_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as GENDER_TYPE_CODE, 			
     last_value(GENDER_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as GENDER_TYPE_DESC, 
     last_value(BIRTH_DATE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BIRTH_DATE, 
     last_value(RETIREMENT_DATE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as RETIREMENT_DATE, 
     last_value(DEATH_DATE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEATH_DATE, 
     last_value(DEATH_ENTRY_DATE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEATH_ENTRY_DATE, 
			last_value(FOREIGN_CITIZEN_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FOREIGN_CITIZEN_IND, 
     last_value(PRIMARY_LANGUAGE_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRIMARY_LANGUAGE_TYPE_DESC, 
     last_value(DOCUMENT_BLOCK_IND) over 
         (partition by UNIQUE_ID_KEY 
         order by dbt_updated_at 
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
             ) as DOCUMENT_BLOCK_IND, 
     last_value(THREATENING_BEHAVIOR_BLOCK_IND) over 
         (partition by UNIQUE_ID_KEY 
         order by dbt_updated_at 
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
             ) as THREATENING_BEHAVIOR_BLOCK_IND, 
     last_value(PARTICIPATION_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PARTICIPATION_TYPE_CODE, 
     last_value(PARTICIPATION_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PARTICIPATION_TYPE_DESC,  
 COURTESY_TITLE_NAME, 
 FIRST_NAME, 
 MIDDLE_NAME, 
 LAST_NAME, 
 SUFFIX_NAME, 
 FULL_NAME,
 MARITAL_STATUS_TYPE_DESC, 
 PHYSICAL_STREET_ADDRESS_1, 
 PHYSICAL_STREET_ADDRESS_2, 
 PHYSICAL_ADDRESS_CITY_NAME, 
 PHYSICAL_ADDRESS_STATE_CODE, 
 PHYSICAL_ADDRESS_STATE_NAME, 
 PHYSICAL_ADDRESS_POSTAL_CODE, 
 PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE, 
 PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE, 
 PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE, 
 PHYSICAL_ADDRESS_COUNTY_NAME, 
 PHYSICAL_ADDRESS_COUNTRY_NAME, 
 PHYSICAL_ADDRESS_VALIDATED_IND, 
 PHYSICAL_ADDRESS_COMMENT_TEXT, 
 MAILING_STREET_ADDRESS_1, 
 MAILING_STREET_ADDRESS_2, 
 MAILING_ADDRESS_CITY_NAME, 
 MAILING_ADDRESS_STATE_CODE, 
 MAILING_ADDRESS_STATE_NAME, 
 MAILING_ADDRESS_POSTAL_CODE, 
 MAILING_FORMATTED_ADDRESS_POSTAL_CODE, 
 MAILING_FORMATTED_ADDRESS_ZIP_CODE, 
 MAILING_FORMATTED_ADDRESS_ZIP4_CODE, 
 MAILING_ADDRESS_COUNTY_NAME, 
 MAILING_ADDRESS_COUNTRY_NAME, 
 MAILING_ADDRESS_VALIDATED_IND, 
 MAILING_ADDRESS_COMMENT_TEXT
,DBT_VALID_FROM AS EFFECTIVE_TIMESTAMP 
,DBT_VALID_TO   AS END_TIMESTAMP 
,
COALESCE(CASE WHEN (ROW_NUMBER() OVER (PARTITION BY UNIQUE_ID_KEY ORDER BY DBT_VALID_FROM )) = 1 
      AND  CAST(DBT_VALID_FROM AS DATE) <> '1901-01-01' THEN CAST(DBT_VALID_FROM AS DATE)
    WHEN CAST(DBT_VALID_FROM AS DATE) = '1901-01-01' then CAST(DBT_VALID_FROM AS DATE)
      WHEN CAST(DBT_VALID_FROM AS DATE) <> '1901-01-01' THEN dateadd(day,1,CAST(DBT_VALID_FROM AS DATE))
    else CAST(DBT_VALID_FROM AS DATE) end,'1901-01-01'::DATE) as EFFECTIVE_DATE
,
    CAST(DBT_VALID_TO AS DATE) as END_DATE 
,CASE WHEN DBT_VALID_TO IS NULL THEN 'Y' ELSE 'N' END AS CURRENT_INDICATOR
     
	FROM EDW_STAGING.DIM_INJURED_WORKER_SCDALL_STEP2)

--
 -- ETL LAYER --
, ETL AS (select 
  UNIQUE_ID_KEY
, md5(cast(
    
    coalesce(cast(CUSTOMER_NUMBER as 
    varchar
), '') || '-' || coalesce(cast(EFFECTIVE_DATE as 
    varchar
), '')

 as 
    varchar
)) as INJURED_WORKER_HKEY
, CUSTOMER_NUMBER
, COURTESY_TITLE_NAME
, FIRST_NAME
, MIDDLE_NAME
, LAST_NAME
, SUFFIX_NAME
, FULL_NAME
, FORMATTED_TAX_ID_NUMBER
, GENDER_TYPE_CODE
, GENDER_TYPE_DESC
, MARITAL_STATUS_TYPE_DESC
, BIRTH_DATE
, RETIREMENT_DATE
, DEATH_DATE
, DEATH_ENTRY_DATE
, FOREIGN_CITIZEN_IND
, PRIMARY_LANGUAGE_TYPE_DESC
, DOCUMENT_BLOCK_IND
, THREATENING_BEHAVIOR_BLOCK_IND
, PHYSICAL_STREET_ADDRESS_1
, PHYSICAL_STREET_ADDRESS_2
, PHYSICAL_ADDRESS_CITY_NAME
, PHYSICAL_ADDRESS_STATE_CODE
, PHYSICAL_ADDRESS_STATE_NAME
, PHYSICAL_ADDRESS_POSTAL_CODE
, PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE
, PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE
, PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE
, PHYSICAL_ADDRESS_COUNTY_NAME
, PHYSICAL_ADDRESS_COUNTRY_NAME
, PHYSICAL_ADDRESS_VALIDATED_IND
, PHYSICAL_ADDRESS_COMMENT_TEXT
, MAILING_STREET_ADDRESS_1
, MAILING_STREET_ADDRESS_2
, MAILING_ADDRESS_CITY_NAME
, MAILING_ADDRESS_STATE_CODE
, MAILING_ADDRESS_STATE_NAME
, MAILING_ADDRESS_POSTAL_CODE
, MAILING_FORMATTED_ADDRESS_POSTAL_CODE
, MAILING_FORMATTED_ADDRESS_ZIP_CODE
, MAILING_FORMATTED_ADDRESS_ZIP4_CODE
, MAILING_ADDRESS_COUNTY_NAME
, MAILING_ADDRESS_COUNTRY_NAME
, MAILING_ADDRESS_VALIDATED_IND
, MAILING_ADDRESS_COMMENT_TEXT
, PARTICIPATION_TYPE_CODE
, PARTICIPATION_TYPE_DESC
, CASE WHEN END_DATE IS NULL THEN 'Y' ELSE 'N' END AS CURRENT_RECORD_IND
, EFFECTIVE_DATE AS RECORD_EFFECTIVE_DATE
, END_DATE AS RECORD_END_DATE
, EFFECTIVE_TIMESTAMP AS LOAD_DATETIME
, END_TIMESTAMP AS UPDATE_DATETIME
, 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM
 from SCD)

SELECT * FROM ETL