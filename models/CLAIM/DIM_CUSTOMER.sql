{{ config( 
post_hook = ("
ALTER TABLE EDW_STAGING_DIM.DIM_CUSTOMER ADD PRIMARY KEY (CUSTOMER_HKEY);
ALTER TABLE EDW_STAGING_DIM.DIM_CUSTOMER MODIFY CUSTOMER_HKEY SET NOT NULL;
ALTER TABLE EDW_STAGING_DIM.DIM_CUSTOMER MODIFY LOAD_DATETIME SET NOT NULL;
ALTER TABLE EDW_STAGING_DIM.DIM_CUSTOMER
MODIFY PRIMARY_SOURCE_SYSTEM VARCHAR(); 
ALTER TABLE EDW_STAGING_DIM.DIM_CUSTOMER MODIFY PRIMARY_SOURCE_SYSTEM SET NOT NULL; 
INSERT INTO EDW_STAGING_DIM.DIM_CUSTOMER (CUSTOMER_HKEY,UNIQUE_ID_KEY,CUSTOMER_NUMBER, CUSTOMER_NAME, LOAD_DATETIME,CURRENT_RECORD_IND, RECORD_EFFECTIVE_DATE, PRIMARY_SOURCE_SYSTEM) SELECT MD5($1), MD5($2), $3, $4, $5, $6, $7, $8 FROM  VALUES ('00000','00000','00000', 'UNKNOWN',  SYSDATE(), 'Y' ,'1901-01-01', 'MANUAL ENTRY');
")  
) }}

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(CUSTOMER_HKEY) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CUSTOMER_HKEY, 
     last_value(CUSTOMER_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CUSTOMER_NUMBER, 
     last_value(ROLE_ID_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ROLE_ID_NUMBER, 
     last_value(ROLE_ID_NUMBER_TYPE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ROLE_ID_NUMBER_TYPE, 
     last_value(TAX_ID_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as TAX_ID_TYPE_CODE, 
     last_value(TAX_ID_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as TAX_ID_NUMBER, 
     last_value(PHONE_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHONE_NUMBER, 
     last_value(EMPLOYER_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EMPLOYER_IND, 
     last_value(LEGAL_REPRESENTATIVE_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as LEGAL_REPRESENTATIVE_IND, 
     last_value(INJURED_WORKER_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as INJURED_WORKER_IND, 
     last_value(COVERED_INDIVIDUAL_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as COVERED_INDIVIDUAL_IND, 
     last_value(PROVIDER_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PROVIDER_IND, 
     last_value(NETWORK_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NETWORK_IND, 
     last_value(MISCELLANEOUS_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MISCELLANEOUS_IND, 
     last_value(CURRENT_RECORD_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CURRENT_RECORD_IND, 
     last_value(RECORD_EFFECTIVE_DATE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as RECORD_EFFECTIVE_DATE, 
     last_value(RECORD_END_DATE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as RECORD_END_DATE, 
     last_value(CUSTOMER_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CUSTOMER_NAME, 
     last_value(FIRST_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FIRST_NAME, 
     last_value(MIDDLE_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MIDDLE_NAME, 
     last_value(LAST_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as LAST_NAME, 
     last_value(SUFFIX_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SUFFIX_NAME, 
     last_value(DOCUMENT_BLOCK_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DOCUMENT_BLOCK_IND, 
     last_value(PHYSICAL_ADDRESS_LINE_1) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHYSICAL_ADDRESS_LINE_1, 
     last_value(PHYSICAL_ADDRESS_LINE_2) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHYSICAL_ADDRESS_LINE_2, 
     last_value(PHYSICAL_ADDRESS_CITY_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHYSICAL_ADDRESS_CITY_NAME, 
     last_value(PHYSICAL_ADDRESS_STATE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHYSICAL_ADDRESS_STATE_CODE, 
     last_value(PHYSICAL_ADDRESS_STATE_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHYSICAL_ADDRESS_STATE_NAME, 
     last_value(PHYSICAL_ADDRESS_POSTAL_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHYSICAL_ADDRESS_POSTAL_CODE, 
     last_value(PHYSICAL_ADDRESS_COUNTY_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHYSICAL_ADDRESS_COUNTY_NAME, 
     last_value(PHYSICAL_ADDRESS_COUNTRY_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHYSICAL_ADDRESS_COUNTRY_NAME, 
     last_value(PHYSICAL_ADDRESS_VALIDATED_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHYSICAL_ADDRESS_VALIDATED_IND, 
     last_value(PHYSICAL_ADDRESS_COMMENT_TEXT) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHYSICAL_ADDRESS_COMMENT_TEXT, 
     last_value(PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE, 
     last_value(PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE, 
     last_value(PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE, 
     last_value(MAILING_STREET_ADDRESS_1) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MAILING_STREET_ADDRESS_1, 
     last_value(MAILING_STREET_ADDRESS_2) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MAILING_STREET_ADDRESS_2, 
     last_value(MAILING_ADDRESS_CITY_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MAILING_ADDRESS_CITY_NAME, 
     last_value(MAILING_ADDRESS_STATE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MAILING_ADDRESS_STATE_CODE, 
     last_value(MAILING_ADDRESS_STATE_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MAILING_ADDRESS_STATE_NAME, 
     last_value(MAILING_ADDRESS_POSTAL_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MAILING_ADDRESS_POSTAL_CODE, 
     last_value(MAILING_ADDRESS_COUNTY_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MAILING_ADDRESS_COUNTY_NAME, 
     last_value(MAILING_ADDRESS_COUNTRY_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MAILING_ADDRESS_COUNTRY_NAME, 
     last_value(MAILING_FORMATTED_ADDRESS_POSTAL_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MAILING_FORMATTED_ADDRESS_POSTAL_CODE, 
     last_value(MAILING_FORMATTED_ADDRESS_ZIP_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MAILING_FORMATTED_ADDRESS_ZIP_CODE, 
     last_value(MAILING_FORMATTED_ADDRESS_ZIP4_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MAILING_FORMATTED_ADDRESS_ZIP4_CODE, 
     last_value(MAILING_ADDRESS_VALIDATED_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MAILING_ADDRESS_VALIDATED_IND, 
     last_value(MAILING_ADDRESS_COMMENT_TEXT) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MAILING_ADDRESS_COMMENT_TEXT, 
 CUSTOMER_NAME, 
 FIRST_NAME, 
 MIDDLE_NAME, 
 LAST_NAME, 
 SUFFIX_NAME, 
 DOCUMENT_BLOCK_IND, 
 PHYSICAL_ADDRESS_LINE_1, 
 PHYSICAL_ADDRESS_LINE_2, 
 PHYSICAL_ADDRESS_CITY_NAME, 
 PHYSICAL_ADDRESS_STATE_CODE, 
 PHYSICAL_ADDRESS_STATE_NAME, 
 PHYSICAL_ADDRESS_POSTAL_CODE, 
 PHYSICAL_ADDRESS_COUNTY_NAME, 
 PHYSICAL_ADDRESS_COUNTRY_NAME, 
 PHYSICAL_ADDRESS_VALIDATED_IND, 
 PHYSICAL_ADDRESS_COMMENT_TEXT, 
 PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE, 
 PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE, 
 PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE, 
 MAILING_STREET_ADDRESS_1, 
 MAILING_STREET_ADDRESS_2, 
 MAILING_ADDRESS_CITY_NAME, 
 MAILING_ADDRESS_STATE_CODE, 
 MAILING_ADDRESS_STATE_NAME, 
 MAILING_ADDRESS_POSTAL_CODE, 
 MAILING_ADDRESS_COUNTY_NAME, 
 MAILING_ADDRESS_COUNTRY_NAME, 
 MAILING_FORMATTED_ADDRESS_POSTAL_CODE, 
 MAILING_FORMATTED_ADDRESS_ZIP_CODE, 
 MAILING_FORMATTED_ADDRESS_ZIP4_CODE, 
 MAILING_ADDRESS_VALIDATED_IND, 
 MAILING_ADDRESS_COMMENT_TEXT
,DBT_VALID_FROM AS EFFECTIVE_TIMESTAMP 
,DBT_VALID_TO   AS END_TIMESTAMP 
,
    CASE WHEN CAST(DBT_VALID_FROM AS DATE) = '1901-01-01' then CAST(DBT_VALID_FROM AS DATE)
      WHEN CAST(DBT_VALID_FROM AS DATE) <> '1901-01-01' THEN dateadd(day,1,CAST(DBT_VALID_FROM AS DATE))
    else CAST(DBT_VALID_FROM AS DATE) end as EFFECTIVE_DATE
,
    CAST(DBT_VALID_TO AS DATE) as END_DATE 
,CASE WHEN DBT_VALID_TO IS NULL THEN 'Y' ELSE 'N' END AS CURRENT_INDICATOR
     
	FROM {{ ref('DIM_CUSTOMER_SCDALL_STEP2') }})

select * from SCD
