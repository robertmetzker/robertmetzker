

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(NDC_11_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NDC_11_CODE, 
     last_value(NDC_11_FORMATTED_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NDC_11_FORMATTED_CODE, 
     last_value(NDC_VERSION_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NDC_VERSION_NUMBER, 
     last_value(NDC_5_LABELER_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NDC_5_LABELER_CODE, 
     last_value(NDC_5_LABELER_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NDC_5_LABELER_NAME, 
     last_value(NDC_4_PRODUCT_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NDC_4_PRODUCT_CODE, 
     last_value(NDC_4_PRODUCT_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NDC_4_PRODUCT_DESC, 
     last_value(NDC_2_PACKAGE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NDC_2_PACKAGE_CODE, 
     last_value(NDC_EFFECTIVE_DATE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NDC_EFFECTIVE_DATE, 
     last_value(NDC_END_DATE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NDC_END_DATE, 
     last_value(BWC_FORMULARY_PRIOR_AUTHORIZATION_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BWC_FORMULARY_PRIOR_AUTHORIZATION_IND, 
     last_value(BWC_FORMULARY_COVERAGE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BWC_FORMULARY_COVERAGE_CODE, 
     last_value(BWC_FORMULARY_COVERAGE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BWC_FORMULARY_COVERAGE_DESC, 
     last_value(BWC_FORMULARY_COVERAGE_LIMITATION_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BWC_FORMULARY_COVERAGE_LIMITATION_DESC, 
     last_value(OVER_THE_COUNTER_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OVER_THE_COUNTER_CODE, 
     last_value(OVER_THE_COUNTER_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OVER_THE_COUNTER_DESC, 
     last_value(DEA_DRUG_SCHEDULE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEA_DRUG_SCHEDULE_CODE, 
     last_value(DEA_DRUG_SCHEDULE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEA_DRUG_SCHEDULE_DESC, 
     last_value(DRUG_STRENGTH_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DRUG_STRENGTH_CODE, 
     last_value(STRENGTH_MEASURE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as STRENGTH_MEASURE_CODE, 
     last_value(ROUTE_OF_ADMINISTRATION_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ROUTE_OF_ADMINISTRATION_CODE, 
     last_value(ROUTE_OF_ADMINISTRATION_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ROUTE_OF_ADMINISTRATION_DESC, 
     last_value(DOSAGE_FORM_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DOSAGE_FORM_CODE, 
     last_value(DOSAGE_FORM_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DOSAGE_FORM_DESC, 
     last_value(PACKAGE_SIZE_UNIT_OF_MEASURE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PACKAGE_SIZE_UNIT_OF_MEASURE_CODE, 
     last_value(PACKAGE_SIZE_UNIT_OF_MEASURE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PACKAGE_SIZE_UNIT_OF_MEASURE_DESC, 
     last_value(PACKAGE_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PACKAGE_TYPE_CODE, 
     last_value(PACKAGE_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PACKAGE_TYPE_DESC, 
     last_value(PACKAGE_DOSE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PACKAGE_DOSE_CODE, 
     last_value(PACKAGE_DOSE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PACKAGE_DOSE_DESC, 
     last_value(PACKAGE_SIZE_QTY) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PACKAGE_SIZE_QTY, 
     last_value(PACKAGE_QTY) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PACKAGE_QTY, 
     last_value(MILLIGRAMS_EQUIVALENCE_PER_UNIT) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MILLIGRAMS_EQUIVALENCE_PER_UNIT, 
     last_value(ACTIVE_DRUG_STRENGTH_QUANTITY) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ACTIVE_DRUG_STRENGTH_QUANTITY, 
     last_value(CURRENT_AWP_DRUG_AMOUNT) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CURRENT_AWP_DRUG_AMOUNT, 
     last_value(MED_CONVERSION_FACTOR) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MED_CONVERSION_FACTOR, 
     last_value(AED_CONVERSION_FACTOR) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as AED_CONVERSION_FACTOR, 
     last_value(GPI_PAYMENT_CATEGORY_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as GPI_PAYMENT_CATEGORY_DESC,
 GPI_14_CODE, 
 GPI_14_DESC, 
 GPI_2_GROUP_CODE, 
 GPI_2_GROUP_DESC, 
 GPI_4_CLASS_CODE, 
 GPI_4_CLASS_DESC, 
 GPI_6_SUBCLASS_CODE, 
 GPI_6_SUBCLASS_DESC, 
 GPI_8_DRUG_NAME_CODE, 
 GPI_8_DRUG_NAME_DESC, 
 GPI_10_DRUG_NAME_EXT_CODE, 
 GPI_10_DRUG_NAME_EXT_DESC, 
 GPI_12_DRUG_DOSAGE_FORM_CODE, 
 GPI_12_DRUG_DOSAGE_FORM_DESC, 
 GPI_EFFECTIVE_DATE, 
 GPI_END_DATE
,DBT_VALID_FROM AS EFFECTIVE_TIMESTAMP 
,DBT_VALID_TO   AS END_TIMESTAMP 
,
    CASE WHEN (ROW_NUMBER() OVER (PARTITION BY UNIQUE_ID_KEY ORDER BY DBT_VALID_FROM )) = 1 
            THEN  
            (CASE WHEN GPI_EFFECTIVE_DATE IS NOT NULL 
                    AND NDC_EFFECTIVE_DATE >= GPI_EFFECTIVE_DATE THEN GPI_EFFECTIVE_DATE ELSE NDC_EFFECTIVE_DATE END)
        WHEN CAST(DBT_VALID_FROM AS DATE) = '1901-01-01' then CAST(DBT_VALID_FROM AS DATE)
      WHEN CAST(DBT_VALID_FROM AS DATE) <> '1901-01-01' THEN dateadd(day,1,CAST(DBT_VALID_FROM AS DATE))
    else CAST(DBT_VALID_FROM AS DATE) end as EFFECTIVE_DATE
,
    CAST(DBT_VALID_TO AS DATE) as END_DATE      
	FROM EDW_STAGING.DIM_NDC_SCDALL_STEP2)

-----ETL LAYER----

SELECT 
  md5(cast(
    
    coalesce(cast(NDC_11_CODE as 
    varchar
), '') || '-' || coalesce(cast(EFFECTIVE_DATE as 
    varchar
), '')

 as 
    varchar
)) AS NDC_GPI_HKEY
, UNIQUE_ID_KEY
, NDC_11_CODE
, NDC_11_FORMATTED_CODE
, NDC_VERSION_NUMBER
, NDC_5_LABELER_CODE
, NDC_5_LABELER_NAME
, NDC_4_PRODUCT_CODE
, NDC_4_PRODUCT_DESC
, NDC_2_PACKAGE_CODE
, GPI_14_CODE
, GPI_14_DESC
, GPI_2_GROUP_CODE
, GPI_2_GROUP_DESC
, GPI_4_CLASS_CODE
, GPI_4_CLASS_DESC
, GPI_6_SUBCLASS_CODE
, GPI_6_SUBCLASS_DESC
, GPI_8_DRUG_NAME_CODE
, GPI_8_DRUG_NAME_DESC
, GPI_10_DRUG_NAME_EXT_CODE
, GPI_10_DRUG_NAME_EXT_DESC
, GPI_12_DRUG_DOSAGE_FORM_CODE
, GPI_12_DRUG_DOSAGE_FORM_DESC
, NDC_EFFECTIVE_DATE
, NDC_END_DATE
, GPI_EFFECTIVE_DATE
, GPI_END_DATE
, BWC_FORMULARY_PRIOR_AUTHORIZATION_IND
, BWC_FORMULARY_COVERAGE_CODE
, BWC_FORMULARY_COVERAGE_DESC
, BWC_FORMULARY_COVERAGE_LIMITATION_DESC
, OVER_THE_COUNTER_CODE
, OVER_THE_COUNTER_DESC
, DEA_DRUG_SCHEDULE_CODE
, DEA_DRUG_SCHEDULE_DESC
, DRUG_STRENGTH_CODE
, STRENGTH_MEASURE_CODE
, ROUTE_OF_ADMINISTRATION_CODE
, ROUTE_OF_ADMINISTRATION_DESC
, DOSAGE_FORM_CODE
, DOSAGE_FORM_DESC
, PACKAGE_SIZE_UNIT_OF_MEASURE_CODE
, PACKAGE_SIZE_UNIT_OF_MEASURE_DESC
, PACKAGE_TYPE_CODE
, PACKAGE_TYPE_DESC
, PACKAGE_DOSE_CODE
, PACKAGE_DOSE_DESC
, PACKAGE_SIZE_QTY
, PACKAGE_QTY
, MILLIGRAMS_EQUIVALENCE_PER_UNIT::numeric(32,4) AS MILLIGRAMS_EQUIVALENCE_PER_UNIT
, ACTIVE_DRUG_STRENGTH_QUANTITY
, CURRENT_AWP_DRUG_AMOUNT
, MED_CONVERSION_FACTOR
, AED_CONVERSION_FACTOR
, GPI_PAYMENT_CATEGORY_DESC
, case when END_DATE is null then 'Y' else 'N' end as CURRENT_RECORD_IND
, EFFECTIVE_DATE AS RECORD_EFFECTIVE_DATE
, END_DATE AS RECORD_END_DATE
, EFFECTIVE_TIMESTAMP AS LOAD_DATETIME
, END_TIMESTAMP AS UPDATE_DATETIME
, 'BWC'::VARCHAR(50) AS PRIMARY_SOURCE_SYSTEM
    FROM SCD