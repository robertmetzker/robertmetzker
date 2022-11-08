----SRC LAYER----
WITH
SCD1 as ( SELECT PACKAGE_TYPE_DESC, NDC_4_PRODUCT_DESC, DOSAGE_FORM_CODE
, NDC_5_LABELER_CODE, CURRENT_AWP_DRUG_AMOUNT, DRUG_STRENGTH_CODE, NDC_11_FORMATTED_CODE
, ROUTE_OF_ADMINISTRATION_CODE, PACKAGE_DOSE_DESC, MILLIGRAMS_EQUIVALENCE_PER_UNIT
, PACKAGE_DOSE_CODE, NDC_4_PRODUCT_CODE, PACKAGE_QTY, ROUTE_OF_ADMINISTRATION_DESC, PACKAGE_SIZE_QTY
, DEA_DRUG_SCHEDULE_DESC, PACKAGE_SIZE_UNIT_OF_MEASURE_CODE, NDC_END_DATE
, PACKAGE_SIZE_UNIT_OF_MEASURE_DESC, OVER_THE_COUNTER_CODE, DOSAGE_FORM_DESC
, OVER_THE_COUNTER_DESC, PACKAGE_TYPE_CODE, STRENGTH_MEASURE_CODE, BWC_FORMULARY_COVERAGE_CODE
, GPI_PAYMENT_CATEGORY_DESC, NDC_2_PACKAGE_CODE, BWC_FORMULARY_PRIOR_AUTHORIZATION_IND
, BWC_FORMULARY_COVERAGE_DESC, NDC_VERSION_NUMBER, NDC_11_CODE, NDC_5_LABELER_NAME, MED_CONVERSION_FACTOR
, AED_CONVERSION_FACTOR, NDC_EFFECTIVE_DATE, DEA_DRUG_SCHEDULE_CODE, BWC_FORMULARY_COVERAGE_LIMITATION_DESC
, ACTIVE_DRUG_STRENGTH_QUANTITY , UNIQUE_ID_KEY    
	from      STAGING.DSV_NDC_GPI_REFERENCE ),
SCD2 as ( SELECT *    
	from      EDW_STAGING_SNAPSHOT.DIM_NDC_SNAPSHOT_STEP1 ),
FINAL as ( SELECT * 
			from  SCD2 
				INNER JOIN SCD1 USING( UNIQUE_ID_KEY )  )
select * from FINAL