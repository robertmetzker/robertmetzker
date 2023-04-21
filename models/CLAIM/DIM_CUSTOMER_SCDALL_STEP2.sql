
 ----SRC LAYER----
WITH
SCD1 as ( SELECT PHYSICAL_ADDRESS_COMMENT_TEXT, MAILING_ADDRESS_COUNTRY_NAME, MAILING_ADDRESS_STATE_NAME, MAILING_ADDRESS_STATE_CODE, DOCUMENT_BLOCK_IND, ROLE_ID_NUMBER, MAILING_FORMATTED_ADDRESS_ZIP4_CODE, TAX_ID_TYPE_CODE, COVERED_INDIVIDUAL_IND, MAILING_FORMATTED_ADDRESS_POSTAL_CODE, PHYSICAL_ADDRESS_LINE_2, PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE, LAST_NAME, LEGAL_REPRESENTATIVE_IND, CUSTOMER_NUMBER, NETWORK_IND, MAILING_FORMATTED_ADDRESS_ZIP_CODE, MAILING_ADDRESS_COUNTY_NAME, MIDDLE_NAME, PHYSICAL_ADDRESS_VALIDATED_IND, PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE, MAILING_ADDRESS_POSTAL_CODE, TAX_ID_NUMBER, PHYSICAL_ADDRESS_POSTAL_CODE, INJURED_WORKER_IND, ROLE_ID_NUMBER_TYPE, PHYSICAL_ADDRESS_CITY_NAME, CUSTOMER_HKEY, SUFFIX_NAME, PHYSICAL_ADDRESS_LINE_1, PHYSICAL_ADDRESS_COUNTY_NAME, PROVIDER_IND, PHONE_NUMBER, MAILING_STREET_ADDRESS_1, MISCELLANEOUS_IND, EMPLOYER_IND, RECORD_EFFECTIVE_DATE, CUSTOMER_NAME, PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE, MAILING_ADDRESS_CITY_NAME, MAILING_STREET_ADDRESS_2, MAILING_ADDRESS_VALIDATED_IND, PHYSICAL_ADDRESS_STATE_NAME, PHYSICAL_ADDRESS_STATE_CODE, PHYSICAL_ADDRESS_COUNTRY_NAME, MAILING_ADDRESS_COMMENT_TEXT, CURRENT_RECORD_IND, FIRST_NAME, RECORD_END_DATE , UNIQUE_ID_KEY    
	--, '1901-01-01' as DBT_VALID_FROM, '2099-12-31' as DBT_VALID_TO
	from      {{ ref( 'DSV_CUSTOMER') }} ),
SCD2 as ( SELECT *    
	FROM      {{ ref('DIM_CUSTOMER_SNAPSHOT_STEP1') }} ),
FINAL as ( SELECT * 
            FROM  SCD2 
                INNER JOIN SCD1 USING( UNIQUE_ID_KEY )  )
select * from FINAL