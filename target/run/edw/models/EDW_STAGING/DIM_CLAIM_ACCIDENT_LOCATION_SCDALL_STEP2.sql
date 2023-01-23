

      create or replace  table DEV_EDW.EDW_STAGING.DIM_CLAIM_ACCIDENT_LOCATION_SCDALL_STEP2  as
      (----SRC LAYER----
WITH
SCD1 as ( SELECT ACCIDENT_LOCATION_STATE, ACCIDENT_LOCATION_STATE_CODE, ACCIDENT_ADDRESS_LINE_2, ACCIDENT_LOCATION_CITY, ACCIDENT_ADDRESS_LINE_1, ACCIDENT_LOCATION_COUNTRY, ACCIDENT_LOCATION_COMMENT, ACCIDENT_LOCATION_COUNTY, ACCIDENT_LOCATION_ZIP_CODE, ACCIDENT_LOCATION_NAME, ACCIDENT_PREMISE_TYPE_DESC, UNIQUE_ID_KEY    
	--, '1901-01-01' as DBT_VALID_FROM, '2099-12-31' as DBT_VALID_TO
	from      STAGING.DSV_CLAIM_ACCIDENT_LOCATION )

select * from SCD1
      );
    