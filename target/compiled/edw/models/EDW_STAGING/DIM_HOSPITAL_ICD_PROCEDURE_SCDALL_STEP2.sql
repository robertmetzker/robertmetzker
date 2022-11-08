
 ----SRC LAYER----
WITH
SCD1 as ( SELECT 
   ICD_CODE_VERSION_NUMBER, 
    ICD_PROCEDURE_CODE , 
	GENDER_SPECIFIC_DESC,
	UNIQUE_ID_KEY    
	--, '1901-01-01' as DBT_VALID_FROM, '2099-12-31' as DBT_VALID_TO
	from      STAGING.DSV_HOSPITAL_ICD_PROCEDURE ),
SCD2 as ( SELECT *    
	from      EDW_STAGING_SNAPSHOT.DIM_HOSPITAL_ICD_PROCEDURE_SNAPSHOT_STEP1 ),
FINAL as ( SELECT * 
			from  SCD2 
				INNER JOIN SCD1 USING( UNIQUE_ID_KEY )  )
select * from FINAL