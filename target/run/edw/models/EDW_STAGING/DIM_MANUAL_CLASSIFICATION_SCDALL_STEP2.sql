

      create or replace  table DEV_EDW.EDW_STAGING.DIM_MANUAL_CLASSIFICATION_SCDALL_STEP2  as
      (----SRC LAYER----
WITH
SCD1 as ( SELECT RATE_TIER_TYPE_DESC, MANUAL_CLASS_CODE, SUFFIX_DISCOUNT_IND, RATE_TIER_TYPE_CODE, UNIQUE_ID_KEY    
	--, '1901-01-01' as DBT_VALID_FROM, '2099-12-31' as DBT_VALID_TO
	from      STAGING.DSV_MANUAL_CLASSIFICATION ),
SCD2 as ( SELECT *    
	from      EDW_STAGING_SNAPSHOT.DIM_MANUAL_CLASSIFICATION_SNAPSHOT_STEP1 ),
FINAL as ( SELECT * 
            from  SCD2 
                INNER JOIN SCD1 USING( UNIQUE_ID_KEY )  )
select * from FINAL
      );
    