

      create or replace  table DEV_EDW.EDW_STAGING.DIM_EOB_SCDALL_STEP2  as
      (
 ----SRC LAYER----
WITH
SCD1 as ( SELECT EOB_CODE, APPLIED_DESC , UNIQUE_ID_KEY    
	from      STAGING.DSV_EOB ),
SCD2 as ( SELECT *    
	from      EDW_STAGING_SNAPSHOT.DIM_EOB_SNAPSHOT_STEP1 ),
FINAL as ( SELECT * 
			from  SCD2 
				INNER JOIN SCD1 USING( UNIQUE_ID_KEY )  )
select * from FINAL
      );
    