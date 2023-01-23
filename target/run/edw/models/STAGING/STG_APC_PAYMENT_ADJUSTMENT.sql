

      create or replace  table DEV_EDW.STAGING.STG_APC_PAYMENT_ADJUSTMENT  as
      (----SRC LAYER----
WITH
SRC_APC as ( SELECT *     from      DEV_VIEWS.BWC_FILES.APC_PAYMENT_ADJUSTMENT )
//SRC_APC as ( SELECT *     from      APC_PAYMENT_ADJUSTMENT)
----LOGIC LAYER----
,
LOGIC_APC as ( SELECT 
		UPPER(TRIM(APC_PAYMENT_ADJUSTMENT_CODE)) AS APC_PAYMENT_ADJUSTMENT_CODE,
		UPPER(TRIM(Description)) AS Description 
				from SRC_APC
            )
----RENAME LAYER ----
,
RENAME_APC as ( SELECT APC_PAYMENT_ADJUSTMENT_CODE AS APC_PAYMENT_ADJUSTMENT_CODE,
			
			Description AS DESCRIPTION 
			from      LOGIC_APC
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_APC as ( SELECT  * 
			from     RENAME_APC 
            
        )
----JOIN LAYER----
,
 JOIN_APC as ( SELECT * 
			from  FILTER_APC )
 SELECT * FROM JOIN_APC
      );
    