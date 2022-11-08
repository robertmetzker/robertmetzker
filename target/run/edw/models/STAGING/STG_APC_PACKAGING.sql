

      create or replace  table DEV_EDW.STAGING.STG_APC_PACKAGING  as
      (----SRC LAYER----
WITH
SRC_APC as ( SELECT *     from      DEV_VIEWS.BWC_FILES.APC_PACKAGING )
//SRC_APC as ( SELECT *     from      APC_PACKAGING)
----LOGIC LAYER----
,
LOGIC_APC as ( SELECT 
		UPPER(TRIM(APC_PACKAGING_CODE)) AS APC_PACKAGING_CODE,
		UPPER(TRIM(Description)) AS Description 
				from SRC_APC
            )
----RENAME LAYER ----
,
RENAME_APC as ( SELECT APC_PACKAGING_CODE AS APC_PACKAGING_CODE,
			
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
    