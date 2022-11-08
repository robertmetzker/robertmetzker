----SRC LAYER----
WITH
SRC_APC as ( SELECT *     from      DEV_VIEWS.BWC_FILES.OPPS_RETURN )
//SRC_APC as ( SELECT *     from      OPPS_RETURN)
----LOGIC LAYER----
,
LOGIC_APC as ( SELECT 
		UPPER(TRIM(OPPS_RETURN_CODE)) AS OPPS_RETURN_CODE,
		UPPER(TRIM(Description)) AS Description 
				from SRC_APC
            )
----RENAME LAYER ----
,
RENAME_APC as ( SELECT OPPS_RETURN_CODE AS OPPS_RETURN_CODE,
			
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