

      create or replace  table DEV_EDW.STAGING.STG_CPT_VR_FEE_SCHEDULE  as
      (----SRC LAYER----
WITH
SRC_VR as ( SELECT *     from      DEV_VIEWS.BWC_FILES.PROCEDURE_CODE_VR_FEE_SCHEDULE )
----LOGIC LAYER----
,
LOGIC_VR as ( SELECT 
		UPPER(PROCEDURE_CODE) AS PROCEDURE_CODE,
		UPPER(FEE_SCHEDULE) AS FEE_SCHEDULE 
				from SRC_VR
            )
----RENAME LAYER ----
,
RENAME_VR as ( SELECT PROCEDURE_CODE AS PROCEDURE_CODE,FEE_SCHEDULE AS FEE_SCHEDULE 
			from      LOGIC_VR
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_VR as ( SELECT  * 
			from     RENAME_VR 
            
        )
----JOIN LAYER----
,
 JOIN_VR as ( SELECT * 
			from  FILTER_VR )
 SELECT * FROM JOIN_VR
      );
    