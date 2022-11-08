

      create or replace  table DEV_EDW.STAGING.STG_CPT_DEP_FEE_SCHEDULE  as
      (----SRC LAYER----
WITH
SRC_DEP as ( SELECT *     from      DEV_VIEWS.BWC_FILES.PROCEDURE_CODE_DEP_FEE_SCHEDULE )
----LOGIC LAYER----
,
LOGIC_DEP as ( SELECT 
		UPPER(PROCEDURE_CODE) AS PROCEDURE_CODE,
		UPPER(FEE_SCHEDULE) AS FEE_SCHEDULE 
				from SRC_DEP
            )
----RENAME LAYER ----
,
RENAME_DEP as ( SELECT PROCEDURE_CODE AS PROCEDURE_CODE,FEE_SCHEDULE AS FEE_SCHEDULE 
			from      LOGIC_DEP
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_DEP as ( SELECT  * 
			from     RENAME_DEP 
            
        )
----JOIN LAYER----
,
 JOIN_DEP as ( SELECT * 
			from  FILTER_DEP )
 SELECT * FROM JOIN_DEP
      );
    