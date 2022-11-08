----SRC LAYER----
WITH
SRC_P as ( SELECT *     from      DEV_VIEWS.DBDWQP00.TCDPBMP ),
//SRC_P as ( SELECT *     from      TCDPBMP)
----LOGIC LAYER----

LOGIC_P as ( SELECT 
		UPPER(TRIM(PBM_PRCNG_SRC_CODE)) AS PBM_PRCNG_SRC_CODE,
		UPPER(TRIM(PBM_PRCNG_SRC_TYPE)) AS PBM_PRCNG_SRC_TYPE,
		UPPER(TRIM(PBM_PRCNG_SRC_DESC)) AS PBM_PRCNG_SRC_DESC,
		DW_CNTRL_DATE AS DW_CNTRL_DATE 
				from SRC_P
            )
----RENAME LAYER ----
,
RENAME_P as ( SELECT PBM_PRCNG_SRC_CODE AS PBM_PRCNG_SRC_CODE,PBM_PRCNG_SRC_TYPE AS PBM_PRCNG_SRC_TYPE,PBM_PRCNG_SRC_DESC AS PBM_PRCNG_SRC_DESC,DW_CNTRL_DATE AS DW_CNTRL_DATE 
			from      LOGIC_P
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_P as ( SELECT  * 
			from     RENAME_P 
            
        )
----JOIN LAYER----
,
 JOIN_P as ( SELECT * 
			from  FILTER_P )
 SELECT * FROM JOIN_P