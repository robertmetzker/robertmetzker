
  create or replace  view DEV_EDW.STAGING.DSV_CLAIM_STATUS  as (
    
----SRC LAYER----
WITH
SRC_CS as ( SELECT *     from      STAGING.DST_CLAIM_STATUS)
----LOGIC LAYER----
,
LOGIC_CS as ( SELECT 
		CLAIM_STATUS_HKEY AS CLAIM_STATUS_HKEY,
		UNIQUE_ID_KEY AS UNIQUE_ID_KEY,
		CLM_STT_TYP_CD AS CLM_STT_TYP_CD,
		CLM_STS_TYP_CD AS CLM_STS_TYP_CD,
		CLM_TRANS_RSN_TYP_CD AS CLM_TRANS_RSN_TYP_CD,
		CLM_STT_TYP_NM AS CLM_STT_TYP_NM,
		CLM_STS_TYP_NM AS CLM_STS_TYP_NM,
		CLM_TRANS_RSN_TYP_NM AS CLM_TRANS_RSN_TYP_NM 
				from SRC_CS
            )
----RENAME LAYER ----
,
RENAME_CS as ( SELECT CLAIM_STATUS_HKEY AS CLAIM_STATUS_HKEY,UNIQUE_ID_KEY AS UNIQUE_ID_KEY,
			
			CLM_STT_TYP_CD AS CLAIM_STATE_CODE,
			
			CLM_STS_TYP_CD AS CLAIM_STATUS_CODE,
			
			CLM_TRANS_RSN_TYP_CD AS CLAIM_STATUS_REASON_CODE,
			
			CLM_STT_TYP_NM AS CLAIM_STATE_DESC,
			
			CLM_STS_TYP_NM AS CLAIM_STATUS_DESC,
			
			CLM_TRANS_RSN_TYP_NM AS CLAIM_STATUS_REASON_DESC 
			from      LOGIC_CS
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_CS as ( SELECT  * 
			from     RENAME_CS 
            
        )
----JOIN LAYER----
,
 JOIN_CS as ( SELECT * 
			from  FILTER_CS )
 SELECT * FROM JOIN_CS
  );
