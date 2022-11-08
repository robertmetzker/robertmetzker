

      create or replace  table DEV_EDW.STAGING.DST_CLAIM_STATUS  as
      (----SRC LAYER----
WITH
SRC_CSH as ( SELECT *     from      STAGING.STG_CLAIM_STATUS_HISTORY)
----LOGIC LAYER----
,
LOGIC_CSH as ( SELECT 
		CLM_STT_TYP_CD AS CLM_STT_TYP_CD,
		CLM_STT_TYP_NM AS CLM_STT_TYP_NM,
		CLM_STS_TYP_CD AS CLM_STS_TYP_CD,
		CLM_STS_TYP_NM AS CLM_STS_TYP_NM,
		CLM_TRANS_RSN_TYP_CD AS CLM_TRANS_RSN_TYP_CD,
		CLM_TRANS_RSN_TYP_NM AS CLM_TRANS_RSN_TYP_NM 
				from SRC_CSH
            )
----RENAME LAYER ----
,
RENAME_CSH as ( SELECT CLM_STT_TYP_CD AS CLM_STT_TYP_CD,CLM_STT_TYP_NM AS CLM_STT_TYP_NM,CLM_STS_TYP_CD AS CLM_STS_TYP_CD
,CLM_STS_TYP_NM AS CLM_STS_TYP_NM,CLM_TRANS_RSN_TYP_CD AS CLM_TRANS_RSN_TYP_CD,CLM_TRANS_RSN_TYP_NM AS CLM_TRANS_RSN_TYP_NM 
			from      LOGIC_CSH
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_CSH as ( SELECT  * 
			from     RENAME_CSH 
            
        )
----JOIN LAYER----
,
 JOIN_CSH as ( SELECT * 
			from  FILTER_CSH )
----ETL LAYER----
,
 ETL1 AS (
	 SELECT DISTINCT CLM_STT_TYP_CD,
CLM_STT_TYP_NM,
CLM_STS_TYP_CD,
CLM_STS_TYP_NM,
CLM_TRANS_RSN_TYP_CD,
CLM_TRANS_RSN_TYP_NM FROM JOIN_CSH)
SELECT 
md5(cast(
    
    coalesce(cast(CLM_STT_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_STT_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_STS_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_TRANS_RSN_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_TRANS_RSN_TYP_NM as 
    varchar
), '')

 as 
    varchar
)) AS CLAIM_STATUS_HKEY,
md5(cast(
    
    coalesce(cast(CLM_STT_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_STT_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_STS_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_TRANS_RSN_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_TRANS_RSN_TYP_NM as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY,
CLM_STT_TYP_CD,
CLM_STT_TYP_NM,
CLM_STS_TYP_CD,
CLM_STS_TYP_NM,
CLM_TRANS_RSN_TYP_CD,
CLM_TRANS_RSN_TYP_NM
FROM ETL1
      );
    