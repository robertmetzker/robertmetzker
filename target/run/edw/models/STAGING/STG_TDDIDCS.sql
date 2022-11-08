

      create or replace  table DEV_EDW.STAGING.STG_TDDIDCS  as
      (----SRC LAYER----
WITH
SRC_IDCS as ( SELECT *     from      DEV_VIEWS.DBDWQP00.TDDIDCS )
//SRC_IDCS as ( SELECT *     from      TDDIDCS)
----LOGIC LAYER----
,
LOGIC_IDCS as ( SELECT 
		NULLIF(TRIM(ICDC_CODE), '') AS ICDC_CODE,
		TRIM(ICDV_CODE) AS ICDV_CODE,
		TRIM(REGEXP_REPLACE(ICDV_CODE, '[^[:digit:]]', ' '))::INTEGER AS ICD_CODE_VERSION_NUMBER,
		cast(IDCS_VRSN_BGN_DATE as DATE) AS IDCS_VRSN_BGN_DATE,
		IDCS_VRSN_BGN_TMS AS IDCS_VRSN_BGN_TMS,
		cast(IDCS_VRSN_END_DATE as DATE) AS IDCS_VRSN_END_DATE,
		IDCS_VRSN_END_TMS AS IDCS_VRSN_END_TMS,
		UPPER(TRIM(CMS_ICD_STS_CODE)) AS CMS_ICD_STS_CODE,
		UPPER(TRIM(CMS_ICD_STS)) AS CMS_ICD_STS,
		cast(IDCS_EFCTV_DATE as DATE) AS IDCS_EFCTV_DATE,
		cast(IDCS_ENDNG_DATE as DATE) AS IDCS_ENDNG_DATE,
		UPPER(TRIM(IDCS_CRNT_FLAG)) AS IDCS_CRNT_FLAG,
		UPPER(TRIM(IDCS_CRNT_MAX_FLAG)) AS IDCS_CRNT_MAX_FLAG,
		UPPER(TRIM(IDCS_FTR_FLAG)) AS IDCS_FTR_FLAG,
		TRIM(CRT_USER_ID) AS CRT_USER_ID,
		NULLIF(TRIM(DCTVT_USER_ID), '') AS DCTVT_USER_ID 
				from SRC_IDCS
            )
----RENAME LAYER ----
,
RENAME_IDCS as ( SELECT 
			
			ICDC_CODE AS ICD_CODE,ICDV_CODE AS ICDV_CODE,
			
			ICD_CODE_VERSION_NUMBER AS ICD_CODE_VERSION_NUMBER,IDCS_VRSN_BGN_DATE AS IDCS_VRSN_BGN_DATE,IDCS_VRSN_BGN_TMS AS IDCS_VRSN_BGN_TMS,IDCS_VRSN_END_DATE AS IDCS_VRSN_END_DATE,IDCS_VRSN_END_TMS AS IDCS_VRSN_END_TMS,
			
			CMS_ICD_STS_CODE AS ICD_CODE_STATUS_CODE,
			
			CMS_ICD_STS AS ICD_CODE_STATUS_DESC,
			
			IDCS_EFCTV_DATE AS ICD_CODE_STATUS_EFFECTIVE_DATE,
			
			IDCS_ENDNG_DATE AS ICD_CODE_STATUS_END_DATE,IDCS_CRNT_FLAG AS IDCS_CRNT_FLAG,IDCS_CRNT_MAX_FLAG AS IDCS_CRNT_MAX_FLAG,IDCS_FTR_FLAG AS IDCS_FTR_FLAG,CRT_USER_ID AS CRT_USER_ID,DCTVT_USER_ID AS DCTVT_USER_ID 
			from      LOGIC_IDCS
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_IDCS as ( SELECT  * 
			from     RENAME_IDCS 
            
        )
----JOIN LAYER----
,
 JOIN_IDCS as ( SELECT * 
			from  FILTER_IDCS )
 SELECT * FROM JOIN_IDCS
      );
    