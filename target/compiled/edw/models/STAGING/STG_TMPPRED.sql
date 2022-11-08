----SRC LAYER----
WITH
SRC_PRED as ( SELECT *     from      DEV_VIEWS.DBMOBP00.TMPPRED )
----LOGIC LAYER----
,
LOGIC_PRED as ( SELECT 
		cast(PRVDR_BASE_NMBR as TEXT) AS PRVDR_BASE_NMBR,
		cast(LPAD(PRVDR_SFX_NMBR,4,'0') as TEXT) AS PRVDR_SFX_NMBR,
		PRED_CRT_DTTM::timestamp AS PRED_CRT_DTTM,
		UPPER(TRIM(DBA_NAME)) AS DBA_NAME,
		UPPER(TRIM(NEW_PTNT_ACPT_IND)) AS NEW_PTNT_ACPT_IND,
		CASE UPPER(TRIM(NEW_PTNT_ACPT_IND)) WHEN 'Y' THEN 'YES' WHEN 'N' THEN 'NO'
			WHEN 'C' THEN 'CONTACT PROVIDER' ELSE NULL END AS NEW_PATIENT_DESC,
		UPPER(CRDNT_RVW_IND) AS CRDNT_RVW_IND,
		UPPER(TRIM(CRT_USER_CODE)) AS CRT_USER_CODE,
		case when DCTVT_DTTM = '10000-01-01 00:00:00' THEN '2999-12-31 00:00:00' ELSE DCTVT_DTTM END::TIMESTAMP AS DCTVT_DTTM,
		UPPER(TRIM(DCTVT_USER_CODE)) AS DCTVT_USER_CODE,
		TRIM(UPDT_PRGRM_NAME) AS UPDT_PRGRM_NAME 
				from SRC_PRED
            )
----RENAME LAYER ----
,
RENAME_PRED as ( SELECT PRVDR_BASE_NMBR AS PRVDR_BASE_NMBR,PRVDR_SFX_NMBR AS PRVDR_SFX_NMBR,PRED_CRT_DTTM AS PRED_CRT_DTTM
,DBA_NAME AS DBA_NAME,NEW_PTNT_ACPT_IND AS NEW_PATIENT_CODE, NEW_PATIENT_DESC AS NEW_PATIENT_DESC,CRDNT_RVW_IND AS CRDNT_RVW_IND,CRT_USER_CODE AS CRT_USER_CODE
,DCTVT_DTTM AS DCTVT_DTTM
,DCTVT_USER_CODE AS DCTVT_USER_CODE
,UPDT_PRGRM_NAME AS UPDT_PRGRM_NAME 
			from      LOGIC_PRED
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_PRED as ( SELECT  * 
			from     RENAME_PRED 
            
        )
----JOIN LAYER----
,
 JOIN_PRED as ( SELECT * 
			from  FILTER_PRED )
 SELECT * FROM JOIN_PRED