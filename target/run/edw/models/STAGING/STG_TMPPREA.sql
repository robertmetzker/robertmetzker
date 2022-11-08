

      create or replace  table DEV_EDW.STAGING.STG_TMPPREA  as
      (----SRC LAYER----
WITH
SRC_PREA as ( SELECT *     from      DEV_VIEWS.DBMOBP00.TMPPREA )
----LOGIC LAYER----
,
LOGIC_PREA as ( SELECT 
		PRVDR_ID::timestamp AS PRVDR_ID,
		cast(PRVDR_BASE_NMBR as TEXT) AS PRVDR_BASE_NMBR,
		cast(LPAD(PRVDR_SFX_NMBR,4,'0') as TEXT) AS PRVDR_SFX_NMBR,
		PREA_CRT_DTTM::timestamp AS PREA_CRT_DTTM,
		UPPER(TRIM(CRT_USER_CODE)) AS CRT_USER_CODE,
		case when DCTVT_DTTM = '10000-01-01 00:00:00' THEN '2999-12-31 00:00:00' ELSE DCTVT_DTTM END::TIMESTAMP AS DCTVT_DTTM,
		UPPER(TRIM(DCTVT_USER_CODE)) AS DCTVT_USER_CODE,
		TRIM(UPDT_PRGRM_NAME) AS UPDT_PRGRM_NAME 
				from SRC_PREA
            )
----RENAME LAYER ----
,
RENAME_PREA as ( SELECT PRVDR_ID AS PRVDR_ID,PRVDR_BASE_NMBR AS PRVDR_BASE_NMBR,PRVDR_SFX_NMBR AS PRVDR_SFX_NMBR,PREA_CRT_DTTM AS PREA_CRT_DTTM,CRT_USER_CODE AS CRT_USER_CODE,DCTVT_DTTM AS DCTVT_DTTM,DCTVT_USER_CODE AS DCTVT_USER_CODE,UPDT_PRGRM_NAME AS UPDT_PRGRM_NAME 
			from      LOGIC_PREA
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_PREA as ( SELECT  * 
			from     RENAME_PREA 
            
        )
----JOIN LAYER----
,
 JOIN_PREA as ( SELECT * 
			from  FILTER_PREA )
 SELECT * FROM JOIN_PREA
      );
    