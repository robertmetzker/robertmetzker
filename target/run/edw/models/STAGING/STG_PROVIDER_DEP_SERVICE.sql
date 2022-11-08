

      create or replace  table DEV_EDW.STAGING.STG_PROVIDER_DEP_SERVICE  as
      (----SRC LAYER----
WITH
SRC_DEPP as ( SELECT *     from      DEV_VIEWS.DBMOBP00.TMPDEPP ),
SRC_DEPC as ( SELECT *     from      DEV_VIEWS.BWC_PEACH.TMPDEPC )
//SRC_DEPP as ( SELECT *     from      TMPDEPP),
//SRC_DEPC as ( SELECT *     from      TMPDEPC)
----LOGIC LAYER----
,
LOGIC_DEPP as ( SELECT 
		cast(PRVDR_BASE_NMBR as TEXT) AS PRVDR_BASE_NMBR,
		cast(lpad(PRVDR_SFX_NMBR,4,0) as TEXT) AS PRVDR_SFX_NMBR,
		UPPER(DEP_SRVC_CODE) AS DEP_SRVC_CODE,
		EFCTV_DATE AS EFCTV_DATE,
		ENDNG_DATE AS ENDNG_DATE,
		DEPP_CRT_DTTM AS DEPP_CRT_DTTM,
		UPPER(TRIM(CRT_USER_CODE)) AS CRT_USER_CODE,
		REPLACE(DCTVT_DTTM, '10000-01-01 00:00:00', '2999-12-31 00:00:00')::TIMESTAMP   AS DCTVT_DTTM ,
		UPPER(TRIM(DCTVT_USER_CODE)) AS DCTVT_USER_CODE,
		UPPER(TRIM(UPDT_PRGRM_NAME)) AS UPDT_PRGRM_NAME 
				from SRC_DEPP
            ),
LOGIC_DEPC as ( SELECT 
		UPPER(TRIM(DEP_SRVC_DESCR)) AS DEP_SRVC_NAME,
		UPPER(DEP_SRVC_CODE) AS DEP_SRVC_CODE,
		REPLACE(DCTVT_DTTM, '10000-01-01 00:00:00', '2999-12-31 00:00:00')::TIMESTAMP   AS DCTVT_DTTM 
				from SRC_DEPC
            )
----RENAME LAYER ----
,
RENAME_DEPP as ( SELECT PRVDR_BASE_NMBR AS PRVDR_BASE_NMBR,PRVDR_SFX_NMBR AS PRVDR_SFX_NMBR,DEP_SRVC_CODE AS DEP_SRVC_CODE,EFCTV_DATE AS EFCTV_DATE,ENDNG_DATE AS ENDNG_DATE,DEPP_CRT_DTTM AS DEPP_CRT_DTTM,CRT_USER_CODE AS CRT_USER_CODE,DCTVT_DTTM AS DCTVT_DTTM,DCTVT_USER_CODE AS DCTVT_USER_CODE,UPDT_PRGRM_NAME AS UPDT_PRGRM_NAME 
			from      LOGIC_DEPP
        ),
RENAME_DEPC as ( SELECT DEP_SRVC_NAME AS DEP_SRVC_NAME,
			
			DEP_SRVC_CODE AS DEPC_DEP_SRVC_CODE,
			
			DCTVT_DTTM AS DEPC_DCTVT_DTTM 
			from      LOGIC_DEPC
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_DEPP as ( SELECT  * 
			from     RENAME_DEPP 
            
        ),

        FILTER_DEPC as ( SELECT  * 
			from     RENAME_DEPC 
            WHERE DEPC_DCTVT_DTTM > current_date
        )
----JOIN LAYER----
,
DEPP as ( SELECT * 
			from  FILTER_DEPP
				LEFT JOIN FILTER_DEPC ON FILTER_DEPP.DEP_SRVC_CODE = FILTER_DEPC.DEPC_DEP_SRVC_CODE  )
select * from DEPP
      );
    