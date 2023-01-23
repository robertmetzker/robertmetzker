----SRC LAYER----
WITH
SRC_PNPI as ( SELECT *     from      DEV_VIEWS.DBMOBP00.TMPPNPI )
----LOGIC LAYER----
,
LOGIC_PNPI as ( SELECT 
		cast(PRVDR_BASE_NMBR as TEXT) AS PRVDR_BASE_NMBR,
		LPAD(cast(PRVDR_SFX_NMBR as TEXT), 4, '0') AS PRVDR_SFX_NMBR,
		PNPI_CRT_DTTM AS PNPI_CRT_DTTM,
		cast(NPI_NMBR as TEXT) AS NPI_NMBR,
		UPPER(NPI_CNFRM_IND) AS NPI_CNFRM_IND,
		cast(EFCTV_DATE as DATE) AS EFCTV_DATE,
		cast(ENDNG_DATE as DATE) AS ENDNG_DATE,
		UPPER(DCTVT_MTHD_CODE) AS DCTVT_MTHD_CODE,
		UPPER(TRIM(CRT_USER_CODE)) AS CRT_USER_CODE,
		DCTVT_DTTM AS DCTVT_DTTM,
		UPPER(TRIM(DCTVT_USER_CODE)) AS DCTVT_USER_CODE,
		UPPER(TRIM(UPDT_PRGRM_NAME)) AS UPDT_PRGRM_NAME 
				from SRC_PNPI
            )
----RENAME LAYER ----
,
RENAME_PNPI as ( SELECT PRVDR_BASE_NMBR AS PRVDR_BASE_NMBR,PRVDR_SFX_NMBR AS PRVDR_SFX_NMBR,PNPI_CRT_DTTM AS PNPI_CRT_DTTM,NPI_NMBR AS NPI_NMBR,NPI_CNFRM_IND AS NPI_CNFRM_IND,EFCTV_DATE AS EFCTV_DATE,ENDNG_DATE AS ENDNG_DATE,DCTVT_MTHD_CODE AS DCTVT_MTHD_CODE,CRT_USER_CODE AS CRT_USER_CODE,DCTVT_DTTM AS DCTVT_DTTM,DCTVT_USER_CODE AS DCTVT_USER_CODE,UPDT_PRGRM_NAME AS UPDT_PRGRM_NAME 
			from      LOGIC_PNPI
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_PNPI as ( SELECT  * 
			from     RENAME_PNPI 
            
        )
----JOIN LAYER----
,
 JOIN_PNPI as ( SELECT * 
			from  FILTER_PNPI )
 SELECT * FROM JOIN_PNPI