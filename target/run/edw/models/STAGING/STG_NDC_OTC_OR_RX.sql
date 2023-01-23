

      create or replace  table DEV_EDW.STAGING.STG_NDC_OTC_OR_RX  as
      (----SRC LAYER----
WITH
SRC_MDNO as ( SELECT *     from      DEV_VIEWS.DBEABP00.TEAMDNO ),
SRC_MNOT as ( SELECT *     from      DEV_VIEWS.DBEABP00.TEAMNOT )
//SRC_MDNO as ( SELECT *     from      TEAMDNO),
//SRC_MNOT as ( SELECT *     from      TEAMNOT)
----LOGIC LAYER----
,
LOGIC_MDNO as ( SELECT 
		case when TRIM(FK_NDCL_CODE)= '' then NULL else trim(FK_NDCL_CODE) end AS FK_NDCL_CODE,
		case when TRIM(FK_DCPR_CODE)= '' then null else trim(FK_DCPR_CODE) end  AS FK_DCPR_CODE,
		case when TRIM(FK_NDCP_CODE) = '' then null else trim(FK_NDCP_CODE) end AS FK_NDCP_CODE,
		FK_NDCV_NMBR AS FK_NDCV_NMBR,
		case when TRIM(FK_MNOT_CODE)= '' then Null else trim(FK_MNOT_CODE) END AS FK_MNOT_CODE,
		CAST(MDNO_CRT_DTTM AS TIMESTAMP) AS MDNO_CRT_DTTM,
		cast(EFCTV_DATE as DATE) AS EFCTV_DATE,
		cast(ENDNG_DATE as DATE) AS ENDNG_DATE,
		case when TRIM(CRT_USER_CODE)= '' then null else trim(CRT_USER_CODE) end AS CRT_USER_CODE,
		case when TRIM(CRT_PRGRM_NAME)= '' then null else trim(CRT_PRGRM_NAME) end AS CRT_PRGRM_NAME,
		case when TRIM(DCTVT_USER_CODE)= '' then null else trim(DCTVT_USER_CODE) end AS DCTVT_USER_CODE,
		case when TRIM(DCTVT_PRGRM_NAME)='' then NULL else trim(DCTVT_PRGRM_NAME) end AS DCTVT_PRGRM_NAME,
		case when DCTVT_DTTM = '10000-01-01 00:00:00' THEN '2999-12-31 00:00:00' ELSE DCTVT_DTTM END::TIMESTAMP AS DCTVT_DTTM 
				from SRC_MDNO
            ),
LOGIC_MNOT as ( SELECT 
		case when TRIM(NAME)='' then null else trim(NAME) END AS NAME,
		cast(EFCTV_DATE as DATE) AS EFCTV_DATE,
		cast(ENDNG_DATE as DATE) AS ENDNG_DATE,
		CAST(CRT_DTTM AS TIMESTAMP) AS CRT_DTTM,
		case when DCTVT_DTTM = '10000-01-01 00:00:00' THEN '2999-12-31 00:00:00' ELSE DCTVT_DTTM END::TIMESTAMP AS DCTVT_DTTM,
		CASE WHEN TRIM(MNOT_CODE)= '' THEN NULL ELSE TRIM(MNOT_CODE) END AS MNOT_CODE 
				from SRC_MNOT
            )
----RENAME LAYER ----
,
RENAME_MDNO as ( SELECT FK_NDCL_CODE AS FK_NDCL_CODE,FK_DCPR_CODE AS FK_DCPR_CODE,FK_NDCP_CODE AS FK_NDCP_CODE,FK_NDCV_NMBR AS FK_NDCV_NMBR,FK_MNOT_CODE AS FK_MNOT_CODE,MDNO_CRT_DTTM AS MDNO_CRT_DTTM,EFCTV_DATE AS EFCTV_DATE,ENDNG_DATE AS ENDNG_DATE,CRT_USER_CODE AS CRT_USER_CODE,CRT_PRGRM_NAME AS CRT_PRGRM_NAME,DCTVT_USER_CODE AS DCTVT_USER_CODE,DCTVT_PRGRM_NAME AS DCTVT_PRGRM_NAME,DCTVT_DTTM AS DCTVT_DTTM 
			from      LOGIC_MDNO
        ),
RENAME_MNOT as ( SELECT 
			
			NAME AS MNOT_NAME,
			
			EFCTV_DATE AS MNOT_EFCTV_DATE,
			
			ENDNG_DATE AS MNOT_ENDNG_DATE,
			
			CRT_DTTM AS MNOT_CRT_DTTM,
			
			DCTVT_DTTM AS MNOT_DCTVT_DTTM,MNOT_CODE AS MNOT_CODE 
			from      LOGIC_MNOT
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_MDNO as ( SELECT  * 
			from     RENAME_MDNO 
            
        ),

        FILTER_MNOT as ( SELECT  * 
			from     RENAME_MNOT 
            WHERE MNOT_DCTVT_DTTM > CURRENT_DATE AND MNOT_EFCTV_DATE <= CURRENT_DATE AND MNOT_ENDNG_DATE >= CURRENT_DATE
        )
----JOIN LAYER----
,
MDNO as ( SELECT * 
			from  FILTER_MDNO
				LEFT JOIN FILTER_MNOT ON FILTER_MDNO.FK_MNOT_CODE = FILTER_MNOT.MNOT_CODE  )
select * from MDNO
      );
    