----SRC LAYER----
WITH
SRC_MNDC as ( SELECT *     from      DEV_VIEWS.DBEABP00.TEAMNDC ),
SRC_MDCT as ( SELECT *     from      DEV_VIEWS.DBEABP00.TEAMDCT ),
//SRC_MNDC as ( SELECT *     from      TEAMNDC),
//SRC_MDCT as ( SELECT *     from      TEAMDCT)
----LOGIC LAYER----

LOGIC_MNDC as ( SELECT 
		case when TRIM(FK_NDCL_CODE) ='' then NULL else trim(FK_NDCL_CODE) end  AS FK_NDCL_CODE,
		case when TRIM(FK_DCPR_CODE)= '' then null else trim(FK_DCPR_CODE) end AS FK_DCPR_CODE,
		case when TRIM(FK_NDCP_CODE) = '' then null else trim(FK_NDCP_CODE) end AS FK_NDCP_CODE,
		FK_NDCV_NMBR AS FK_NDCV_NMBR,
		case when TRIM(FK_MDCT_CODE)='' then '0' else trim(FK_MDCT_CODE) end AS FK_MDCT_CODE,
		MNDC_CRT_DTTM::timestamp AS MNDC_CRT_DTTM,
		cast(EFCTV_DATE as DATE) AS EFCTV_DATE,
		cast(ENDNG_DATE as DATE) AS ENDNG_DATE,
		case when TRIM(CRT_USER_CODE) = '' then NULL else trim(CRT_USER_CODE) end AS CRT_USER_CODE,
		case when TRIM(CRT_PRGRM_NAME) = '' then NULL else trim(CRT_PRGRM_NAME) end AS CRT_PRGRM_NAME,
		case when TRIM(DCTVT_USER_CODE)= '' then NULL else trim(DCTVT_USER_CODE) end AS DCTVT_USER_CODE,
		case when TRIM(DCTVT_PRGRM_NAME)= ''  then NULL else trim(DCTVT_PRGRM_NAME) end AS DCTVT_PRGRM_NAME,
		case when DCTVT_DTTM = '10000-01-01 00:00:00' THEN '2999-12-31 00:00:00' ELSE DCTVT_DTTM END::TIMESTAMP AS DCTVT_DTTM
				from SRC_MNDC
            ),
LOGIC_MDCT as ( SELECT 
		case when TRIM(NAME)= '' then  null else trim(name) end  AS NAME,
		cast(EFCTV_DATE as DATE) AS EFCTV_DATE,
		cast(ENDNG_DATE as DATE) AS ENDNG_DATE,
		CRT_DTTM::TIMESTAMP AS CRT_DTTM,
		case when DCTVT_DTTM = '10000-01-01 00:00:00' THEN '2999-12-31 00:00:00' ELSE DCTVT_DTTM END::TIMESTAMP AS DCTVT_DTTM,
		case when TRIM(MDCT_CODE) = '' then '0' else trim(MDCT_CODE) end AS MDCT_CODE 
				from SRC_MDCT
            )
----RENAME LAYER ----
,
RENAME_MNDC as ( SELECT FK_NDCL_CODE AS FK_NDCL_CODE,FK_DCPR_CODE AS FK_DCPR_CODE,FK_NDCP_CODE AS FK_NDCP_CODE,FK_NDCV_NMBR AS FK_NDCV_NMBR,FK_MDCT_CODE AS FK_MDCT_CODE,MNDC_CRT_DTTM AS MNDC_CRT_DTTM,EFCTV_DATE AS EFCTV_DATE,ENDNG_DATE AS ENDNG_DATE,CRT_USER_CODE AS CRT_USER_CODE,CRT_PRGRM_NAME AS CRT_PRGRM_NAME,DCTVT_USER_CODE AS DCTVT_USER_CODE,DCTVT_PRGRM_NAME AS DCTVT_PRGRM_NAME,DCTVT_DTTM AS DCTVT_DTTM 
			from      LOGIC_MNDC
        ),
RENAME_MDCT as ( SELECT 
			
			NAME AS MDCT_NAME,
			
			EFCTV_DATE AS MDCT_EFCTV_DATE,
			
			ENDNG_DATE AS MDCT_ENDNG_DATE,
			
			CRT_DTTM AS MDCT_CRT_DTTM,
			
			DCTVT_DTTM AS MDCT_DCTVT_DTTM,MDCT_CODE AS MDCT_CODE 
			from      LOGIC_MDCT
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_MNDC as ( SELECT  * 
			from     RENAME_MNDC 
            
        ),

        FILTER_MDCT as ( SELECT  * 
			from     RENAME_MDCT 
            WHERE MDCT_DCTVT_DTTM > CURRENT_DATE
        )
----JOIN LAYER----
,
JOIN_MNDC as ( SELECT * 
			from  FILTER_MNDC
				LEFT JOIN FILTER_MDCT ON COALESCE(FK_MDCT_CODE, '0') = COALESCE(MDCT_CODE, '0')  )
select * from JOIN_MNDC