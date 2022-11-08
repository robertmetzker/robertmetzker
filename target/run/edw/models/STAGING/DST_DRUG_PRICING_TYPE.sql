

      create or replace  table DEV_EDW.STAGING.DST_DRUG_PRICING_TYPE  as
      (----SRC LAYER----
WITH
SRC_TEA as ( SELECT *     from      STAGING.STG_TEANDPT )
//SRC_TEA as ( SELECT *     from      STG_TEANDPT)
----LOGIC LAYER----
,
LOGIC_TEA as ( SELECT 
		md5(cast(
    
    coalesce(cast(NDPT_CODE as 
    varchar
), '')

 as 
    varchar
))   as  UNIQUE_ID_KEY ,
		CASE WHEN trim(NDPT_CODE) = 'A' THEN 'AWP'
             WHEN trim(NDPT_CODE) = 'BB' THEN 'BBU'
            WHEN trim(NDPT_CODE) = 'D' THEN 'DP'
            WHEN trim(NDPT_CODE) = 'H' THEN 'CMS'
            WHEN trim(NDPT_CODE) = 'U' THEN 'CMSU'
            WHEN trim(NDPT_CODE) = 'W' THEN 'WAC' else trim(NDPT_CODE) end as NDPT_CODE,
		CRT_DTTM AS CRT_DTTM,
            CASE WHEN trim(NAME) = 'AWP' THEN 'AVERAGE WHOLESALE PRICE'
                WHEN trim(NAME) = 'DP' THEN 'DIRECT PRICE (MANUFACTURER)'
                WHEN trim(NAME) = 'HCFA FFP' THEN 'FEDERAL UPPER LIMIT (CMS FUL)'
                WHEN trim(NAME) = 'HCFA FFP FOR UNIT DOSE ITEMS' THEN 'FEDERAL UPPER LIMIT (CMS FUL) UNIT PRICE'
                WHEN trim(NAME) = 'WAC' THEN 'WHOLESALE ACQUISITION COST'  else trim(name) end   AS NAME,
		EFCTV_DATE AS EFCTV_DATE,
		ENDNG_DATE AS ENDNG_DATE,
		DCTVT_DTTM AS DCTVT_DTTM,
		CRT_USER_CODE AS CRT_USER_CODE,
		CRT_PRGRM_NAME AS CRT_PRGRM_NAME 
				from SRC_TEA
            )
----RENAME LAYER ----
,
RENAME_TEA as ( SELECT 
			
			UNIQUE_ID_KEY AS UNIQUE_ID_KEY,NDPT_CODE AS NDPT_CODE,CRT_DTTM AS CRT_DTTM,NAME AS NAME,EFCTV_DATE AS EFCTV_DATE,ENDNG_DATE AS ENDNG_DATE,DCTVT_DTTM AS DCTVT_DTTM,CRT_USER_CODE AS CRT_USER_CODE,CRT_PRGRM_NAME AS CRT_PRGRM_NAME 
			from      LOGIC_TEA
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_TEA as ( SELECT  * 
			from     RENAME_TEA 
            
        )
----JOIN LAYER----
,
 JOIN_TEA as ( SELECT * 
			from  FILTER_TEA )
 SELECT * FROM JOIN_TEA
      );
    