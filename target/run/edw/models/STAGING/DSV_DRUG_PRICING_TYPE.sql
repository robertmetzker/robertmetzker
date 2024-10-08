
  create or replace  view DEV_EDW.STAGING.DSV_DRUG_PRICING_TYPE  as (
    

----SRC LAYER----
WITH
SRC_DPT as ( SELECT *     from      STAGING.DST_DRUG_PRICING_TYPE )
//SRC_DPT as ( SELECT *     from      DST_DRUG_PRICING_TYPE)
----LOGIC LAYER----
,
LOGIC_DPT as ( SELECT 
		UNIQUE_ID_KEY AS UNIQUE_ID_KEY,
		NDPT_CODE AS NDPT_CODE,
		NAME AS NAME,
		EFCTV_DATE AS EFCTV_DATE,
		ENDNG_DATE AS ENDNG_DATE 
				from SRC_DPT
            )
----RENAME LAYER ----
,
RENAME_DPT as ( SELECT UNIQUE_ID_KEY AS UNIQUE_ID_KEY,
			
			NDPT_CODE AS PRICE_TYPE_CODE,
			
			NAME AS PRICE_TYPE_DESC,EFCTV_DATE AS EFCTV_DATE,ENDNG_DATE AS ENDNG_DATE 
			from      LOGIC_DPT
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_DPT as ( SELECT  * 
			from     RENAME_DPT 
            
        )
----JOIN LAYER----
,
 JOIN_DPT as ( SELECT * 
			from  FILTER_DPT )
 SELECT * FROM JOIN_DPT
  );
