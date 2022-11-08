
  create or replace  view DEV_EDW.STAGING.DSV_REVENUE_CODE  as (
    

----SRC LAYER----
WITH
SRC_RC as ( SELECT *     from      STAGING.DST_REVENUE_CODE )
//SRC_RC as ( SELECT *     from      DST_REVENUE_CODE)
----LOGIC LAYER----
,
LOGIC_RC as ( SELECT 
		TRIM(UNIQUE_ID_KEY) AS UNIQUE_ID_KEY,
		TRIM(SERVICE_CODE) AS SERVICE_CODE,
		TRIM(SERVICE_LONG_DESC) AS SERVICE_LONG_DESC,
		TRIM(PAYMENT_CATEGORY) AS PAYMENT_CATEGORY,
		EFFECTIVE_DATE AS EFFECTIVE_DATE,
		EXPIRATION_DATE AS EXPIRATION_DATE 
		 
				from SRC_RC
            )
----RENAME LAYER ----  
,
RENAME_RC as ( SELECT UNIQUE_ID_KEY AS UNIQUE_ID_KEY,
			
			SERVICE_CODE AS HOSPITAL_REVENUE_CENTER_CODE,
			
			SERVICE_LONG_DESC AS HOSPITAL_REVENUE_CENTER_DESC,
			
			PAYMENT_CATEGORY AS REVENUE_CENTER_PAYMENT_CATEGORY,

			EFFECTIVE_DATE AS REVENUE_CENTER_EFFECTIVE_DATE,
			
			EXPIRATION_DATE AS REVENUE_CENTER_EXPIRATION_DATE 
			
			from      LOGIC_RC
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_RC as ( SELECT  * 
			from     RENAME_RC 
            
        )
----JOIN LAYER----
,
 JOIN_RC as ( SELECT * 
			from  FILTER_RC ),
--- ETL LAYER ----
ETL AS ( SELECT UNIQUE_ID_KEY
,HOSPITAL_REVENUE_CENTER_CODE
,HOSPITAL_REVENUE_CENTER_DESC
,REVENUE_CENTER_PAYMENT_CATEGORY
,REVENUE_CENTER_EFFECTIVE_DATE
,REVENUE_CENTER_EXPIRATION_DATE
 FROM JOIN_RC)
SELECT * FROM ETL
  );
