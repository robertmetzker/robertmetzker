----SRC LAYER----
WITH
SRC_FSC as ( SELECT *     from      STAGING.STG_FSC ),
SRC_PAYCAT as ( SELECT *     from      STAGING.STG_REVENUE_CODE_PAYMENT_CATEGORY )
//SRC_FSC as ( SELECT *     from      STG_FSC),
//SRC_PAYCAT as ( SELECT *     from      STG_REVENUE_CODE_PAYMENT_CATEGORY)
----LOGIC LAYER----
,
LOGIC_FSC as ( SELECT 
		
		TRIM(SERVICE_CODE) AS SERVICE_CODE,
		UPPER(TRIM(SERVICE_LONG_DESC)) AS SERVICE_LONG_DESC,
		EFFECTIVE_DATE AS EFFECTIVE_DATE,
		EXPIRATION_DATE AS EXPIRATION_DATE 
				from SRC_FSC
            ),
LOGIC_PAYCAT as ( SELECT 
		UPPER(TRIM(PAYMENT_CATEGORY)) AS PAYMENT_CATEGORY,
		TRIM(REVENUE_CODE) AS REVENUE_CODE 
				from SRC_PAYCAT
            )
----RENAME LAYER ----
,
RENAME_FSC as ( SELECT 
			
			SERVICE_CODE AS SERVICE_CODE,SERVICE_LONG_DESC AS SERVICE_LONG_DESC,EFFECTIVE_DATE AS EFFECTIVE_DATE,EXPIRATION_DATE AS EXPIRATION_DATE 
			from      LOGIC_FSC
        ),
RENAME_PAYCAT as ( SELECT PAYMENT_CATEGORY AS PAYMENT_CATEGORY,REVENUE_CODE AS REVENUE_CODE 
			from      LOGIC_PAYCAT
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_FSC as ( SELECT  * 
			from     RENAME_FSC 
            WHERE EXPIRATION_DATE IS NULL AND LENGTH(SERVICE_CODE) = 4
        ),

        FILTER_PAYCAT as ( SELECT  * 
			from     RENAME_PAYCAT 
            
        )
----JOIN LAYER----
,
FSC as ( SELECT * 
			from  FILTER_FSC
				LEFT JOIN FILTER_PAYCAT ON FILTER_FSC.SERVICE_CODE = FILTER_PAYCAT.REVENUE_CODE  ),

-- ETL LAYER -----

ETL AS ( SELECT md5(cast(
    
    coalesce(cast(SERVICE_CODE as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY	,SERVICE_CODE
,SERVICE_LONG_DESC
,PAYMENT_CATEGORY
,EFFECTIVE_DATE
,EXPIRATION_DATE
,CASE WHEN REVENUE_CODE = '-1' THEN REVENUE_CODE ELSE LPAD(TRIM(REVENUE_CODE), 4, '0') END AS REVENUE_CODE 
 FROM FSC)
select *  from ETL