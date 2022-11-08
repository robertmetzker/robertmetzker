

      create or replace  table DEV_EDW.STAGING.DST_PBM_PRICING_METHOD  as
      (----SRC LAYER----
WITH
SRC_ILP as ( SELECT *     from      STAGING.STG_INVOICE_LINE_PHARM ),
SRC_P as ( SELECT *     from      STAGING.STG_TCDPBMP ),
SRC_T as ( SELECT *     from      STAGING.STG_TCDPBMT ),
----LOGIC LAYER----
LOGIC_ILP as ( SELECT 
		TRIM(CLIENT_PRICING) AS CLIENT_PRICING,
		TRIM(PRICE_TYPE) AS PRICE_TYPE 
				from SRC_ILP
            ),
LOGIC_P as ( SELECT 
		TRIM(PBM_PRCNG_SRC_CODE) AS PBM_PRCNG_SRC_CODE,
		TRIM(PBM_PRCNG_SRC_DESC) AS PBM_PRCNG_SRC_DESC 
				from SRC_P
            ),
LOGIC_T as ( SELECT 
	        TRIM(PBM_PRCNG_CODE) AS PBM_PRCNG_CODE,
		TRIM(PBM_PRCNG_DESC) AS PBM_PRCNG_DESC 
				from SRC_T
            )
----RENAME LAYER ----
,
RENAME_ILP as ( SELECT CLIENT_PRICING AS CLIENT_PRICING,PRICE_TYPE AS PRICE_TYPE 
			from      LOGIC_ILP
        ),
RENAME_P as ( SELECT PBM_PRCNG_SRC_CODE AS PBM_PRCNG_SRC_CODE,PBM_PRCNG_SRC_DESC AS PBM_PRCNG_SRC_DESC 
			from      LOGIC_P
        ),
RENAME_T as ( SELECT PBM_PRCNG_CODE AS PBM_PRCNG_CODE,PBM_PRCNG_DESC AS PBM_PRCNG_DESC 
			from      LOGIC_T
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_ILP as ( SELECT  * 
			from     RENAME_ILP 
            
        ),

        FILTER_P as ( SELECT  * 
			from     RENAME_P 
            
        ),

        FILTER_T as ( SELECT  * 
			from     RENAME_T 
                        
          
        )
----JOIN LAYER----
,
ILP as ( SELECT distinct PBM_PRCNG_SRC_CODE,
                        PBM_PRCNG_SRC_DESC,
                        case when TRIM(PBM_PRCNG_CODE) = 'AWP' then 'AW' 
           when TRIM(PBM_PRCNG_CODE) = 'MACBWC' THEN 'MA'
		   WHEN TRIM(PBM_PRCNG_CODE) = 'PLAN MAC' THEN 'MA'
           when TRIM(PBM_PRCNG_CODE) = 'U' THEN 'UC'
		   WHEN TRIM(PBM_PRCNG_CODE) =  'U&C' then 'UC' else trim(PBM_PRCNG_CODE) end as PBM_PRCNG_CODE,
                         PBM_PRCNG_DESC

			from  FILTER_ILP
				LEFT JOIN FILTER_P ON FILTER_ILP.CLIENT_PRICING = FILTER_P.PBM_PRCNG_SRC_CODE 
				LEFT JOIN FILTER_T ON FILTER_ILP.PRICE_TYPE = FILTER_T.PBM_PRCNG_CODE  )
----ETL LAYER----
,
ETL1 as ( SELECT
        distinct md5(cast(
    
    coalesce(cast(PBM_PRCNG_CODE as 
    varchar
), '') || '-' || coalesce(cast(PBM_PRCNG_SRC_CODE as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY,
        *                FROM ILP),
MAXETL AS ( SELECT UNIQUE_ID_KEY, PBM_PRCNG_SRC_CODE,PBM_PRCNG_SRC_DESC,PBM_PRCNG_CODE,MAX(PBM_PRCNG_DESC) AS PBM_PRCNG_DESC  FROM  ETL1 GROUP BY UNIQUE_ID_KEY, PBM_PRCNG_SRC_CODE,PBM_PRCNG_SRC_DESC,PBM_PRCNG_CODE )
SELECT * FROM MAXETL ORDER BY PBM_PRCNG_CODE
      );
    