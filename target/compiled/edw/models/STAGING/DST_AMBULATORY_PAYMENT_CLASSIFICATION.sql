----SRC LAYER----
WITH
SRC_APC as ( SELECT *     from      STAGING.STG_APC_INFO ),
----LOGIC LAYER----

LOGIC_APC as ( SELECT 
		APC_CODE AS APC_CODE,
		EFFECTIVE_DATE AS EFFECTIVE_DATE,
		EXPIRATION_DATE AS EXPIRATION_DATE,
		DESCRIPTION AS DESCRIPTION,
		STATUS AS STATUS,
		RELATIVE_WEIGHT AS RELATIVE_WEIGHT,
		APC_AMOUNT AS APC_AMOUNT,
		STATUS_DESC AS STATUS_DESC 
				from SRC_APC
            )
----RENAME LAYER ----
,
RENAME_APC as ( SELECT APC_CODE AS APC_CODE,EFFECTIVE_DATE AS EFFECTIVE_DATE,EXPIRATION_DATE AS EXPIRATION_DATE,DESCRIPTION AS DESCRIPTION,STATUS AS STATUS,RELATIVE_WEIGHT AS RELATIVE_WEIGHT,APC_AMOUNT AS APC_AMOUNT,STATUS_DESC AS STATUS_DESC 
			from      LOGIC_APC
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_APC as ( SELECT  *,row_number() over(PARTITION BY APC_CODE ORDER BY EFFECTIVE_DATE DESC) AS ROWN 
			from     RENAME_APC 
            qualify ROWN=1
        )
----JOIN LAYER----
,
 JOIN_APC as ( SELECT * 
			from  FILTER_APC )
----ETL LAYER----
,
ETL1 as (SELECT md5(cast(
    
    coalesce(cast(APC_CODE as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY,
			APC_CODE,
			EFFECTIVE_DATE,
			EXPIRATION_DATE,
			DESCRIPTION,
			STATUS,
			RELATIVE_WEIGHT,
			APC_AMOUNT,
			STATUS_DESC
        from JOIN_APC)

SELECT * FROM ETL1