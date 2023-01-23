 
----SRC LAYER----
WITH
SRC_IS as ( SELECT *     from      STAGING.DST_INVOICE_STATUS )

----LOGIC LAYER----
,
LOGIC_IS as ( SELECT 
		UNIQUE_ID_KEY AS UNIQUE_ID_KEY,
		INVOICE_STATUS_HKEY AS INVOICE_STATUS_HKEY,
		INVOICE_STATUS_CODE AS INVOICE_STATUS_CODE,
		INVOICE_STATUS_DESC AS INVOICE_STATUS_DESC,
		INVOICE_STATUS_EFFECTIVE_DATE AS INVOICE_STATUS_EFFECTIVE_DATE,
		INVOICE_STATUS_END_DATE AS INVOICE_STATUS_END_DATE,
		CURRENT_IND AS CURRENT_IND 
				from SRC_IS
            )
----RENAME LAYER ----
,
RENAME_IS as ( SELECT UNIQUE_ID_KEY AS UNIQUE_ID_KEY,INVOICE_STATUS_HKEY AS INVOICE_STATUS_HKEY,
			
			INVOICE_STATUS_CODE AS MEDICAL_INVOICE_STATUS_CODE,
			
			INVOICE_STATUS_DESC AS MEDICAL_INVOICE_STATUS_DESC,
			
			INVOICE_STATUS_EFFECTIVE_DATE AS EFFECTIVE_DATE,
			
			INVOICE_STATUS_END_DATE AS END_DATE,
			
			CURRENT_IND AS CURRENT_INDICATOR 
			from      LOGIC_IS
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_IS as ( SELECT  * 
			from     RENAME_IS 
            
        )
----JOIN LAYER----
,
 JOIN_IS as ( SELECT * 
			from  FILTER_IS )
 SELECT * FROM JOIN_IS