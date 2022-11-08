

      create or replace  table DEV_EDW.STAGING.STG_BWC_DRUG_CATEGORY_REFERENCE  as
      (----SRC LAYER----
WITH
SRC_DCF as ( SELECT *     from      DEV_VIEWS.BWC_FILES.BWC_DRUG_CATEGORY_REFERENCE ),
----LOGIC LAYER----
LOGIC_DCF as ( SELECT 
	UPPER(LPAD(TRIM(MEDISPAN_4_GPI_CLASS_CODE),4,'0'))  AS MEDISPAN_4_GPI_CLASS_CODE,
		UPPER(MEDISPAN_4_GPI_CLASS_NAME) AS MEDISPAN_4_GPI_CLASS_NAME,
		UPPER(BWC_DRUG_CATEGORY_NAME) AS BWC_DRUG_CATEGORY_NAME 
				from SRC_DCF
            )
----RENAME LAYER ----
,
RENAME_DCF as ( SELECT MEDISPAN_4_GPI_CLASS_CODE AS MEDISPAN_4_GPI_CLASS_CODE,MEDISPAN_4_GPI_CLASS_NAME AS MEDISPAN_4_GPI_CLASS_NAME,BWC_DRUG_CATEGORY_NAME AS BWC_DRUG_CATEGORY_NAME 
			from      LOGIC_DCF
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_DCF as ( SELECT  * 
			from     RENAME_DCF 
            
        )
----JOIN LAYER----
,
 JOIN_DCF as ( SELECT * 
			from  FILTER_DCF )
 SELECT * FROM JOIN_DCF
      );
    