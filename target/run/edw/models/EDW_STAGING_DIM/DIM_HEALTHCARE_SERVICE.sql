

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_HEALTHCARE_SERVICE  as
      ( 
----SRC LAYER----
WITH
SRC_CPT as ( SELECT *     from      EDW_STAGING_DIM.DIM_CPT ),
SRC_NDC as ( SELECT *     from      EDW_STAGING_DIM.DIM_NDC ),
SRC_RC as ( SELECT *      from      EDW_STAGING_DIM.DIM_REVENUE_CENTER )
----LOGIC LAYER----
,
LOGIC_CPT as ( SELECT 
		CPT_HKEY AS CPT_HKEY,
		PROCEDURE_CODE AS PROCEDURE_CODE,
		PROCEDURE_DESC AS PROCEDURE_DESC,
		CPT_PAYMENT_CATEGORY AS CPT_PAYMENT_CATEGORY,
		CPT_PAYMENT_SUBCATEGORY AS CPT_PAYMENT_SUBCATEGORY,
		CPT_FEE_SCHEDULE_DESC AS CPT_FEE_SCHEDULE_DESC,
		'CPT' AS SERVICE_CODE_TYPE,
		PROCEDURE_CODE_ENTRY_DATE AS PROCEDURE_CODE_ENTRY_DATE,
		PROCEDURE_CODE_EFFECTIVE_DATE AS PROCEDURE_CODE_EFFECTIVE_DATE, 
		PROCEDURE_CODE_END_DATE AS PROCEDURE_CODE_END_DATE,
		PROCEDURE_SERVICE_TYPE_DESC AS PROCEDURE_SERVICE_TYPE_DESC,
		CURRENT_RECORD_IND AS CURRENT_RECORD_IND,
		RECORD_EFFECTIVE_DATE AS RECORD_EFFECTIVE_DATE,
		RECORD_END_DATE AS RECORD_END_DATE,
		NULL AS GPI_4_CLASS_CODE,
        NULL AS GPI_4_CLASS_DESC
				from SRC_CPT
            ),
LOGIC_NDC as ( SELECT 
        'NDC' AS SERVICE_CODE_TYPE,
		GPI_4_CLASS_CODE AS GPI_4_CLASS_CODE,
		GPI_4_CLASS_DESC AS GPI_4_CLASS_DESC,
		RECORD_EFFECTIVE_DATE AS RECORD_EFFECTIVE_DATE,
		RECORD_END_DATE AS RECORD_END_DATE,
		CURRENT_RECORD_IND AS CURRENT_RECORD_IND,
		NDC_GPI_HKEY AS NDC_GPI_HKEY,
		NDC_11_CODE AS NDC_11_CODE,
		NULL AS CODE_DESC,
		NULL AS PAYMENT_CATEGORY,
		'NOT APPLICABLE' AS PAYMENT_SUBCATEGORY,
		NULL AS FEE_SCHEDULE_DESC,
		try_to_date('invalid') AS PROCEDURE_CODE_ENTRY_DATE,
		try_to_date('invalid') AS PROCEDURE_CODE_EFFECTIVE_DATE,
		try_to_date('invalid') AS PROCEDURE_CODE_END_DATE,
		'NOT APPLICABLE' AS  PROCEDURE_SERVICE_TYPE_DESC
				from SRC_NDC
            ),
LOGIC_RC as ( SELECT 
		REVENUE_CENTER_HKEY AS REVENUE_CENTER_HKEY,
		TRIM(HOSPITAL_REVENUE_CENTER_CODE) AS HOSPITAL_REVENUE_CENTER_CODE,
		TRIM(HOSPITAL_REVENUE_CENTER_DESC) AS HOSPITAL_REVENUE_CENTER_DESC,
		TRIM(REVENUE_CENTER_PAYMENT_CATEGORY) AS REVENUE_CENTER_PAYMENT_CATEGORY,
		'NOT APPLICABLE' AS PAYMENT_SUBCATEGORY,
		'RC' AS SERVICE_CODE_TYPE,
		CURRENT_RECORD_IND AS CURRENT_RECORD_IND,
		RECORD_EFFECTIVE_DATE AS RECORD_EFFECTIVE_DATE,
		RECORD_END_DATE AS RECORD_END_DATE,
		NULL AS FEE_SCHEDULE_DESC,
		try_to_date('invalid') AS PROCEDURE_CODE_ENTRY_DATE,
		try_to_date('invalid') AS PROCEDURE_CODE_EFFECTIVE_DATE,
		try_to_date('invalid') AS PROCEDURE_CODE_END_DATE,
		'NOT APPLICABLE' AS PROCEDURE_SERVICE_TYPE_DESC,
		NULL as GPI_4_CLASS_CODE,
		NULL as GPI_4_CLASS_DESC
		 
				from SRC_RC
            )
----RENAME LAYER ----
,
RENAME_CPT as ( SELECT 
			
			CPT_HKEY as UNIQUE_ID_KEY,

			CPT_HKEY AS HEALTHCARE_SERVICE_HKEY,
			
			PROCEDURE_CODE AS CODE,
			
			PROCEDURE_DESC AS CODE_DESC,
			
			CPT_PAYMENT_CATEGORY AS PAYMENT_CATEGORY,

			CPT_PAYMENT_SUBCATEGORY AS PAYMENT_SUBCATEGORY,
			
			CPT_FEE_SCHEDULE_DESC AS FEE_SCHEDULE_DESC,
			
			SERVICE_CODE_TYPE AS SERVICE_CODE_TYPE,
			
			PROCEDURE_CODE_ENTRY_DATE AS PROCEDURE_CODE_ENTRY_DATE,PROCEDURE_CODE_EFFECTIVE_DATE AS PROCEDURE_CODE_EFFECTIVE_DATE,
			
			PROCEDURE_CODE_END_DATE AS PROCEDURE_CODE_END_DATE,CURRENT_RECORD_IND AS CURRENT_RECORD_IND,RECORD_EFFECTIVE_DATE AS RECORD_EFFECTIVE_DATE,RECORD_END_DATE AS RECORD_END_DATE,

			PROCEDURE_SERVICE_TYPE_DESC AS PROCEDURE_SERVICE_TYPE_DESC,

			GPI_4_CLASS_CODE AS GPI_4_CLASS_CODE,
			
			GPI_4_CLASS_DESC AS GPI_4_CLASS_DESC 
			from      LOGIC_CPT
        ),
RENAME_NDC as ( SELECT 

			NDC_GPI_HKEY AS UNIQUE_ID_KEY,

			GPI_4_CLASS_CODE AS GPI_4_CLASS_CODE,

			GPI_4_CLASS_DESC AS GPI_4_CLASS_DESC,

			RECORD_EFFECTIVE_DATE AS RECORD_EFFECTIVE_DATE,RECORD_END_DATE AS RECORD_END_DATE,CURRENT_RECORD_IND AS CURRENT_RECORD_IND,
			
			NDC_GPI_HKEY AS HEALTHCARE_SERVICE_HKEY,
			
			NDC_11_CODE AS CODE,
			
			CODE_DESC AS CODE_DESC,

			SERVICE_CODE_TYPE AS SERVICE_CODE_TYPE,
			
			PAYMENT_CATEGORY AS PAYMENT_CATEGORY,

			PAYMENT_SUBCATEGORY AS PAYMENT_SUBCATEGORY,
			
			FEE_SCHEDULE_DESC AS FEE_SCHEDULE_DESC,
			
			PROCEDURE_CODE_ENTRY_DATE AS PROCEDURE_CODE_ENTRY_DATE,
			
			PROCEDURE_CODE_EFFECTIVE_DATE AS PROCEDURE_CODE_EFFECTIVE_DATE,
			
			PROCEDURE_CODE_END_DATE AS PROCEDURE_CODE_END_DATE, 

			PROCEDURE_SERVICE_TYPE_DESC AS PROCEDURE_SERVICE_TYPE_DESC

			from      LOGIC_NDC
        ),
RENAME_RC as ( SELECT 
			
			REVENUE_CENTER_HKEY AS UNIQUE_ID_KEY,
			
			REVENUE_CENTER_HKEY AS HEALTHCARE_SERVICE_HKEY,
			
			HOSPITAL_REVENUE_CENTER_CODE AS CODE,
			
			HOSPITAL_REVENUE_CENTER_DESC AS CODE_DESC,
			
			REVENUE_CENTER_PAYMENT_CATEGORY AS PAYMENT_CATEGORY,

			PAYMENT_SUBCATEGORY AS PAYMENT_SUBCATEGORY,
			
			SERVICE_CODE_TYPE AS SERVICE_CODE_TYPE,CURRENT_RECORD_IND AS CURRENT_RECORD_IND,RECORD_EFFECTIVE_DATE AS RECORD_EFFECTIVE_DATE,RECORD_END_DATE AS RECORD_END_DATE,
			
			FEE_SCHEDULE_DESC AS FEE_SCHEDULE_DESC,
			
			PROCEDURE_CODE_ENTRY_DATE AS PROCEDURE_CODE_ENTRY_DATE,
			
			PROCEDURE_CODE_EFFECTIVE_DATE AS PROCEDURE_CODE_EFFECTIVE_DATE,
			
			PROCEDURE_CODE_END_DATE AS PROCEDURE_CODE_END_DATE,

            PROCEDURE_SERVICE_TYPE_DESC AS PROCEDURE_SERVICE_TYPE_DESC,
			
			GPI_4_CLASS_CODE AS GPI_4_CLASS_CODE,
			
			GPI_4_CLASS_DESC AS GPI_4_CLASS_DESC 
			from      LOGIC_RC
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_CPT as ( SELECT  * 
			from     RENAME_CPT 
			where HEALTHCARE_SERVICE_HKEY not in (md5('99999'), md5('-2222'), md5('-1111'))
            
        ),

        FILTER_NDC as ( SELECT  * 
			from     RENAME_NDC 
            where HEALTHCARE_SERVICE_HKEY not in (md5('99999'), md5('-2222'), md5('-1111'))
        ),
		FILTER_RC as ( SELECT  * 
			from     RENAME_RC 
            where HEALTHCARE_SERVICE_HKEY not in (md5('99999'), md5('-1111'))
        )
----JOIN LAYER----
,
 JOIN_NDC as ( SELECT 
 		UNIQUE_ID_KEY,
		HEALTHCARE_SERVICE_HKEY,
		CODE,
		CODE_DESC,
		PAYMENT_CATEGORY,
		PAYMENT_SUBCATEGORY,
		FEE_SCHEDULE_DESC,
		SERVICE_CODE_TYPE,
		PROCEDURE_CODE_ENTRY_DATE,
		PROCEDURE_CODE_EFFECTIVE_DATE,
		PROCEDURE_CODE_END_DATE,
		PROCEDURE_SERVICE_TYPE_DESC,
		GPI_4_CLASS_CODE,
		GPI_4_CLASS_DESC,
		CURRENT_RECORD_IND,
		RECORD_EFFECTIVE_DATE,
		RECORD_END_DATE
FROM  FILTER_NDC

UNION ALL
			   SELECT 
		UNIQUE_ID_KEY,
		HEALTHCARE_SERVICE_HKEY,
		CODE,
		CODE_DESC,
		PAYMENT_CATEGORY,
		PAYMENT_SUBCATEGORY,
		FEE_SCHEDULE_DESC,
		SERVICE_CODE_TYPE,
		PROCEDURE_CODE_ENTRY_DATE,
		PROCEDURE_CODE_EFFECTIVE_DATE,
		PROCEDURE_CODE_END_DATE,
		PROCEDURE_SERVICE_TYPE_DESC,
		GPI_4_CLASS_CODE,
		GPI_4_CLASS_DESC,
		CURRENT_RECORD_IND,
		RECORD_EFFECTIVE_DATE,
		RECORD_END_DATE 
FROM  FILTER_CPT

UNION ALL
			   SELECT 
		UNIQUE_ID_KEY,
		HEALTHCARE_SERVICE_HKEY,
		CODE,
		CODE_DESC,
		PAYMENT_CATEGORY,
		PAYMENT_SUBCATEGORY,
		FEE_SCHEDULE_DESC,
		SERVICE_CODE_TYPE,
		PROCEDURE_CODE_ENTRY_DATE,
		PROCEDURE_CODE_EFFECTIVE_DATE,
		PROCEDURE_CODE_END_DATE,
		PROCEDURE_SERVICE_TYPE_DESC,
		GPI_4_CLASS_CODE,
		GPI_4_CLASS_DESC,
		CURRENT_RECORD_IND,
		RECORD_EFFECTIVE_DATE,
		RECORD_END_DATE 
FROM  FILTER_RC),

----ETL LAYER----

ETL1 as (
 SELECT *,
 current_timestamp() as LOAD_DATETIME,
 try_to_timestamp('invalid') as UPDATE_DATETIME,
 'CAM' as PRIMARY_SOURCE_SYSTEM
  FROM JOIN_NDC
  )

select * from ETL1
      );
    