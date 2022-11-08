

      create or replace  table DEV_EDW.EDW_STG_MEDICAL_MART.FLF_PROVIDER_CERTIFICATION_LOG  as
      (

---- SRC LAYER ----
WITH
SRC_PROV as ( SELECT *     from     EDW_STAGING_DIM.DIM_PROVIDER ),
SRC_PCS as ( SELECT *     from     EDW_STAGING_DIM.DIM_PROVIDER_CERTIFICATION_STATUS ),
SRC_PCSL as ( SELECT *     from     STAGING.DSV_PROVIDER_CERTIFICATION_STATUS_LOG ),
--SRC_PROV as ( SELECT *     from     DIMENSIONS.DIM_PROVIDER) ,
--SRC_PCS as ( SELECT *     from     DIMENSIONS.DIM_PROVIDER_CERTIFICATION_STATUS) ,
--SRC_PCSL as ( SELECT *     from     DSV_PROVIDER_CERTIFICATION_STATUS_LOG) ,

---- LOGIC LAYER ----

LOGIC_PROV as ( SELECT 
		  PROVIDER_HKEY                                      as                                      PROVIDER_HKEY 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE
        , PROVIDER_PEACH_NUMBER                              as                              PROVIDER_PEACH_NUMBER
		from SRC_PROV
            ),
LOGIC_PCS as ( SELECT 
		  BWC_CERTIFICATION_STATUS_HKEY                      as                      BWC_CERTIFICATION_STATUS_HKEY 
		, BWC_CERTIFICATION_STATUS_CODE                      as                      BWC_CERTIFICATION_STATUS_CODE
		, BWC_CERTIFICATION_STATUS_REASON_CODE               as                      BWC_CERTIFICATION_STATUS_REASON_CODE
		from SRC_PCS
            ),
LOGIC_PCSL as ( SELECT 
		  DERIVED_EFFECTIVE_DATE                             as                             DERIVED_EFFECTIVE_DATE 
		, DERIVED_ENDING_DATE                                as                                DERIVED_ENDING_DATE 
		, CERTIFICATION_EFFECTIVE_DATE                       as                       CERTIFICATION_EFFECTIVE_DATE 
		, CERTIFICATION_END_DATE                             as                             CERTIFICATION_END_DATE 
		, PROVIDER_COMBINED_IND                              as                              PROVIDER_COMBINED_IND 
		, PROVIDER_PEACH_NUMBER                              as                              PROVIDER_PEACH_NUMBER 
		, BWC_CERTIFICATION_STATUS_CODE                      as                      BWC_CERTIFICATION_STATUS_CODE 
		, BWC_CERTIFICATION_STATUS_DESC                      as                      BWC_CERTIFICATION_STATUS_DESC 
		, BWC_CERTIFICATION_STATUS_REASON_CODE               as               BWC_CERTIFICATION_STATUS_REASON_CODE 
		, BWC_CERTIFICATION_STATUS_REASON_DESC               as               BWC_CERTIFICATION_STATUS_REASON_DESC 
		from SRC_PCSL
            )

---- RENAME LAYER ----
,

RENAME_PROV as ( SELECT 
		  PROVIDER_HKEY                                      as                                      PROVIDER_HKEY
		, RECORD_EFFECTIVE_DATE                              as                         PROV_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                               PROV_RECORD_END_DATE
        , PROVIDER_PEACH_NUMBER                              as                              PROVIDER_PEACH_NUMBER
				FROM     LOGIC_PROV   ), 
RENAME_PCS as ( SELECT 
		  BWC_CERTIFICATION_STATUS_HKEY                      as                      BWC_CERTIFICATION_STATUS_HKEY 
		, BWC_CERTIFICATION_STATUS_CODE                      as                      BWC_CERTIFICATION_STATUS_CODE
		, BWC_CERTIFICATION_STATUS_REASON_CODE               as                      BWC_CERTIFICATION_STATUS_REASON_CODE	
               FROM     LOGIC_PCS   ), 
RENAME_PCSL as ( SELECT 
		  DERIVED_EFFECTIVE_DATE                             as                         DERIVED_EFFECTIVE_DATE_KEY
		, DERIVED_ENDING_DATE                                as                            DERIVED_ENDING_DATE_KEY
		, CERTIFICATION_EFFECTIVE_DATE                       as                   CERTIFICATION_EFFECTIVE_DATE_KEY
		, CERTIFICATION_END_DATE                             as                         CERTIFICATION_END_DATE_KEY
		, PROVIDER_COMBINED_IND                              as                              PROVIDER_COMBINED_IND
		, PROVIDER_PEACH_NUMBER                              as                              PROVIDER_PEACH_NUMBER
		, BWC_CERTIFICATION_STATUS_CODE                      as                      BWC_CERTIFICATION_STATUS_CODE
		, BWC_CERTIFICATION_STATUS_DESC                      as                      BWC_CERTIFICATION_STATUS_DESC
		, BWC_CERTIFICATION_STATUS_REASON_CODE               as               BWC_CERTIFICATION_STATUS_REASON_CODE
		, BWC_CERTIFICATION_STATUS_REASON_DESC               as               BWC_CERTIFICATION_STATUS_REASON_DESC 
				FROM     LOGIC_PCSL   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PCSL                           as ( SELECT * from    RENAME_PCSL   ),
FILTER_PROV                           as ( SELECT * from    RENAME_PROV   ),
FILTER_PCS                            as ( SELECT * from    RENAME_PCS   ),

---- JOIN LAYER ----

PCSL as ( SELECT * 
				FROM  FILTER_PCSL
				LEFT JOIN FILTER_PROV ON  FILTER_PCSL.PROVIDER_PEACH_NUMBER =  FILTER_PROV.PROVIDER_PEACH_NUMBER 
				AND FILTER_PCSL.DERIVED_EFFECTIVE_DATE_KEY BETWEEN FILTER_PROV.PROV_RECORD_EFFECTIVE_DATE AND COALESCE(FILTER_PROV.PROV_RECORD_END_DATE, '2999-12-31')
				LEFT JOIN FILTER_PCS ON  FILTER_PCSL.BWC_CERTIFICATION_STATUS_CODE =  FILTER_PCS.BWC_CERTIFICATION_STATUS_CODE
				AND FILTER_PCSL.BWC_CERTIFICATION_STATUS_REASON_CODE = FILTER_PCS.BWC_CERTIFICATION_STATUS_REASON_CODE ),

-----ETL LAYER------
ETL AS (

SELECT  distinct
coalesce( PROVIDER_HKEY, md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
)) ) AS PROVIDER_HKEY
  ,coalesce( BWC_CERTIFICATION_STATUS_HKEY, md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
)) ) AS BWC_CERTIFICATION_STATUS_HKEY
  ,case when DERIVED_EFFECTIVE_DATE_KEY  is null then -1
    when replace(cast(DERIVED_EFFECTIVE_DATE_KEY ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(DERIVED_EFFECTIVE_DATE_KEY ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(DERIVED_EFFECTIVE_DATE_KEY     ::DATE as varchar),'-','')::INTEGER 
    END AS DERIVED_EFFECTIVE_DATE_KEY 
,case when DERIVED_ENDING_DATE_KEY  is null then -1
    when replace(cast(DERIVED_ENDING_DATE_KEY ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(DERIVED_ENDING_DATE_KEY ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(DERIVED_ENDING_DATE_KEY    ::DATE as varchar),'-','')::INTEGER 
    END AS DERIVED_ENDING_DATE_KEY
,case when CERTIFICATION_EFFECTIVE_DATE_KEY  is null then -1
    when replace(cast(CERTIFICATION_EFFECTIVE_DATE_KEY ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(CERTIFICATION_EFFECTIVE_DATE_KEY ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(CERTIFICATION_EFFECTIVE_DATE_KEY      ::DATE as varchar),'-','')::INTEGER 
    END AS CERTIFICATION_EFFECTIVE_DATE_KEY  	
,case when CERTIFICATION_END_DATE_KEY  is null then -1
    when replace(cast(CERTIFICATION_END_DATE_KEY ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(CERTIFICATION_END_DATE_KEY ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(CERTIFICATION_END_DATE_KEY    ::DATE as varchar),'-','')::INTEGER 
    END AS CERTIFICATION_END_DATE_KEY 	
,CURRENT_TIMESTAMP AS LOAD_DATETIME
,TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME 
,'PEACH' AS PRIMARY_SOURCE_SYSTEM 
from PCSL
)

SELECT *
from ETL
      );
    