

---- SRC LAYER ----
WITH
SRC_FLF as ( SELECT *     from     STAGING.DSV_MEDICAL_INVOICE_HEADER_EDIT_EOB ),
SRC_EDIT as ( SELECT *     from     EDW_STAGING_DIM.DIM_EDIT ),
SRC_EOB as ( SELECT *     from     EDW_STAGING_DIM.DIM_EOB ),
//SRC_FLF as ( SELECT *     from     DSV_MEDICAL_INVOICE_HEADER_EDIT_EOB) ,
//SRC_EDIT as ( SELECT *     from    DIMENSIONS.DIM_EDIT) ,
//SRC_EOB as ( SELECT *     from     DIMENSIONS.DIM_EOB) ,

---- LOGIC LAYER ----

LOGIC_FLF as ( SELECT 
		  MEDICAL_INVOICE_NUMBER                             AS                             MEDICAL_INVOICE_NUMBER 
		, CASE WHEN EDIT_ENTRY_DATE is NULL then -1
               WHEN replace(cast(EDIT_ENTRY_DATE as varchar),'-','')::INTEGER < 19010101 then -2
               WHEN replace(cast(EDIT_ENTRY_DATE as varchar),'-','')::INTEGER > 20991231 then -3
               ELSE replace(EDIT_ENTRY_DATE::varchar,'-','')::INTEGER 
               END                                           AS                                    EDIT_ENTRY_DATE 
		, CASE WHEN EOB_ENTRY_DATE is NULL then -1
               WHEN replace(cast(EOB_ENTRY_DATE as varchar),'-','')::INTEGER < 19010101 then -2
               WHEN replace(cast(EOB_ENTRY_DATE as varchar),'-','')::INTEGER > 20991231 then -3
               ELSE replace(EOB_ENTRY_DATE::varchar,'-','')::INTEGER
               END                                           AS                                     EOB_ENTRY_DATE 
		, RECEIPT_DATE                                       AS                                       RECEIPT_DATE 
		, HDR_EDIT_SOURCE                                    AS                                    HDR_EDIT_SOURCE 
		, HDR_EDIT_PHASE_APPLIED                             AS                             HDR_EDIT_PHASE_APPLIED 
		, HDR_EDIT_DISPOSITION                               AS                               HDR_EDIT_DISPOSITION 
		, EDIT_CODE                                          AS                                          EDIT_CODE 
		, EOB_CODE                                           AS                                           EOB_CODE 
		, HDR_EOB_SOURCE                                     AS                                     HDR_EOB_SOURCE 
		, HDR_EOB_PHASE_APPLIED                              AS                              HDR_EOB_PHASE_APPLIED 
		, EOB_DISPOSITION                                    AS                                    EOB_DISPOSITION
		, md5(cast(
    
    coalesce(cast(HDR_EDIT_SOURCE as 
    varchar
), '') || '-' || coalesce(cast(HDR_EDIT_PHASE_APPLIED as 
    varchar
), '') || '-' || coalesce(cast(HDR_EDIT_DISPOSITION as 
    varchar
), '')

 as 
    varchar
)) 
															 AS                                    EDIT_ENTRY_HKEY 
		, md5(cast(
    
    coalesce(cast(HDR_EOB_SOURCE as 
    varchar
), '') || '-' || coalesce(cast(HDR_EOB_PHASE_APPLIED as 
    varchar
), '') || '-' || coalesce(cast(EOB_DISPOSITION as 
    varchar
), '')

 as 
    varchar
)) 
															 AS 									EOB_ENTRY_HKEY
		, EDIT_ENTRY_DATE                                    AS                               JOIN_EDIT_ENTRY_DATE 
		, EOB_ENTRY_DATE                                     AS                                JOIN_EOB_ENTRY_DATE 	
		from SRC_FLF
            ),
LOGIC_EDIT as ( SELECT 
		  EDIT_HKEY                                          AS                                          EDIT_HKEY 
		, EDIT_CODE                                          AS                                          EDIT_CODE 
		, RECORD_EFFECTIVE_DATE	                             AS                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    AS                                    RECORD_END_DATE
		, CURRENT_RECORD_IND                                 AS                                 CURRENT_RECORD_IND 
		from SRC_EDIT
            ),
LOGIC_EOB as ( SELECT 
		  EOB_HKEY                                           AS                                           EOB_HKEY 
		, EOB_CODE                                           AS                                           EOB_CODE 
		, RECORD_EFFECTIVE_DATE	                             AS                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    AS                                    RECORD_END_DATE
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
		from SRC_EOB
            )

---- RENAME LAYER ----
,

RENAME_FLF as ( SELECT 
		  MEDICAL_INVOICE_NUMBER                             as                             MEDICAL_INVOICE_NUMBER
		, EDIT_ENTRY_DATE                                    as                                EDIT_ENTRY_DATE_KEY
		, EOB_ENTRY_DATE                                     as                                 EOB_ENTRY_DATE_KEY
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE
		, HDR_EDIT_SOURCE                                    as                                    HDR_EDIT_SOURCE
		, HDR_EDIT_PHASE_APPLIED                             as                             HDR_EDIT_PHASE_APPLIED
		, HDR_EDIT_DISPOSITION                               as                               HDR_EDIT_DISPOSITION
		, EDIT_CODE                                          as                                          EDIT_CODE
		, EOB_CODE                                           as                                           EOB_CODE
		, HDR_EOB_SOURCE                                     as                                     HDR_EOB_SOURCE
		, HDR_EOB_PHASE_APPLIED                              as                              HDR_EOB_PHASE_APPLIED
		, EOB_DISPOSITION                                    as                                    EOB_DISPOSITION
		, EOB_ENTRY_DATE                                     as                                         ENTRY_DATE
		, EDIT_ENTRY_HKEY                                    as                                    EDIT_ENTRY_HKEY 
		, EOB_ENTRY_HKEY                                     as                                     EOB_ENTRY_HKEY
		, JOIN_EDIT_ENTRY_DATE                               as                               JOIN_EDIT_ENTRY_DATE 
		, JOIN_EOB_ENTRY_DATE                                as                                JOIN_EOB_ENTRY_DATE 		
				FROM     LOGIC_FLF   ), 
RENAME_EDIT as ( SELECT 
		  EDIT_HKEY                                          as                                          EDIT_HKEY
		, EDIT_CODE                                          as                                      DIM_EDIT_CODE
		, RECORD_EFFECTIVE_DATE                              as                         EDIT_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                               EDIT_RECORD_END_DATE
		, CURRENT_RECORD_IND                                 as                            EDIT_CURRENT_RECORD_IND
				FROM     LOGIC_EDIT   ), 
RENAME_EOB as ( SELECT 
		  EOB_HKEY                                           as                                           EOB_HKEY
		, EOB_CODE                                           as                                       DIM_EOB_CODE
		, RECORD_EFFECTIVE_DATE                              as                          EOB_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                EOB_RECORD_END_DATE
		, CURRENT_RECORD_IND                                 as                             EOB_CURRENT_RECORD_IND         
				FROM     LOGIC_EOB   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_FLF                            as ( SELECT * from    RENAME_FLF   ),
FILTER_EDIT                           as ( SELECT * from    RENAME_EDIT   ),
FILTER_EOB                            as ( SELECT * from    RENAME_EOB   ),

---- JOIN LAYER ----
INVOICE_HDR as ( SELECT *
				FROM  FILTER_FLF
						LEFT JOIN FILTER_EDIT ON  coalesce( FILTER_FLF.EDIT_CODE, 'N/E') =  FILTER_EDIT.DIM_EDIT_CODE AND JOIN_EDIT_ENTRY_DATE BETWEEN FILTER_EDIT.EDIT_RECORD_EFFECTIVE_DATE AND coalesce( FILTER_EDIT.EDIT_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_EOB ON  coalesce( FILTER_FLF.EOB_CODE, 'N/E') =  FILTER_EOB.DIM_EOB_CODE 
								AND coalesce(JOIN_EOB_ENTRY_DATE, RECEIPT_DATE)  BETWEEN FILTER_EOB.EOB_RECORD_EFFECTIVE_DATE AND coalesce( FILTER_EOB.EOB_RECORD_END_DATE, '2099-12-31')  )

-- ETL join layer to handle that are outside of date range MD5('-2222') 								                                
, ETL_FLF AS (SELECT FILTER_INVOICE_HDR.*
            	, FILTER_EDIT.DIM_EDIT_CODE AS FILTER_EDIT_EDIT_CODE
				, FILTER_EOB.DIM_EOB_CODE AS FILTER_EOB_EOB_CODE
              FROM INVOICE_HDR FILTER_INVOICE_HDR
            	LEFT JOIN FILTER_EDIT ON   coalesce( FILTER_INVOICE_HDR.EDIT_CODE,  'Z') =  FILTER_EDIT.DIM_EDIT_CODE AND FILTER_EDIT.EDIT_CURRENT_RECORD_IND = 'Y'
					LEFT JOIN FILTER_EOB ON   coalesce( FILTER_INVOICE_HDR.EOB_CODE,  'Z') =  FILTER_EOB.DIM_EOB_CODE AND FILTER_EOB.EOB_CURRENT_RECORD_IND = 'Y' 
				) --,
                
SELECT
		  MEDICAL_INVOICE_NUMBER
		, (CASE WHEN EDIT_HKEY IS NOT NULL THEN EDIT_HKEY
                WHEN FILTER_EDIT_EDIT_CODE IS NOT NULL THEN MD5('-2222')
				ELSE md5('88888') END)::VARCHAR(32) AS EDIT_HKEY
        , (CASE WHEN EOB_HKEY IS NOT NULL THEN EOB_HKEY
                WHEN FILTER_EOB_EOB_CODE IS NOT NULL THEN MD5('-2222')
				ELSE md5('88888') END)::VARCHAR(32) AS EOB_HKEY 
        , EDIT_ENTRY_HKEY
		, EOB_ENTRY_HKEY
		, EDIT_ENTRY_DATE_KEY
		, EOB_ENTRY_DATE_KEY
        , CURRENT_TIMESTAMP AS LOAD_DATETIME
		, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
		, 'CAM' AS PRIMARY_SOURCE_SYSTEM
from ETL_FLF