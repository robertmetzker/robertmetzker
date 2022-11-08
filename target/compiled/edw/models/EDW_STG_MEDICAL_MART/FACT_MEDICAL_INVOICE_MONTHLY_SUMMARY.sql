

---- SRC LAYER ----
WITH
SRC_CS as ( SELECT *     from     EDW_STG_MEDICAL_MART.FACT_CONSOLIDATED_MEDICAL_BILLING ),
SRC_CTS as ( SELECT *     from     EDW_STAGING_DIM.DIM_CLAIM_TYPE_STATUS ),
//SRC_CS as ( SELECT *     from     FACT_CONSOLIDATED_MEDICAL_BILLING) ,
//SRC_CTS as ( SELECT *     from     DIM_CLAIM_TYPE_STATUS) ,

---- LOGIC LAYER ----

LOGIC_CS as ( SELECT 
		  INVOICE_PROFILE_HKEY                               as                               INVOICE_PROFILE_HKEY 
		, LEFT(BWC_BILL_RECEIPT_DATE_KEY,6):: INTEGER        as                          BWC_BILL_RECEIPT_DATE_KEY 
		, LEFT(BWC_ADJUDICATION_DATE_KEY,6):: INTEGER        as                          BWC_ADJUDICATION_DATE_KEY 
		, LEFT(PAID_DATE_KEY,6):: INTEGER                    as                                      PAID_DATE_KEY 
		, INVOICE_LINE_ITEM_STATUS_HKEY                      as                      INVOICE_LINE_ITEM_STATUS_HKEY 
		, INVOICE_HEADER_CURRENT_STATUS_HKEY                 as                 INVOICE_HEADER_CURRENT_STATUS_HKEY 
		, LINE_UNITS_OF_BILLED_SERVICE_COUNT                 as                 LINE_UNITS_OF_BILLED_SERVICE_COUNT 
		, LINE_UNITS_OF_PAID_SERVICE_COUNT                   as                   LINE_UNITS_OF_PAID_SERVICE_COUNT 
		, LINE_PROVIDER_BILLED_AMOUNT                        as                        LINE_PROVIDER_BILLED_AMOUNT 
		, LINE_NETWORK_ALLOWED_AMOUNT                        as                        LINE_NETWORK_ALLOWED_AMOUNT 
		, LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT                as                LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT 
		, LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT            as            LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT 
		, LINE_BWC_APPROVED_AMOUNT                           as                           LINE_BWC_APPROVED_AMOUNT 
		, LINE_INTEREST_AMOUNT                               as                               LINE_INTEREST_AMOUNT 
		, LINE_REIMBURSED_AMOUNT                             as                             LINE_REIMBURSED_AMOUNT 
		--, BUSINESS_BATCH_NUMBER                              as                              BUSINESS_BATCH_NUMBER 
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER 
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY 
		, MEDICAL_INVOICE_NUMBER                             as                             MEDICAL_INVOICE_NUMBER 
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER               as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER 
        , CONCAT(MEDICAL_INVOICE_NUMBER,MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER) as                INVOICE_LINE_COUNT
  		from SRC_CS
            ),
LOGIC_CTS as ( SELECT 
		  CASE WHEN CLAIM_TYPE_CODE IS NULL THEN md5('99999')
		   ELSE md5(cast(
    
    coalesce(cast(CLAIM_TYPE_CODE as 
    varchar
), '')

 as 
    varchar
))                                  
													END		 as                					   CLAIM_TYPE_HKEY 
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY 
		, PRIMARY_SOURCE_SYSTEM                              as                              PRIMARY_SOURCE_SYSTEM 
		from SRC_CTS
            )

---- RENAME LAYER ----
,

RENAME_CS as ( SELECT 
		  INVOICE_PROFILE_HKEY                               as                               INVOICE_PROFILE_HKEY
		, BWC_BILL_RECEIPT_DATE_KEY                          as                        BWC_BILL_RECEIPT_MONTH_SKEY
		, BWC_ADJUDICATION_DATE_KEY                          as                        BWC_ADJUDICATION_MONTH_SKEY
		, PAID_DATE_KEY                                      as                                    PAID_MONTH_SKEY
		, INVOICE_LINE_ITEM_STATUS_HKEY                      as                      INVOICE_LINE_ITEM_STATUS_HKEY
		, INVOICE_HEADER_CURRENT_STATUS_HKEY                 as                 INVOICE_HEADER_CURRENT_STATUS_HKEY
		, LINE_UNITS_OF_BILLED_SERVICE_COUNT                 as                 LINE_UNITS_OF_BILLED_SERVICE_COUNT
		, LINE_UNITS_OF_PAID_SERVICE_COUNT                   as                   LINE_UNITS_OF_PAID_SERVICE_COUNT
		, LINE_PROVIDER_BILLED_AMOUNT                        as                        LINE_PROVIDER_BILLED_AMOUNT
		, LINE_NETWORK_ALLOWED_AMOUNT                        as                        LINE_NETWORK_ALLOWED_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT                as                LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT            as            LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
		, LINE_BWC_APPROVED_AMOUNT                           as                           LINE_BWC_APPROVED_AMOUNT
		, LINE_INTEREST_AMOUNT                               as                               LINE_INTEREST_AMOUNT
		, LINE_REIMBURSED_AMOUNT                             as                             LINE_REIMBURSED_AMOUNT
	--	, BUSINESS_BATCH_NUMBER                              as                              BUSINESS_BATCH_NUMBER
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY
		, MEDICAL_INVOICE_NUMBER                             as                             MEDICAL_INVOICE_NUMBER
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER               as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER 
		, INVOICE_LINE_COUNT                                 as                                 INVOICE_LINE_COUNT
				FROM     LOGIC_CS   ), 
RENAME_CTS as ( SELECT 
		  CLAIM_TYPE_HKEY                                    as                                    CLAIM_TYPE_HKEY
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY
		, PRIMARY_SOURCE_SYSTEM                              as                              PRIMARY_SOURCE_SYSTEM 

				FROM     LOGIC_CTS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CS                             as ( SELECT * from    RENAME_CS   ),
FILTER_CTS                             as ( SELECT * from    RENAME_CTS WHERE PRIMARY_SOURCE_SYSTEM <> 'MANUAL ENTRY'  ),

---- JOIN LAYER ----

CS as ( SELECT * 
				FROM  FILTER_CS
				LEFT JOIN FILTER_CTS ON  FILTER_CS.CLAIM_TYPE_STATUS_HKEY =  FILTER_CTS.CLAIM_TYPE_STATUS_HKEY  ),
ETL_FACT AS (                
SELECT 
		  INVOICE_PROFILE_HKEY
		, BWC_BILL_RECEIPT_MONTH_SKEY
		, BWC_ADJUDICATION_MONTH_SKEY
		, PAID_MONTH_SKEY 
		, INVOICE_LINE_ITEM_STATUS_HKEY
		, INVOICE_HEADER_CURRENT_STATUS_HKEY
		, coalesce(CLAIM_TYPE_HKEY,md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))) AS CLAIM_TYPE_HKEY
        , COUNT(DISTINCT MEDICAL_INVOICE_NUMBER) AS INVOICE_COUNT
        , COUNT(DISTINCT INVOICE_LINE_COUNT) AS INVOICE_LINE_COUNT
		, COUNT(DISTINCT CLAIM_NUMBER) AS CLAIM_COUNT
        , COUNT(LINE_UNITS_OF_BILLED_SERVICE_COUNT) AS LINE_UNITS_OF_BILLED_SERVICE_COUNT
		, COUNT(LINE_UNITS_OF_PAID_SERVICE_COUNT) AS LINE_UNITS_OF_PAID_SERVICE_COUNT
		, SUM(LINE_PROVIDER_BILLED_AMOUNT) AS LINE_PROVIDER_BILLED_AMOUNT
		, SUM(LINE_NETWORK_ALLOWED_AMOUNT) AS LINE_NETWORK_ALLOWED_AMOUNT
		, SUM(LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT) AS LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
		, SUM(LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT) AS LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
		, SUM(LINE_BWC_APPROVED_AMOUNT) AS LINE_BWC_APPROVED_AMOUNT
		, SUM(LINE_INTEREST_AMOUNT) AS LINE_INTEREST_AMOUNT
		, SUM(LINE_REIMBURSED_AMOUNT) AS LINE_REIMBURSED_AMOUNT
from CS
GROUP BY INVOICE_PROFILE_HKEY,  BWC_BILL_RECEIPT_MONTH_SKEY, BWC_ADJUDICATION_MONTH_SKEY, PAID_MONTH_SKEY,
 INVOICE_LINE_ITEM_STATUS_HKEY, INVOICE_HEADER_CURRENT_STATUS_HKEY,  coalesce(CLAIM_TYPE_HKEY,md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
)))
)

SELECT 
		  INVOICE_PROFILE_HKEY
		, BWC_BILL_RECEIPT_MONTH_SKEY
		, BWC_ADJUDICATION_MONTH_SKEY
		, PAID_MONTH_SKEY
		, INVOICE_LINE_ITEM_STATUS_HKEY
		, INVOICE_HEADER_CURRENT_STATUS_HKEY
		, CLAIM_TYPE_HKEY
        , INVOICE_COUNT
        , INVOICE_LINE_COUNT
		, CLAIM_COUNT
		, LINE_UNITS_OF_BILLED_SERVICE_COUNT
		, LINE_UNITS_OF_PAID_SERVICE_COUNT
		, LINE_PROVIDER_BILLED_AMOUNT
		, LINE_NETWORK_ALLOWED_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
		, LINE_BWC_APPROVED_AMOUNT
		, LINE_INTEREST_AMOUNT
		, LINE_REIMBURSED_AMOUNT
        , CURRENT_TIMESTAMP AS LOAD_DATETIME
		, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME 
        , 'CAM' AS PRIMARY_SOURCE_SYSTEM 
from ETL_FACT