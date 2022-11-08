 

---- SRC LAYER ----
WITH
SRC_DPC as ( SELECT *     from     STAGING.DSV_DETAIL_PAYMENT_CODING ),
//SRC_DPC as ( SELECT *     from     DSV_DETAIL_PAYMENT_CODING) ,

---- LOGIC LAYER ----

LOGIC_DPC as ( SELECT 
		  CHECK_NUMBER                                       AS                                       CHECK_NUMBER 
		, WARRANT_NUMBER                                     AS                                     WARRANT_NUMBER 
		, TCN_NUMBER                                         AS                                         TCN_NUMBER 
		, WARRANT_DATE                                       AS                                       WARRANT_DATE 
		, md5(cast(
    
    coalesce(cast(ACNTB_CODE as 
    varchar
), '') || '-' || coalesce(cast(PYMNT_FUND_TYPE as 
    varchar
), '') || '-' || coalesce(cast(CVRG_TYPE as 
    varchar
), '') || '-' || coalesce(cast(BILL_TYPE_F2 as 
    varchar
), '') || '-' || coalesce(cast(BILL_TYPE_L3 as 
    varchar
), '') || '-' || coalesce(cast(ACDNT_TYPE as 
    varchar
), '') || '-' || coalesce(cast(STS_CODE as 
    varchar
), '')

 as 
    varchar
)) AS   PAYMENT_CODER_HKEY 
	    , md5(cast(
    
    coalesce(cast(PAYEE_FULL_NAME as 
    varchar
), '')

 as 
    varchar
))AS                                         PAYEE_HKEY 
	    , CLAIM_NUMBER                                       AS                                       CLAIM_NUMBER
	    , md5(cast(
    
    coalesce(cast(CLAIM_PAYMENT_CATEGORY_DESC as 
    varchar
), '')

 as 
    varchar
)) 
                                                             AS                        CLAIM_PAYMENT_CATEGORY_HKEY
		, POLICY_NUMBER                                      AS                                      POLICY_NUMBER 
		, REMIT_ADVANCE_NUMBER                               AS                               REMIT_ADVANCE_NUMBER 
		, PAYMENT_CODE_AMOUNT                                AS                                PAYMENT_CODE_AMOUNT 
		, ACNTB_CODE                                         AS                                         ACNTB_CODE 
		, PYMNT_FUND_TYPE                                    AS                                    PYMNT_FUND_TYPE 
		, CVRG_TYPE                                          AS                                          CVRG_TYPE 
		, BILL_TYPE_F2                                       AS                                       BILL_TYPE_F2 
		, BILL_TYPE_L3                                       AS                                       BILL_TYPE_L3 
		, ACDNT_TYPE                                         AS                                         ACDNT_TYPE 
		, STS_CODE                                           AS                                           STS_CODE 
				from SRC_DPC
            )

---- RENAME LAYER ----
,

RENAME_DPC as ( SELECT 
		  CHECK_NUMBER                                       as                                       CHECK_NUMBER
		, WARRANT_NUMBER                                     as                                     WARRANT_NUMBER
		, TCN_NUMBER                                         as                                         TCN_NUMBER
		, WARRANT_DATE                                       as                                  WARRANT_DATE_HKEY
		, PAYMENT_CODER_HKEY                                 as                                 PAYMENT_CODER_HKEY
		, PAYEE_HKEY                                         as                                         PAYEE_HKEY
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		, CLAIM_PAYMENT_CATEGORY_HKEY                        as                        CLAIM_PAYMENT_CATEGORY_HKEY
		, POLICY_NUMBER                                      as                                      POLICY_NUMBER
		, REMIT_ADVANCE_NUMBER                               as                               REMIT_ADVANCE_NUMBER
		, PAYMENT_CODE_AMOUNT                                as                                PAYMENT_CODE_AMOUNT
		, ACNTB_CODE                                         as                                         ACNTB_CODE
		, PYMNT_FUND_TYPE                                    as                                    PYMNT_FUND_TYPE
		, CVRG_TYPE                                          as                                          CVRG_TYPE
		, BILL_TYPE_F2                                       as                                       BILL_TYPE_F2
		, BILL_TYPE_L3                                       as                                       BILL_TYPE_L3
		, ACDNT_TYPE                                         as                                         ACDNT_TYPE
		, STS_CODE                                           as                                           STS_CODE
						FROM     LOGIC_DPC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DPC                            as ( SELECT * from    RENAME_DPC   ),

---- JOIN LAYER ----

 JOIN_DPC  as  ( SELECT * 
				FROM  FILTER_DPC ),
---- ETL LAYER ----
ETL AS ( SELECT 
  CHECK_NUMBER
, WARRANT_NUMBER
, TCN_NUMBER
, CASE WHEN WARRANT_DATE_HKEY  IS NULL THEN -1
       WHEN replace(cast(WARRANT_DATE_HKEY ::DATE as varchar),'-','')::INTEGER  < 19010101 THEN -2
       WHEN replace(cast(WARRANT_DATE_HKEY ::DATE as varchar),'-','')::INTEGER  > 20991231 THEN -3
       ELSE replace(cast(WARRANT_DATE_HKEY 	 ::DATE as varchar),'-','')::INTEGER 
        END AS WARRANT_DATE_HKEY 
, PAYMENT_CODER_HKEY
, coalesce(PAYEE_HKEY,md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))) AS PAYEE_HKEY
, CLAIM_NUMBER
, coalesce(CLAIM_PAYMENT_CATEGORY_HKEY,md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))) AS CLAIM_PAYMENT_CATEGORY_HKEY
, coalesce(POLICY_NUMBER,md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))) AS POLICY_NUMBER
, REMIT_ADVANCE_NUMBER::INTEGER
, PAYMENT_CODE_AMOUNT
, CURRENT_TIMESTAMP AS LOAD_DATETIME
, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
, 'RNP' AS PRIMARY_SOURCE_SYSTEM
 from  JOIN_DPC)

 SELECT * FROM ETL