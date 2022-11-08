---- SRC LAYER ----
WITH
SRC_DPC as ( SELECT *     from     STAGING.STG_DETAIL_PAYMENT_CODING ),
//SRC_DPC as ( SELECT *     from     STG_DETAIL_PAYMENT_CODING) ,

---- LOGIC LAYER ----

LOGIC_DPC as ( SELECT 
		  TRIM( CHECK_NO )                                   AS                                           CHECK_NO 
		, TRIM( WRNT_NO )                                    AS                                            WRNT_NO 
		, TRIM( TCN_NO )                                     AS                                             TCN_NO 
		, TRIM( PAYEE_NAME )                                 AS                                         PAYEE_NAME 
		, WRNT_DATE                                          AS                                          WRNT_DATE 
		, TRIM( PRVDR_NO )                                   AS                                           PRVDR_NO 
		, TRIM( CLAIM_NO )                                   AS                                           CLAIM_NO 
		, TRIM( PLCY_NO )                                    AS                                            PLCY_NO 
		, BSNS_SQNC_NO                                       AS                                       BSNS_SQNC_NO 
		, TRIM( EIN_NO )                                     AS                                             EIN_NO 
		, TRIM( REMIT_ADVC_NO )                              AS                                      REMIT_ADVC_NO 
		, TRIM( WRNT_AMT )                                   AS                                           WRNT_AMT 
		, PYMNT_CODE_AMT                                     AS                                     PYMNT_CODE_AMT 
		, TRIM( ACNTB_CODE )                                 AS                                         ACNTB_CODE 
		, TRIM( PYMNT_FUND_TYPE )                            AS                                    PYMNT_FUND_TYPE 
		, TRIM( CVRG_TYPE )                                  AS                                          CVRG_TYPE 
		, TRIM( BILL_TYPE_F2 )                               AS                                       BILL_TYPE_F2 
		, TRIM( BILL_TYPE_L3 )                               AS                                       BILL_TYPE_L3 
		, TRIM( ACDNT_TYPE )                                 AS                                         ACDNT_TYPE 
		, TRIM( STS_CODE )                                   AS                                           STS_CODE 
		from SRC_DPC
            )

---- RENAME LAYER ----
,

RENAME_DPC as ( SELECT 
		 CHECK_NO                                           as                                           CHECK_NO
		, WRNT_NO                                            as                                            WRNT_NO
		, TCN_NO                                             as                                             TCN_NO
		, PAYEE_NAME                                         as                                         PAYEE_NAME
		, WRNT_DATE                                          as                                          WRNT_DATE
		, PRVDR_NO                                           as                                           PRVDR_NO
		, CLAIM_NO                                           as                                           CLAIM_NO
		, PLCY_NO                                            as                                            PLCY_NO
		, BSNS_SQNC_NO                                       as                                       BSNS_SQNC_NO
		, EIN_NO                                             as                                             EIN_NO
		, REMIT_ADVC_NO                                      as                                      REMIT_ADVC_NO
		, WRNT_AMT                                           as                                           WRNT_AMT
		, PYMNT_CODE_AMT                                     as                                     PYMNT_CODE_AMT
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
				FROM  FILTER_DPC )
 SELECT * FROM  JOIN_DPC