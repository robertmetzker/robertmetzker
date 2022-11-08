
  create or replace  view DEV_EDW.STAGING.DSV_PAYMENT_CODER  as (
    

---- SRC LAYER ----
WITH
SRC_PAY as ( SELECT *     from     STAGING.DST_PAYMENT_CODER ),
//SRC_PAY as ( SELECT *     from     DST_PAYMENT_CODER) ,

---- LOGIC LAYER ----

LOGIC_PAY as ( SELECT 
		  UNIQUE_ID_KEY                                      AS                                      UNIQUE_ID_KEY 
		, ACNTB_CODE                                         AS                                         ACNTB_CODE 
		, PYMNT_FUND_TYPE                                    AS                                    PYMNT_FUND_TYPE 
		, CVRG_TYPE                                          AS                                          CVRG_TYPE 
		, BILL_TYPE_F2                                       AS                                       BILL_TYPE_F2 
		, BILL_TYPE_L3                                       AS                                       BILL_TYPE_L3 
		, ACDNT_TYPE                                         AS                                         ACDNT_TYPE 
		, STS_CODE                                           AS                                           STS_CODE 
		, ACNTB_DESC                                         AS                                         ACNTB_DESC 
		, PYMNT_FUND_DESC                                    AS                                    PYMNT_FUND_DESC 
		, CVRG_DESC                                          AS                                          CVRG_DESC 
		, BILL_TYPE_F2_DESC                                  AS                                  BILL_TYPE_F2_DESC 
		, BILL_TYPE_L3_DESC                                  AS                                  BILL_TYPE_L3_DESC 
		, ACDNT_DESC                                         AS                                         ACDNT_DESC 
		, WRQ_STS_CODE_DESC                                  AS                                  WRQ_STS_CODE_DESC 
		from SRC_PAY
            )

---- RENAME LAYER ----
,

RENAME_PAY as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, ACNTB_CODE                                         as                                ACCOUNTABILITY_CODE
		, PYMNT_FUND_TYPE                                    as                             PAYMENT_FUND_TYPE_CODE
		, CVRG_TYPE                                          as                                 COVERAGE_TYPE_CODE
		, BILL_TYPE_F2                                       as                                  BILL_TYPE_F2_CODE
		, BILL_TYPE_L3                                       as                                  BILL_TYPE_L3_CODE
		, ACDNT_TYPE                                         as                                 ACCIDENT_TYPE_CODE
		, STS_CODE                                           as                                PAYMENT_STATUS_CODE
		, ACNTB_DESC                                         as                                ACCOUNTABILITY_DESC
		, PYMNT_FUND_DESC                                    as                                  PAYMENT_FUND_DESC
		, CVRG_DESC                                          as                                      COVERAGE_DESC
		, BILL_TYPE_F2_DESC                                  as                                  BILL_TYPE_F2_DESC
		, BILL_TYPE_L3_DESC                                  as                                  BILL_TYPE_L3_DESC
		, ACDNT_DESC                                         as                                 ACCIDENT_TYPE_DESC
		, WRQ_STS_CODE_DESC                                  as                                  WRQ_STS_CODE_DESC 
				FROM     LOGIC_PAY   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PAY                            as ( SELECT * from    RENAME_PAY   ),

---- JOIN LAYER ----

 JOIN_PAY  as  ( SELECT * 
				FROM  FILTER_PAY )
 SELECT * FROM  JOIN_PAY
  );
