

      create or replace  table DEV_EDW.STAGING.DST_PAYMENT_CODER  as
      (---- SRC LAYER ----
WITH
SRC_PAY as ( SELECT *     from     STAGING.STG_DETAIL_PAYMENT_CODING ),
//SRC_PAY as ( SELECT *     from     STG_DETAIL_PAYMENT_CODING) ,

---- LOGIC LAYER ----

LOGIC_PAY as ( SELECT 
		  md5(cast(
    
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
)) AS                                      UNIQUE_ID_KEY 
		, TRIM( ACNTB_CODE )                                 AS                                         ACNTB_CODE 
		, TRIM( PYMNT_FUND_TYPE )                            AS                                    PYMNT_FUND_TYPE 
		, TRIM( CVRG_TYPE )                                  AS                                          CVRG_TYPE 
		, TRIM( BILL_TYPE_F2 )                               AS                                       BILL_TYPE_F2 
		, TRIM( BILL_TYPE_L3 )                               AS                                       BILL_TYPE_L3 
		, TRIM( ACDNT_TYPE )                                 AS                                         ACDNT_TYPE 
		, TRIM( STS_CODE )                                   AS                                           STS_CODE 
		, TRIM( ACNTB_DESC )                                 AS                                         ACNTB_DESC 
		, TRIM( PYMNT_FUND_DESC )                            AS                                    PYMNT_FUND_DESC 
		, TRIM( CVRG_DESC )                                  AS                                          CVRG_DESC 
		, TRIM( BILL_TYPE_F2_DESC )                          AS                                  BILL_TYPE_F2_DESC 
		, TRIM( BILL_TYPE_L3_DESC )                          AS                                  BILL_TYPE_L3_DESC 
		, TRIM( ACDNT_DESC )                                 AS                                         ACDNT_DESC 
		, TRIM( WRQ_STS_CODE_DESC )                          AS                                  WRQ_STS_CODE_DESC 
		from SRC_PAY
            )

---- RENAME LAYER ----
,

RENAME_PAY as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, ACNTB_CODE                                         as                                         ACNTB_CODE
		, PYMNT_FUND_TYPE                                    as                                    PYMNT_FUND_TYPE
		, CVRG_TYPE                                          as                                          CVRG_TYPE
		, BILL_TYPE_F2                                       as                                       BILL_TYPE_F2
		, BILL_TYPE_L3                                       as                                       BILL_TYPE_L3
		, ACDNT_TYPE                                         as                                         ACDNT_TYPE
		, STS_CODE                                           as                                           STS_CODE
		, ACNTB_DESC                                         as                                         ACNTB_DESC
		, PYMNT_FUND_DESC                                    as                                    PYMNT_FUND_DESC
		, CVRG_DESC                                          as                                          CVRG_DESC
		, BILL_TYPE_F2_DESC                                  as                                  BILL_TYPE_F2_DESC
		, BILL_TYPE_L3_DESC                                  as                                  BILL_TYPE_L3_DESC
		, ACDNT_DESC                                         as                                         ACDNT_DESC
		, WRQ_STS_CODE_DESC                                  as                                  WRQ_STS_CODE_DESC 
				FROM     LOGIC_PAY   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PAY                            as ( SELECT * from    RENAME_PAY   ),

---- JOIN LAYER ----

 JOIN_PAY  as  ( SELECT * 
				FROM  FILTER_PAY )
 SELECT DISTINCT * FROM  JOIN_PAY
      );
    