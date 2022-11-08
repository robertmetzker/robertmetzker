---- SRC LAYER ----
WITH
SRC_INV as ( SELECT *     from     STAGING.DSV_INVOICE ),
//SRC_INV as ( SELECT *     from     DSV_INVOICE) ,

---- LOGIC LAYER ----

LOGIC_INV as ( SELECT
		  TRIM( INVOICE_TYPE )                               AS                                       INVOICE_TYPE 
		, TRIM( SUBROGATION_FLAG )                           AS                                   SUBROGATION_FLAG 
		, TRIM( ADJUSTMENT_TYPE )                            AS                                    ADJUSTMENT_TYPE 
		, TRIM( INPUT_METHOD_CODE )                          AS                                  INPUT_METHOD_CODE 
		, CASE WHEN MCO_NUMBER = '20001' THEN 'SCANNED'
      			WHEN BATCH_TYPE = 'M' THEN 'MANUAL'
        		WHEN BATCH_TYPE = 'E' THEN 'ELECTRONIC DATA INTERFACE' END
															 AS                                  INPUT_METHOD_DESC 
		, TRIM( PAYMENT_CATEGORY )                           AS                                   PAYMENT_CATEGORY 
		, TRIM( FEE_SCHEDULE )                               AS                                       FEE_SCHEDULE 
		, TRIM( PAID_ABOVE_ZERO_IND )                        AS                                PAID_ABOVE_ZERO_IND 
		, TRIM( SUBROGATION_TYPE_DESC )                      AS                              SUBROGATION_TYPE_DESC 
		, TRIM( MCO_NUMBER )                                 AS                                         MCO_NUMBER 
		, TRIM( BATCH_TYPE )                                 AS                                         BATCH_TYPE 
		from SRC_INV
            )

---- RENAME LAYER ----
,

RENAME_INV as ( SELECT 
		  INVOICE_TYPE                                       as                                       INVOICE_TYPE
		, SUBROGATION_FLAG                                   as                                   SUBROGATION_FLAG
		, ADJUSTMENT_TYPE                                    as                                    ADJUSTMENT_TYPE
		, INPUT_METHOD_CODE                                  as                                  INPUT_METHOD_CODE
		, INPUT_METHOD_DESC                                  as                                  INPUT_METHOD_DESC
		, PAYMENT_CATEGORY                                   as                                   PAYMENT_CATEGORY
		, FEE_SCHEDULE                                       as                                       FEE_SCHEDULE
		, PAID_ABOVE_ZERO_IND                                as                                PAID_ABOVE_ZERO_IND
		, SUBROGATION_TYPE_DESC                              as                              SUBROGATION_TYPE_DESC
		, MCO_NUMBER                                         as                                         MCO_NUMBER
		, BATCH_TYPE                                         as                                         BATCH_TYPE 
				FROM     LOGIC_INV   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_INV                            as ( SELECT * from    RENAME_INV   ),

---- JOIN LAYER ----

JOIN_INV  as  ( SELECT * 
				FROM  FILTER_INV ),

-- ETL LAYER FOR CREATE DISTINCT COMBINATION OF THE VALUES
SRT AS (
SELECT DISTINCT
INVOICE_TYPE
, SUBROGATION_FLAG
, ADJUSTMENT_TYPE
, INPUT_METHOD_CODE
, INPUT_METHOD_DESC
, PAYMENT_CATEGORY
, FEE_SCHEDULE
, PAID_ABOVE_ZERO_IND
, SUBROGATION_TYPE_DESC
FROM JOIN_INV
)
 SELECT 
   md5(cast(
    
    coalesce(cast(INVOICE_TYPE as 
    varchar
), '') || '-' || coalesce(cast(SUBROGATION_FLAG as 
    varchar
), '') || '-' || coalesce(cast(ADJUSTMENT_TYPE as 
    varchar
), '') || '-' || coalesce(cast(INPUT_METHOD_CODE as 
    varchar
), '') || '-' || coalesce(cast(PAYMENT_CATEGORY as 
    varchar
), '') || '-' || coalesce(cast(FEE_SCHEDULE as 
    varchar
), '') || '-' || coalesce(cast(PAID_ABOVE_ZERO_IND as 
    varchar
), '') || '-' || coalesce(cast(SUBROGATION_TYPE_DESC as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY
 , * 
 FROM  SRT