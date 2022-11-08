

---- SRC LAYER ----
WITH
SRC_INV as ( SELECT *     from     STAGING.DST_INVOICE_PROFILE ),
//SRC_INV as ( SELECT *     from     DST_INVOICE_PROFILE) ,

---- LOGIC LAYER ----

LOGIC_INV as ( SELECT 
		  UNIQUE_ID_KEY                                      AS                                      UNIQUE_ID_KEY 
		, FEE_SCHEDULE                                       AS                                       FEE_SCHEDULE 
		, INVOICE_TYPE                                       AS                                       INVOICE_TYPE 
		, PAYMENT_CATEGORY                                   AS                                   PAYMENT_CATEGORY 
		, ADJUSTMENT_TYPE                                    AS                                    ADJUSTMENT_TYPE 
		, SUBROGATION_FLAG                                   AS                                   SUBROGATION_FLAG 
		, INPUT_METHOD_CODE                                  AS                                  INPUT_METHOD_CODE 
		, PAID_ABOVE_ZERO_IND                                AS                                PAID_ABOVE_ZERO_IND 
		, SUBROGATION_TYPE_DESC                              AS                              SUBROGATION_TYPE_DESC 
		, INPUT_METHOD_DESC                                  AS                                  INPUT_METHOD_DESC 
		from SRC_INV
            )

---- RENAME LAYER ----
,

RENAME_INV as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, FEE_SCHEDULE                                       as                          INVOICE_FEE_SCHEDULE_DESC
		, INVOICE_TYPE                                       as                          MEDICAL_INVOICE_TYPE_CODE
		, PAYMENT_CATEGORY                                   as                           INVOICE_PAYMENT_CATEGORY
		, ADJUSTMENT_TYPE                                    as                                    ADJUSTMENT_TYPE
		, SUBROGATION_FLAG                                   as                                 IN_SUBROGATION_IND
		, INPUT_METHOD_CODE                                  as                          INVOICE_INPUT_METHOD_CODE
		, PAID_ABOVE_ZERO_IND                                as                                PAID_ABOVE_ZERO_IND
		, SUBROGATION_TYPE_DESC                              as                              SUBROGATION_TYPE_DESC
		, INPUT_METHOD_DESC                                  as                          INVOICE_INPUT_METHOD_DESC 
				FROM     LOGIC_INV   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_INV                            as ( SELECT * from    RENAME_INV   ),

---- JOIN LAYER ----

 JOIN_INV  as  ( SELECT * 
				FROM  FILTER_INV )
 SELECT * FROM  JOIN_INV