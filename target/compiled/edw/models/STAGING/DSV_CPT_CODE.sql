

---- SRC LAYER ----
WITH
SRC_CPT as ( SELECT *     from     STAGING.DST_CPT_CODE ),
//SRC_CPT as ( SELECT *     from     DST_CPT_CODE) ,

---- LOGIC LAYER ----

LOGIC_CPT as ( SELECT 
		  UNIQUE_ID_KEY                                      AS                                      UNIQUE_ID_KEY 
		, SERVICE_CODE                                       AS                                       SERVICE_CODE 
		, SERVICE_LONG_DESC                                  AS                                  SERVICE_LONG_DESC 
		, EFFECTIVE_DATE                                     AS                                     EFFECTIVE_DATE 
		, EXPIRATION_DATE                                    AS                                    EXPIRATION_DATE 
		, ENTRY_DATE                                         AS                                         ENTRY_DATE 
		, TYPE_OF_SVC_DESC                                   AS                                   TYPE_OF_SVC_DESC 
		, PAYMENT_SUBCATEGORY                                AS                                PAYMENT_SUBCATEGORY 
		, CPT_CODE_PAYMENT_CATEGORY                          AS                          CPT_CODE_PAYMENT_CATEGORY 
		, CPT_CODE_FEE_SCHEDULE                              AS                              CPT_CODE_FEE_SCHEDULE 
		from SRC_CPT
            )

---- RENAME LAYER ----
,

RENAME_CPT as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, SERVICE_CODE                                       as                                     PROCEDURE_CODE
		, SERVICE_LONG_DESC                                  as                                     PROCEDURE_DESC
		, EFFECTIVE_DATE                                     as                      PROCEDURE_CODE_EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                            PROCEDURE_CODE_END_DATE
		, ENTRY_DATE                                         as                          PROCEDURE_CODE_ENTRY_DATE
		, TYPE_OF_SVC_DESC                                   as                        PROCEDURE_SERVICE_TYPE_DESC
		, PAYMENT_SUBCATEGORY                                as                            CPT_PAYMENT_SUBCATEGORY
		, CPT_CODE_PAYMENT_CATEGORY                          as                               CPT_PAYMENT_CATEGORY
		, CPT_CODE_FEE_SCHEDULE                              as                              CPT_FEE_SCHEDULE_DESC 
				FROM     LOGIC_CPT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CPT                            as ( SELECT * from    RENAME_CPT   ),

---- JOIN LAYER ----

 JOIN_CPT  as  ( SELECT * 
				FROM  FILTER_CPT )
 SELECT * FROM  JOIN_CPT