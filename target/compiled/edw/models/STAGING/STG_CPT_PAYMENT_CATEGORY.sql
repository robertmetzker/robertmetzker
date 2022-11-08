---- SRC LAYER ----
WITH
SRC_CPT_PC as ( SELECT *     from     DEV_VIEWS.BWC_FILES.PROCEDURE_CODE_PAYMENT_CATEGORY ),
//SRC_CPT_PC as ( SELECT *     from     PROCEDURE_CODE_PAYMENT_CATEGORY) ,

---- LOGIC LAYER ----

LOGIC_CPT_PC as ( SELECT 
		  UPPER(TRIM( PROCEDURE_CODE ) )                     AS                                     PROCEDURE_CODE 
		, UPPER(TRIM( PAYMENT_CATEGORY ))                    AS                                   PAYMENT_CATEGORY 
		, UPPER(TRIM( PAYMENT_SUBCATEGORY ) )                AS                                PAYMENT_SUBCATEGORY 
		from SRC_CPT_PC
            )

---- RENAME LAYER ----
,

RENAME_CPT_PC as ( SELECT 
		  PROCEDURE_CODE                                     as                                     PROCEDURE_CODE
		, PAYMENT_CATEGORY                                   as                                   PAYMENT_CATEGORY
		, PAYMENT_SUBCATEGORY                                as                                PAYMENT_SUBCATEGORY 
				FROM     LOGIC_CPT_PC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CPT_PC                         as ( SELECT * from    RENAME_CPT_PC   ),

---- JOIN LAYER ----

 JOIN_CPT_PC  as  ( SELECT * 
				FROM  FILTER_CPT_PC )
 SELECT * FROM  JOIN_CPT_PC