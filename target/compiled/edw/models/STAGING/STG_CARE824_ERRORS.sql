---- SRC LAYER ----
WITH
SRC_ERRORS as ( SELECT *     from     DEV_VIEWS.BASE.CARE824_ERRORS ),
//SRC_ERRORS as ( SELECT *     from     CARE824_ERRORS) ,

---- LOGIC LAYER ----

LOGIC_ERRORS as ( SELECT 
		  CARE824_RID                                        AS                                        CARE824_RID 
		, upper( TRIM( ERROR_CODE ) )                        AS                                         ERROR_CODE 
		, upper( TRIM( ERROR_MESSAGE ) )                     AS                                      ERROR_MESSAGE 
		, upper( TRIM( DATA_IN_ERROR ) )                     AS                                      DATA_IN_ERROR 
		from SRC_ERRORS
            )

---- RENAME LAYER ----
,

RENAME_ERRORS as ( SELECT 
		  CARE824_RID                                        as                                        CARE824_RID
		, ERROR_CODE                                         as                                         ERROR_CODE
		, ERROR_MESSAGE                                      as                                      ERROR_MESSAGE
		, DATA_IN_ERROR                                      as                                      DATA_IN_ERROR 
				FROM     LOGIC_ERRORS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ERRORS                         as ( SELECT * from    RENAME_ERRORS   ),

---- JOIN LAYER ----

 JOIN_ERRORS  as  ( SELECT * 
				FROM  FILTER_ERRORS )
 SELECT * FROM  JOIN_ERRORS