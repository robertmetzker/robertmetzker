

---- SRC LAYER ----
WITH
SRC_DPC as ( SELECT *     from     STAGING.DST_PAYEE_NAME ),
//SRC_DPC as ( SELECT *     from     DST_PAYEE_NAME) ,

---- LOGIC LAYER ----

LOGIC_DPC as ( SELECT 
		  UNIQUE_ID_KEY                                      AS                                      UNIQUE_ID_KEY 
		, PAYEE_NAME                                         AS                                         PAYEE_NAME 
		from SRC_DPC
            )

---- RENAME LAYER ----
,

RENAME_DPC as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, PAYEE_NAME                                         as                                    PAYEE_FULL_NAME 
				FROM     LOGIC_DPC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DPC                            as ( SELECT * from    RENAME_DPC   ),

---- JOIN LAYER ----

 JOIN_DPC  as  ( SELECT * 
				FROM  FILTER_DPC )
 SELECT * FROM  JOIN_DPC