

---- SRC LAYER ----
WITH
SRC_CMT            as ( SELECT *     FROM     STAGING.DST_POLICY_FINANCIAL_TRANSACTION_COMMENT ),
//SRC_CMT            as ( SELECT *     FROM     DST_POLICY_FINANCIAL_TRANSACTION_COMMENT) ,

---- LOGIC LAYER ----


LOGIC_CMT as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, PFT_CMT                                            as                                            PFT_CMT 
		FROM SRC_CMT
            )

---- RENAME LAYER ----
,

RENAME_CMT        as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, PFT_CMT                                            as                 FINANCIAL_TRANSACTION_COMMENT_TEXT 
				FROM     LOGIC_CMT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CMT                            as ( SELECT * FROM    RENAME_CMT   ),

---- JOIN LAYER ----

 JOIN_CMT         as  ( SELECT * 
				FROM  FILTER_CMT )
 SELECT * FROM  JOIN_CMT