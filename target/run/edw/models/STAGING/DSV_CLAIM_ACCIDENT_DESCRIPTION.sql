
  create or replace  view DEV_EDW.STAGING.DSV_CLAIM_ACCIDENT_DESCRIPTION  as (
    

---- SRC LAYER ----
WITH
SRC_CLM as ( SELECT *     from     STAGING.DST_CLAIM_ACCIDENT_DESCRIPTION ),
//SRC_CLM as ( SELECT *     from     DST_CLAIM_ACCIDENT_DESCRIPTION) ,

---- LOGIC LAYER ----

LOGIC_CLM as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, CLM_LOSS_DESC                                      as                                      CLM_LOSS_DESC 
		, CLM_CLMT_JOB_TTL                                   as                                   CLM_CLMT_JOB_TTL 
		from SRC_CLM
            )

---- RENAME LAYER ----
,

RENAME_CLM as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CLM_LOSS_DESC                                      as                          ACCIDENT_DESCRIPTION_TEXT
		, CLM_CLMT_JOB_TTL                                   as                                          JOB_TITLE 
				FROM     LOGIC_CLM   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),

---- JOIN LAYER ----

 JOIN_CLM  as  ( SELECT * 
				FROM  FILTER_CLM )
 SELECT 
  UNIQUE_ID_KEY
, ACCIDENT_DESCRIPTION_TEXT
, JOB_TITLE
 FROM  JOIN_CLM
  );
