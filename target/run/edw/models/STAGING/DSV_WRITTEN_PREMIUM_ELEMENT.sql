
  create or replace  view DEV_EDW.STAGING.DSV_WRITTEN_PREMIUM_ELEMENT  as (
    

---- SRC LAYER ----
WITH
SRC_WPE            as ( SELECT *     FROM     STAGING.DST_WRITTEN_PREMIUM_ELEMENT ),
//SRC_WPE            as ( SELECT *     FROM     DST_WRITTEN_PREMIUM_ELEMENT) ,

---- LOGIC LAYER ----


LOGIC_WPE as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, WRITTEN_PREMIUM_ELEMENT_CODE                       as                       WRITTEN_PREMIUM_ELEMENT_CODE 
		, WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE                as                WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE 
		, WRITTEN_PREMIUM_ELEMENT_DESC                       as                       WRITTEN_PREMIUM_ELEMENT_DESC 
		, WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE             as             WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE 
		, WRITTEN_PREMIUM_ELEMENT_END_DATE                   as                   WRITTEN_PREMIUM_ELEMENT_END_DATE 
		FROM SRC_WPE
            )

---- RENAME LAYER ----
,

RENAME_WPE        as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, WRITTEN_PREMIUM_ELEMENT_CODE                       as                       WRITTEN_PREMIUM_ELEMENT_CODE
		, WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE                as                WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE
		, WRITTEN_PREMIUM_ELEMENT_DESC                       as                       WRITTEN_PREMIUM_ELEMENT_DESC
		, WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE             as             WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE
		, WRITTEN_PREMIUM_ELEMENT_END_DATE                   as                   WRITTEN_PREMIUM_ELEMENT_END_DATE 
				FROM     LOGIC_WPE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_WPE                            as ( SELECT * FROM    RENAME_WPE   ),

---- JOIN LAYER ----

 JOIN_WPE         as  ( SELECT * 
				FROM  FILTER_WPE )
 SELECT * FROM  JOIN_WPE
  );
