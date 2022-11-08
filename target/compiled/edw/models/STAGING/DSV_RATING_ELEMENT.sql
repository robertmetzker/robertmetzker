

---- SRC LAYER ----
WITH
SRC_ELE as ( SELECT *     from     STAGING.DST_RATING_ELEMENT ),
//SRC_ELE as ( SELECT *     from     DST_RATING_ELEMENT) ,

---- LOGIC LAYER ----

LOGIC_ELE as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, RT_ELEM_TYP_CD                                     as                                     RT_ELEM_TYP_CD 
		, RT_ELEM_USAGE_TYP_CD                               as                               RT_ELEM_USAGE_TYP_CD 
		, RT_ELEM_TYP_NM                                     as                                     RT_ELEM_TYP_NM 
		, RT_ELEM_USAGE_TYP_DESC                             as                             RT_ELEM_USAGE_TYP_DESC 
		from SRC_ELE
            )

---- RENAME LAYER ----
,

RENAME_ELE as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, RT_ELEM_TYP_CD                                     as                             RATE_ELEMENT_TYPE_CODE
		, RT_ELEM_USAGE_TYP_CD                               as                            RATE_ELEMENT_USAGE_CODE
		, RT_ELEM_TYP_NM                                     as                             RATE_ELEMENT_TYPE_DESC
		, RT_ELEM_USAGE_TYP_DESC                             as                            RATE_ELEMENT_USAGE_DESC 
				FROM     LOGIC_ELE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ELE                            as ( SELECT * from    RENAME_ELE   ),

---- JOIN LAYER ----

 JOIN_ELE  as  ( SELECT * 
				FROM  FILTER_ELE )
 SELECT 
   UNIQUE_ID_KEY
, RATE_ELEMENT_TYPE_CODE
, RATE_ELEMENT_USAGE_CODE
, RATE_ELEMENT_TYPE_DESC
, RATE_ELEMENT_USAGE_DESC
  FROM  JOIN_ELE