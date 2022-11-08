
  create or replace  view DEV_EDW.STAGING.DSV_RATING_PLAN_TYPE  as (
    

---- SRC LAYER ----
WITH
SRC_RE             as ( SELECT *     FROM     STAGING.DST_RATING_PLAN_TYPE ),
//SRC_RE             as ( SELECT *     FROM     DST_RATING_PLAN_TYPE) ,

---- LOGIC LAYER ----


LOGIC_RE as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, RT_ELEM_TYP_CD                                     as                                     RT_ELEM_TYP_CD 
		, RT_ELEM_TYP_NM                                     as                                     RT_ELEM_TYP_NM 
		FROM SRC_RE
            )

---- RENAME LAYER ----
,

RENAME_RE         as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, RT_ELEM_TYP_CD                                     as                                   RATING_PLAN_CODE
		, RT_ELEM_TYP_NM                                     as                                   RATING_PLAN_DESC 
				FROM     LOGIC_RE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_RE                             as ( SELECT * FROM    RENAME_RE   ),

---- JOIN LAYER ----

 JOIN_RE          as  ( SELECT * 
				FROM  FILTER_RE )
 SELECT * FROM  JOIN_RE
  );
