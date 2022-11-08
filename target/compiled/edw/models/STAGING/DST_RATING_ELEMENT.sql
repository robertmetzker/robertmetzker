---- SRC LAYER ----
WITH
SRC_ELE as ( SELECT *     from     STAGING.STG_RATING_ELEMENT ),
//SRC_ELE as ( SELECT *     from     STG_RATING_ELEMENT) ,

---- LOGIC LAYER ----

LOGIC_ELE as ( SELECT 
		  TRIM( RT_ELEM_TYP_CD )                             as                                     RT_ELEM_TYP_CD 
		, TRIM( RT_ELEM_TYP_NM )                             as                                     RT_ELEM_TYP_NM 
		, TRIM( RT_ELEM_USAGE_TYP_CD )                       as                               RT_ELEM_USAGE_TYP_CD 
		, TRIM( RT_ELEM_USAGE_TYP_DESC )                     as                             RT_ELEM_USAGE_TYP_DESC 
		from SRC_ELE
            )

---- RENAME LAYER ----
,

RENAME_ELE as ( SELECT 
		  RT_ELEM_TYP_CD                                     as                                     RT_ELEM_TYP_CD
		, RT_ELEM_TYP_NM                                     as                                     RT_ELEM_TYP_NM
		, RT_ELEM_USAGE_TYP_CD                               as                               RT_ELEM_USAGE_TYP_CD
		, RT_ELEM_USAGE_TYP_DESC                             as                             RT_ELEM_USAGE_TYP_DESC 
				FROM     LOGIC_ELE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ELE                            as ( SELECT * from    RENAME_ELE   ),

---- JOIN LAYER ----

 JOIN_ELE  as  ( SELECT * 
				FROM  FILTER_ELE )
 SELECT DISTINCT
  md5(cast(
    
    coalesce(cast(RT_ELEM_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY  
, RT_ELEM_TYP_CD
, RT_ELEM_TYP_NM
, RT_ELEM_USAGE_TYP_CD
, RT_ELEM_USAGE_TYP_DESC
  FROM  JOIN_ELE