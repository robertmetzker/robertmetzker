---- SRC LAYER ----
WITH
SRC_RE             as ( SELECT *     FROM     STAGING.STG_RATING_ELEMENT ),
//SRC_RE             as ( SELECT *     FROM     STG_RATING_ELEMENT) ,

---- LOGIC LAYER ----


LOGIC_RE as ( SELECT 
		  TRIM( RT_ELEM_TYP_CD )                             as                                     RT_ELEM_TYP_CD 
		, TRIM( RT_ELEM_TYP_NM )                             as                                     RT_ELEM_TYP_NM 
		FROM SRC_RE
            )

---- RENAME LAYER ----
,

RENAME_RE         as ( SELECT 
		  RT_ELEM_TYP_CD                                     as                                     RT_ELEM_TYP_CD
		, RT_ELEM_TYP_NM                                     as                                     RT_ELEM_TYP_NM 
				FROM     LOGIC_RE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_RE                             as ( SELECT * FROM    RENAME_RE 
                                            WHERE RT_ELEM_TYP_CD IN ('GRPEXPRTDPRGM', 'GRPRETRO', 'RTSPINTRA', 'EMOD', 'CLS_RT_TIER')  ),

---- JOIN LAYER ----

 JOIN_RE          as  ( SELECT * 
				FROM  FILTER_RE ),
---------ETL LAYER---------------
ETL AS (SELECT md5(cast(
    
    coalesce(cast(RT_ELEM_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY
,RT_ELEM_TYP_CD
,RT_ELEM_TYP_NM 
FROM JOIN_RE)				

 SELECT * FROM  ETL