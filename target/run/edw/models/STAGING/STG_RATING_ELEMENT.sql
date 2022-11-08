

      create or replace  table DEV_EDW.STAGING.STG_RATING_ELEMENT  as
      (---- SRC LAYER ----
WITH
SRC_RET as ( SELECT *     from     DEV_VIEWS.PCMP.RATING_ELEMENT_TYPE ),
SRC_RENT as ( SELECT *     from     DEV_VIEWS.PCMP.RATING_ELEMENT_NAME_TYPE ),
SRC_REUT as ( SELECT *     from     DEV_VIEWS.PCMP.RATING_ELEMENT_USAGE_TYPE ),
//SRC_RET as ( SELECT *     from     RATING_ELEMENT_TYPE) ,
//SRC_RENT as ( SELECT *     from     RATING_ELEMENT_NAME_TYPE) ,
//SRC_REUT as ( SELECT *     from     RATING_ELEMENT_USAGE_TYPE) ,

---- LOGIC LAYER ----

LOGIC_RET as ( SELECT 
		  upper( TRIM( RT_ELEM_TYP_CD ) )                    AS                                     RT_ELEM_TYP_CD 
		, upper( TRIM( RT_ELEM_USAGE_TYP_CD ) )              AS                               RT_ELEM_USAGE_TYP_CD 
		, upper( TRIM( RT_ELEM_TYP_RLT ) )                   AS                                    RT_ELEM_TYP_RLT 
		, upper( RT_ELEM_TYP_VOID_IND )                      AS                               RT_ELEM_TYP_VOID_IND 
		from SRC_RET
            ),
LOGIC_RENT as ( SELECT 
		  upper( TRIM( RT_ELEM_TYP_NM ) )                    AS                                     RT_ELEM_TYP_NM 
		, cast( RT_ELEM_TYP_NM_EFF_DT as DATE )              AS                              RT_ELEM_TYP_NM_EFF_DT 
		, cast( RT_ELEM_TYP_NM_END_DT as DATE )              AS                              RT_ELEM_TYP_NM_END_DT 
		, upper( TRIM( RT_ELEM_TYP_CD ) )                    AS                                     RT_ELEM_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_RENT
            ),
LOGIC_REUT as ( SELECT 
		  upper( TRIM( RT_ELEM_USAGE_TYP_DESC ) )            AS                             RT_ELEM_USAGE_TYP_DESC 
		, upper( TRIM( RT_ELEM_USAGE_TYP_CD ) )              AS                               RT_ELEM_USAGE_TYP_CD 
		from SRC_REUT
            )

---- RENAME LAYER ----
,

RENAME_RET as ( SELECT 
		  RT_ELEM_TYP_CD                                     as                                     RT_ELEM_TYP_CD
		, RT_ELEM_USAGE_TYP_CD                               as                               RT_ELEM_USAGE_TYP_CD
		, RT_ELEM_TYP_RLT                                    as                                    RT_ELEM_TYP_RLT
		, RT_ELEM_TYP_VOID_IND                               as                                       RET_VOID_IND 
				FROM     LOGIC_RET   ), 
RENAME_RENT as ( SELECT 
		  RT_ELEM_TYP_NM                                     as                                     RT_ELEM_TYP_NM
		, RT_ELEM_TYP_NM_EFF_DT                              as                              RT_ELEM_TYP_NM_EFF_DT
		, RT_ELEM_TYP_NM_END_DT                              as                              RT_ELEM_TYP_NM_END_DT
		, RT_ELEM_TYP_CD                                     as                                RENT_RT_ELEM_TYP_CD
		, VOID_IND                                           as                                      RENT_VOID_IND 
				FROM     LOGIC_RENT   ), 
RENAME_REUT as ( SELECT 
		  RT_ELEM_USAGE_TYP_DESC                             as                             RT_ELEM_USAGE_TYP_DESC
		, RT_ELEM_USAGE_TYP_CD                               as                          REUT_RT_ELEM_USAGE_TYP_CD 
				FROM     LOGIC_REUT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_RET                            as ( SELECT * from    RENAME_RET 
				WHERE RET_VOID_IND = 'N'  ),
FILTER_RENT                           as ( SELECT * from    RENAME_RENT 
				WHERE RENT_VOID_IND = 'N'  ),
FILTER_REUT                           as ( SELECT * from    RENAME_REUT   ),

---- JOIN LAYER ----

RET as ( SELECT * 
				FROM  FILTER_RET
				LEFT JOIN FILTER_RENT ON  FILTER_RET.RT_ELEM_TYP_CD =  FILTER_RENT.RENT_RT_ELEM_TYP_CD 
								LEFT JOIN FILTER_REUT ON  FILTER_RET.RT_ELEM_USAGE_TYP_CD =  FILTER_REUT.REUT_RT_ELEM_USAGE_TYP_CD  )
SELECT * 
from RET
      );
    