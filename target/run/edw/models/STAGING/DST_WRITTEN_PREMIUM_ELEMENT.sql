

      create or replace  table DEV_EDW.STAGING.DST_WRITTEN_PREMIUM_ELEMENT  as
      (---- SRC LAYER ----
WITH
SRC_WC             as ( SELECT *     FROM     STAGING.STG_WC_CLASS ),
SRC_RE             as ( SELECT *     FROM     STAGING.STG_RATING_ELEMENT ),
//SRC_WC             as ( SELECT *     FROM     STG_WC_CLASS) ,
//SRC_RE             as ( SELECT *     FROM     STG_RATING_ELEMENT) ,

---- LOGIC LAYER ----


LOGIC_WC as ( SELECT 
		 md5(cast(
    
    coalesce(cast(WC_CLS_CLS_CD as 
    varchar
), '') || '-' || coalesce(cast(WC_CLS_SUFX_CLS_SUFX as 
    varchar
), '') || '-' || coalesce(cast(WC_CLS_SUFX_EFF_DT as 
    varchar
), '')

 as 
    varchar
))   as  UNIQUE_ID_KEY
		, WC_CLS_CLS_CD                                      as                                      WC_CLS_CLS_CD 
		, WC_CLS_SUFX_CLS_SUFX                               as                               WC_CLS_SUFX_CLS_SUFX 
		, WC_CLS_SUFX_NM                                     as                                     WC_CLS_SUFX_NM 
		, WC_CLS_SUFX_EFF_DT                                 as                                 WC_CLS_SUFX_EFF_DT 
		, WC_CLS_SUFX_END_DT                                 as                                 WC_CLS_SUFX_END_DT 
		FROM SRC_WC
            ),

LOGIC_RE as ( SELECT 
		md5(cast(
    
    coalesce(cast(RT_ELEM_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(RT_ELEM_TYP_NM_EFF_DT as 
    varchar
), '')

 as 
    varchar
))                as  UNIQUE_ID_KEY 
		, RT_ELEM_TYP_CD                                     as                                     RT_ELEM_TYP_CD 
		, null                                               as                WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE 
		, RT_ELEM_TYP_NM                                     as                                     RT_ELEM_TYP_NM 
		, RT_ELEM_TYP_NM_EFF_DT                              as                              RT_ELEM_TYP_NM_EFF_DT 
		, RT_ELEM_TYP_NM_END_DT                              as                              RT_ELEM_TYP_NM_END_DT 
		FROM SRC_RE
            )

---- RENAME LAYER ----
,

RENAME_WC         as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, WC_CLS_CLS_CD                                      as                       WRITTEN_PREMIUM_ELEMENT_CODE
		, WC_CLS_SUFX_CLS_SUFX                               as                WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE
		, WC_CLS_SUFX_NM                                     as                       WRITTEN_PREMIUM_ELEMENT_DESC
		, WC_CLS_SUFX_EFF_DT                                 as             WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE
		, WC_CLS_SUFX_END_DT                                 as                   WRITTEN_PREMIUM_ELEMENT_END_DATE
				FROM     LOGIC_WC   ), 
RENAME_RE         as ( SELECT 
          UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, RT_ELEM_TYP_CD                                     as                       WRITTEN_PREMIUM_ELEMENT_CODE
 		, WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE                as                WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE
		, RT_ELEM_TYP_NM                                     as                       WRITTEN_PREMIUM_ELEMENT_DESC
		, RT_ELEM_TYP_NM_EFF_DT                              as             WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE
		, RT_ELEM_TYP_NM_END_DT                              as                   WRITTEN_PREMIUM_ELEMENT_END_DATE
				FROM     LOGIC_RE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_WC                             as ( SELECT * FROM    RENAME_WC   ),
FILTER_RE                             as ( SELECT * FROM    RENAME_RE 
                                            WHERE WRITTEN_PREMIUM_ELEMENT_CODE IN ('BPMP', 'DFRD_DEP_ASMT', 'DFRD_RPT_ASMT', 'DFSPADP', 'DFSPBSP', 'GOGREENP', 'ISSPDP', 'LPSFREDSCNTP', 'PD', 'PMP', 'PSC', 'R_BMP', 'SCPDP', 'SFCNPFDSP', 'TWPBDP')  ),

---- JOIN LAYER ----

 UNION_ETL          as  ( SELECT * FROM  FILTER_WC 
							UNION 
						  SELECT * FROM FILTER_RE )
 SELECT * FROM  UNION_ETL
      );
    