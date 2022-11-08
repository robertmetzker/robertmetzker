

      create or replace  table DEV_EDW.STAGING.DST_MANUAL_CLASSIFICATION  as
      (---- SRC LAYER ----
WITH
SRC_CLASS as ( SELECT *     from     STAGING.STG_WC_CLASS ),
SRC_RATE as ( SELECT *     from     STAGING.STG_WC_CLASS_RATE_TIER ),
//SRC_CLASS as ( SELECT *     from     STG_WC_CLASS) ,
//SRC_RATE as ( SELECT *     from     STG_WC_CLASS_RATE_TIER) ,

---- LOGIC LAYER ----

LOGIC_CLASS as ( SELECT 
		   md5(cast(
    
    coalesce(cast(WC_CLS_CLS_CD as 
    varchar
), '') || '-' || coalesce(cast(WC_CLS_SUFX_CLS_SUFX as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                                      UNIQUE_ID_KEY               
		,  WC_CLS_SUFX_ID                                    as                                     WC_CLS_SUFX_ID 
		, WC_CLS_ID                                          as                                          WC_CLS_ID 
		, TRIM( WC_CLS_CLS_CD )                              as                                      WC_CLS_CLS_CD 
		, TRIM( WC_CLS_SUFX_CLS_SUFX )                       as                               WC_CLS_SUFX_CLS_SUFX 
		, TRIM( WC_CLS_SUFX_NM )                             as                                     WC_CLS_SUFX_NM 
		, TRIM( WC_CLS_SUFX_DSCNT_IND )                      as                              WC_CLS_SUFX_DSCNT_IND 
		, WC_CLS_SUFX_EFF_DT                                 as                                 WC_CLS_SUFX_EFF_DT 
		, WC_CLS_SUFX_END_DT                                 as                                 WC_CLS_SUFX_END_DT 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_CLASS
            ),
LOGIC_RATE as ( SELECT 
		  WC_CLS_ID                                          as                                          WC_CLS_ID 
		, TRIM( WC_CLS_RT_TIER_TYP_CD )                      as                              WC_CLS_RT_TIER_TYP_CD 
		, WC_CLS_RT_TIER_EFF_DT                              as                              WC_CLS_RT_TIER_EFF_DT 
		, WC_CLS_RT_TIER_END_DT                              as                              WC_CLS_RT_TIER_END_DT 
		, WC_CLS_RT_TIER_RT                                  as                                  WC_CLS_RT_TIER_RT 
		, TRIM( WC_CLS_RT_TIER_TYP_NM )                      as                              WC_CLS_RT_TIER_TYP_NM 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_RATE
            )

---- RENAME LAYER ----
,

RENAME_CLASS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, WC_CLS_SUFX_ID                                     as                                     WC_CLS_SUFX_ID
		, WC_CLS_ID                                          as                                          WC_CLS_ID
		, WC_CLS_CLS_CD                                      as                                      WC_CLS_CLS_CD
		, WC_CLS_SUFX_CLS_SUFX                               as                               WC_CLS_SUFX_CLS_SUFX
		, WC_CLS_SUFX_NM                                     as                                     WC_CLS_SUFX_NM
		, WC_CLS_SUFX_DSCNT_IND                              as                              WC_CLS_SUFX_DSCNT_IND
		, WC_CLS_SUFX_EFF_DT                                 as                                 WC_CLS_SUFX_EFF_DT
		, WC_CLS_SUFX_END_DT                                 as                                 WC_CLS_SUFX_END_DT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_CLASS   ), 
RENAME_RATE as ( SELECT 
		  WC_CLS_ID                                          as                                     RATE_WC_CLS_ID
		, WC_CLS_RT_TIER_TYP_CD                              as                              WC_CLS_RT_TIER_TYP_CD
		, WC_CLS_RT_TIER_EFF_DT                              as                              WC_CLS_RT_TIER_EFF_DT
		, WC_CLS_RT_TIER_END_DT                              as                              WC_CLS_RT_TIER_END_DT
		, WC_CLS_RT_TIER_RT                                  as                                  WC_CLS_RT_TIER_RT
		, WC_CLS_RT_TIER_TYP_NM                              as                              WC_CLS_RT_TIER_TYP_NM
		, AUDIT_USER_ID_CREA                                 as                            RATE_AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                           RATE_AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                            RATE_AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                           RATE_AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_RATE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CLASS                          as ( SELECT * from    RENAME_CLASS ),
FILTER_RATE                           as ( SELECT * from    RENAME_RATE ),

---- JOIN LAYER ----

CLASS as ( SELECT * 
				FROM  FILTER_CLASS
				LEFT JOIN FILTER_RATE ON  FILTER_CLASS.WC_CLS_ID =  FILTER_RATE.RATE_WC_CLS_ID AND
          ((FILTER_CLASS.WC_CLS_SUFX_CLS_SUFX = '000' and FILTER_RATE.WC_CLS_RT_TIER_TYP_CD = 'RT_1') OR (FILTER_CLASS.WC_CLS_SUFX_CLS_SUFX = '970' and FILTER_RATE.WC_CLS_RT_TIER_TYP_CD = 'RT_2')) AND  FILTER_RATE.WC_CLS_RT_TIER_EFF_DT BETWEEN FILTER_CLASS.WC_CLS_SUFX_EFF_DT AND COALESCE((FILTER_CLASS.WC_CLS_SUFX_END_DT-1),'2099-12-31')  ),

QUALIFY_CLASS AS 
( 
 SELECT * 
	FROM CLASS
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY WC_CLS_CLS_CD,WC_CLS_SUFX_CLS_SUFX ORDER BY WC_CLS_RT_TIER_END_DT DESC) ) = 1
)

SELECT 
		  UNIQUE_ID_KEY
		, WC_CLS_SUFX_ID
		, WC_CLS_ID
		, WC_CLS_CLS_CD
		, WC_CLS_SUFX_CLS_SUFX
		, WC_CLS_SUFX_NM
		, WC_CLS_SUFX_DSCNT_IND
		, WC_CLS_SUFX_EFF_DT
		, WC_CLS_SUFX_END_DT
		, AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM
		, RATE_WC_CLS_ID
		, WC_CLS_RT_TIER_TYP_CD
		, WC_CLS_RT_TIER_EFF_DT
		, WC_CLS_RT_TIER_END_DT
		, WC_CLS_RT_TIER_RT
		, WC_CLS_RT_TIER_TYP_NM
		, RATE_AUDIT_USER_ID_CREA
		, RATE_AUDIT_USER_CREA_DTM
		, RATE_AUDIT_USER_ID_UPDT
		, RATE_AUDIT_USER_UPDT_DTM 
from QUALIFY_CLASS
      );
    