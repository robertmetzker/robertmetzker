---- SRC LAYER ----
WITH
SRC_WCS as ( SELECT *     from     DEV_VIEWS.PCMP.WC_CLASS_SUFFIX ),
SRC_WC as ( SELECT *     from     DEV_VIEWS.PCMP.WC_CLASS ),
SRC_JT as ( SELECT *     from     DEV_VIEWS.PCMP.JURISDICTION_TYPE ),
SRC_WCT as ( SELECT *     from     DEV_VIEWS.PCMP.WC_CLASS_TYPE ),
SRC_PBT as ( SELECT *     from     DEV_VIEWS.PCMP.PREMIUM_BASIS_TYPE ),
//SRC_WCS as ( SELECT *     from     WC_CLASS_SUFFIX) ,
//SRC_WC as ( SELECT *     from     WC_CLASS) ,
//SRC_JT as ( SELECT *     from     JURISDICTION_TYPE) ,
//SRC_WCT as ( SELECT *     from     WC_CLASS_TYPE) ,
//SRC_PBT as ( SELECT *     from     PREMIUM_BASIS_TYPE) ,

---- LOGIC LAYER ----

LOGIC_WCS as ( SELECT 
		  WC_CLS_SUFX_ID                                     AS                                     WC_CLS_SUFX_ID 
		, WC_CLS_ID                                          AS                                          WC_CLS_ID 
		, upper( TRIM( WC_CLS_SUFX_CLS_SUFX ) )              AS                               WC_CLS_SUFX_CLS_SUFX 
		, upper( TRIM( WC_CLS_SUFX_NM ) )                    AS                                     WC_CLS_SUFX_NM 
		, upper( WC_CLS_SUFX_DSCNT_IND )                     AS                              WC_CLS_SUFX_DSCNT_IND 
		, cast( WC_CLS_SUFX_EFF_DT as DATE )                 AS                                 WC_CLS_SUFX_EFF_DT 
		, cast( WC_CLS_SUFX_END_DT as DATE )                 AS                                 WC_CLS_SUFX_END_DT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( WC_CLS_SUFX_VOID_IND )                      AS                               WC_CLS_SUFX_VOID_IND 
		from SRC_WCS
            ),
LOGIC_WC as ( SELECT 
		  TRIM( WC_CLS_CLS_CD )                              AS                                      WC_CLS_CLS_CD 
		, upper( WC_CLS_DSCNT_IND )                          AS                                   WC_CLS_DSCNT_IND 
		, upper( TRIM( JUR_TYP_CD ) )                        AS                                         JUR_TYP_CD 
		, upper( TRIM( WC_CLS_TYP_CD ) )                     AS                                      WC_CLS_TYP_CD 
		, upper( TRIM( PREM_BS_TYP_CD ) )                    AS                                     PREM_BS_TYP_CD 
		, WC_CLS_ID                                          AS                                          WC_CLS_ID 
		from SRC_WC
            ),
LOGIC_JT as ( SELECT 
		  upper( TRIM( JUR_TYP_NM ) )                        AS                                         JUR_TYP_NM 
		, upper( TRIM( JUR_TYP_CD ) )                        AS                                         JUR_TYP_CD 
		, upper( JUR_TYP_VOID_IND )                          AS                                   JUR_TYP_VOID_IND 
		from SRC_JT
            ),
LOGIC_WCT as ( SELECT 
		  upper( TRIM( WC_CLS_TYP_NM ) )                     AS                                      WC_CLS_TYP_NM 
		, upper( TRIM( WC_CLS_TYP_CD ) )                     AS                                      WC_CLS_TYP_CD 
		, upper( WC_CLS_TYP_VOID_IND )                       AS                                WC_CLS_TYP_VOID_IND 
		from SRC_WCT
            ),
LOGIC_PBT as ( SELECT 
		  upper( TRIM( PREM_BS_TYP_NM ) )                    AS                                     PREM_BS_TYP_NM 
		, upper( TRIM( PREM_BS_TYP_CD ) )                    AS                                     PREM_BS_TYP_CD 
		, upper( PREM_BS_VOID_IND )                          AS                                   PREM_BS_VOID_IND 
		from SRC_PBT
            )

---- RENAME LAYER ----
,

RENAME_WCS as ( SELECT 
		  WC_CLS_SUFX_ID                                     as                                     WC_CLS_SUFX_ID
		, WC_CLS_ID                                          as                                          WC_CLS_ID
		, WC_CLS_SUFX_CLS_SUFX                               as                               WC_CLS_SUFX_CLS_SUFX
		, WC_CLS_SUFX_NM                                     as                                     WC_CLS_SUFX_NM
		, WC_CLS_SUFX_DSCNT_IND                              as                              WC_CLS_SUFX_DSCNT_IND
		, WC_CLS_SUFX_EFF_DT                                 as                                 WC_CLS_SUFX_EFF_DT
		, WC_CLS_SUFX_END_DT                                 as                                 WC_CLS_SUFX_END_DT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, WC_CLS_SUFX_VOID_IND                               as                               WC_CLS_SUFX_VOID_IND 
				FROM     LOGIC_WCS   ), 
RENAME_WC as ( SELECT 
		  WC_CLS_CLS_CD                                      as                                      WC_CLS_CLS_CD
		, WC_CLS_DSCNT_IND                                   as                                   WC_CLS_DSCNT_IND
		, JUR_TYP_CD                                         as                                         JUR_TYP_CD
		, WC_CLS_TYP_CD                                      as                                      WC_CLS_TYP_CD
		, PREM_BS_TYP_CD                                     as                                     PREM_BS_TYP_CD
		, WC_CLS_ID                                          as                                       WC_WC_CLS_ID 
				FROM     LOGIC_WC   ), 
RENAME_JT as ( SELECT 
		  JUR_TYP_NM                                         as                                         JUR_TYP_NM
		, JUR_TYP_CD                                         as                                      JT_JUR_TYP_CD
		, JUR_TYP_VOID_IND                                   as                                   JUR_TYP_VOID_IND 
				FROM     LOGIC_JT   ), 
RENAME_WCT as ( SELECT 
		  WC_CLS_TYP_NM                                      as                                      WC_CLS_TYP_NM
		, WC_CLS_TYP_CD                                      as                                  WCT_WC_CLS_TYP_CD
		, WC_CLS_TYP_VOID_IND                                as                                WC_CLS_TYP_VOID_IND 
				FROM     LOGIC_WCT   ), 
RENAME_PBT as ( SELECT 
		  PREM_BS_TYP_NM                                     as                                     PREM_BS_TYP_NM
		, PREM_BS_TYP_CD                                     as                                 PBT_PREM_BS_TYP_CD
		, PREM_BS_VOID_IND                                   as                                   PREM_BS_VOID_IND 
				FROM     LOGIC_PBT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_WCS                            as ( SELECT * from    RENAME_WCS   ),
FILTER_WC                             as ( SELECT * from    RENAME_WC   ),
FILTER_JT                             as ( SELECT * from    RENAME_JT 
				WHERE JUR_TYP_VOID_IND = 'N'  ),
FILTER_WCT                            as ( SELECT * from    RENAME_WCT 
				WHERE WC_CLS_TYP_VOID_IND = 'N'  ),
FILTER_PBT                            as ( SELECT * from    RENAME_PBT 
				WHERE PREM_BS_VOID_IND = 'N'  ),

---- JOIN LAYER ----

WC as ( SELECT * 
				FROM  FILTER_WC
				LEFT JOIN FILTER_JT ON  FILTER_WC.JUR_TYP_CD =  FILTER_JT.JT_JUR_TYP_CD 
								LEFT JOIN FILTER_WCT ON  FILTER_WC.WC_CLS_TYP_CD =  FILTER_WCT.WCT_WC_CLS_TYP_CD 
								LEFT JOIN FILTER_PBT ON  FILTER_WC.PREM_BS_TYP_CD =  FILTER_PBT.PBT_PREM_BS_TYP_CD  ),
WCS as ( SELECT * 
				FROM  FILTER_WCS
				INNER JOIN WC ON  FILTER_WCS.WC_CLS_ID = WC.WC_WC_CLS_ID  )
SELECT * 
from WCS