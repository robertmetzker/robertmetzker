---- SRC LAYER ----
WITH
SRC_CWSD as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_WAGE_SOURCE_DETAIL ),
SRC_CWRST as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_WAGE_REPORT_SOURCE_TYPE ),
SRC_CWT as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_WAGE_TYPE ),
SRC_CPPT as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_PAY_PERIOD_TYPE ),
//SRC_CWSD as ( SELECT *     from     CLAIM_WAGE_SOURCE_DETAIL) ,
//SRC_CWRST as ( SELECT *     from     CLAIM_WAGE_REPORT_SOURCE_TYPE) ,
//SRC_CWT as ( SELECT *     from     CLAIM_WAGE_TYPE) ,
//SRC_CPPT as ( SELECT *     from     CLAIM_PAY_PERIOD_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CWSD as ( SELECT 
		  CLM_WG_SRC_DTL_ID                                  AS                                  CLM_WG_SRC_DTL_ID 
		, CLM_WG_SRC_ID                                      AS                                      CLM_WG_SRC_ID 
		, upper( TRIM( CLM_WG_RPT_SRC_TYP_CD ) )             AS                              CLM_WG_RPT_SRC_TYP_CD 
		, upper( TRIM( CLM_WG_TYP_CD ) )                     AS                                      CLM_WG_TYP_CD 
		, upper( TRIM( CLM_PAY_PRD_TYP_CD ) )                AS                                 CLM_PAY_PRD_TYP_CD 
		, CLM_WG_SRC_DTL_AMT                                 AS                                 CLM_WG_SRC_DTL_AMT 
		, CLM_WG_SRC_DTL_AW_AMT                              AS                              CLM_WG_SRC_DTL_AW_AMT 
		, cast( CLM_WG_SRC_DTL_EFF_DT as DATE )              AS                              CLM_WG_SRC_DTL_EFF_DT 
		, cast( CLM_WG_SRC_DTL_END_DT as DATE )              AS                              CLM_WG_SRC_DTL_END_DT 
		, CLM_WG_SRC_DTL_PAY_PRD_UNT                         AS                         CLM_WG_SRC_DTL_PAY_PRD_UNT 
		, CLM_WG_SRC_DTL_HR_WK_NO                            AS                            CLM_WG_SRC_DTL_HR_WK_NO 
		, CLM_WG_SRC_DTL_DD_WK_NO                            AS                            CLM_WG_SRC_DTL_DD_WK_NO 
		, upper( TRIM( CLM_WG_SRC_DTL_COMT ) )               AS                                CLM_WG_SRC_DTL_COMT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CWSD
            ),
LOGIC_CWRST as ( SELECT 
		  upper( TRIM( CLM_WG_RPT_SRC_TYP_NM ) )             AS                              CLM_WG_RPT_SRC_TYP_NM 
		, upper( TRIM( CLM_WG_RPT_SRC_TYP_CD ) )             AS                              CLM_WG_RPT_SRC_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CWRST
            ),
LOGIC_CWT as ( SELECT 
		  upper( TRIM( CLM_WG_TYP_NM ) )                     AS                                      CLM_WG_TYP_NM 
		, upper( TRIM( CLM_WG_TYP_CD ) )                     AS                                      CLM_WG_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CWT
            ),
LOGIC_CPPT as ( SELECT 
		  upper( TRIM( CLM_PAY_PRD_TYP_NM ) )                AS                                 CLM_PAY_PRD_TYP_NM 
		, upper( TRIM( CLM_PAY_PRD_TYP_CD ) )                AS                                 CLM_PAY_PRD_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CPPT
            )

---- RENAME LAYER ----
,

RENAME_CWSD as ( SELECT 
		  CLM_WG_SRC_DTL_ID                                  as                                  CLM_WG_SRC_DTL_ID
		, CLM_WG_SRC_ID                                      as                                      CLM_WG_SRC_ID
		, CLM_WG_RPT_SRC_TYP_CD                              as                              CLM_WG_RPT_SRC_TYP_CD
		, CLM_WG_TYP_CD                                      as                                      CLM_WG_TYP_CD
		, CLM_PAY_PRD_TYP_CD                                 as                                 CLM_PAY_PRD_TYP_CD
		, CLM_WG_SRC_DTL_AMT                                 as                                 CLM_WG_SRC_DTL_AMT
		, CLM_WG_SRC_DTL_AW_AMT                              as                              CLM_WG_SRC_DTL_AW_AMT
		, CLM_WG_SRC_DTL_EFF_DT                              as                              CLM_WG_SRC_DTL_EFF_DT
		, CLM_WG_SRC_DTL_END_DT                              as                              CLM_WG_SRC_DTL_END_DT
		, CLM_WG_SRC_DTL_PAY_PRD_UNT                         as                         CLM_WG_SRC_DTL_PAY_PRD_UNT
		, CLM_WG_SRC_DTL_HR_WK_NO                            as                            CLM_WG_SRC_DTL_HR_WK_NO
		, CLM_WG_SRC_DTL_DD_WK_NO                            as                            CLM_WG_SRC_DTL_DD_WK_NO
		, CLM_WG_SRC_DTL_COMT                                as                                CLM_WG_SRC_DTL_COMT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CWSD   ), 
RENAME_CWRST as ( SELECT 
		  CLM_WG_RPT_SRC_TYP_NM                              as                              CLM_WG_RPT_SRC_TYP_NM
		, CLM_WG_RPT_SRC_TYP_CD                              as                        CWRST_CLM_WG_RPT_SRC_TYP_CD
		, VOID_IND                                           as                                     CWRST_VOID_IND 
				FROM     LOGIC_CWRST   ), 
RENAME_CWT as ( SELECT 
		  CLM_WG_TYP_NM                                      as                                      CLM_WG_TYP_NM
		, CLM_WG_TYP_CD                                      as                                  CWT_CLM_WG_TYP_CD
		, VOID_IND                                           as                                       CWT_VOID_IND 
				FROM     LOGIC_CWT   ), 
RENAME_CPPT as ( SELECT 
		  CLM_PAY_PRD_TYP_NM                                 as                                 CLM_PAY_PRD_TYP_NM
		, CLM_PAY_PRD_TYP_CD                                 as                            CPPT_CLM_PAY_PRD_TYP_CD
		, VOID_IND                                           as                                      CPPT_VOID_IND 
				FROM     LOGIC_CPPT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CWSD                           as ( SELECT * from    RENAME_CWSD   ),
FILTER_CWT                            as ( SELECT * from    RENAME_CWT 
				WHERE CWT_VOID_IND = 'N'  ),
FILTER_CPPT                           as ( SELECT * from    RENAME_CPPT 
				WHERE CPPT_VOID_IND = 'N'  ),
FILTER_CWRST                          as ( SELECT * from    RENAME_CWRST 
				WHERE CWRST_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CWSD as ( SELECT * 
				FROM  FILTER_CWSD
				LEFT JOIN FILTER_CWT ON  FILTER_CWSD.CLM_WG_TYP_CD =  FILTER_CWT.CWT_CLM_WG_TYP_CD 
								LEFT JOIN FILTER_CPPT ON  FILTER_CWSD.CLM_PAY_PRD_TYP_CD =  FILTER_CPPT.CPPT_CLM_PAY_PRD_TYP_CD 
								LEFT JOIN FILTER_CWRST ON  FILTER_CWSD.CLM_WG_RPT_SRC_TYP_CD =  FILTER_CWRST.CWRST_CLM_WG_RPT_SRC_TYP_CD  )
SELECT * 
from CWSD