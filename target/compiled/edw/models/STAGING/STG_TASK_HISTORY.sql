---- SRC LAYER ----
WITH
SRC_TH             as ( SELECT *     FROM     DEV_VIEWS.PCMP.TASK_HISTORY ),
SRC_TST            as ( SELECT *     FROM     DEV_VIEWS.PCMP.TASK_STATUS_TYPE ),
SRC_TPT            as ( SELECT *     FROM     DEV_VIEWS.PCMP.TASK_PRIORITY_TYPE ),
//SRC_TH             as ( SELECT *     FROM     TASK_HISTORY) ,
//SRC_TST            as ( SELECT *     FROM     TASK_STATUS_TYPE) ,
//SRC_TPT            as ( SELECT *     FROM     TASK_PRIORITY_TYPE) ,

---- LOGIC LAYER ----


LOGIC_TH as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID 
		, TASK_ID                                            as                                            TASK_ID 
		, upper( TRIM( TASK_TYP_CD ) )                       as                                        TASK_TYP_CD 
		, TASK_CTG_APP_CNTX_XREF_ID                          as                          TASK_CTG_APP_CNTX_XREF_ID 
		, upper( TRIM( TASK_STS_TYP_CD ) )                   as                                    TASK_STS_TYP_CD 
		, upper( TRIM( TASK_PR_STS_TYP_CD ) )                as                                 TASK_PR_STS_TYP_CD 
		, cast( TASK_PR_ESCL_DT as DATE )                    as                                    TASK_PR_ESCL_DT 
		, upper( TASK_ESCL_TO_SUPR_IND )                     as                              TASK_ESCL_TO_SUPR_IND 
		, upper( TRIM( TASK_NM ) )                           as                                            TASK_NM 
		, upper( TRIM( TASK_NM_DRV_UPCS_NM ) )               as                                TASK_NM_DRV_UPCS_NM 
		, upper( NULLIF (TRIM( TASK_DESC ) ,''))             as                                          TASK_DESC 
		, TASK_TMPL_ID                                       as                                       TASK_TMPL_ID 
		, TASK_DUE_DT                                        as                                        TASK_DUE_DT 
		, TASK_DT                                            as                                            TASK_DT 
		, upper( TRIM( TASK_PRTY_TYP_CD ) )                  as                                   TASK_PRTY_TYP_CD 
		, upper( TRIM( APP_CNTX_TYP_CD ) )                   as                                    APP_CNTX_TYP_CD 
		, upper( TRIM( APP_SUB_CNTX_TYP_CD ) )               as                                APP_SUB_CNTX_TYP_CD 
		, TASK_CNTX_ID                                       as                                       TASK_CNTX_ID 
		, TASK_SUB_CNTX_ID                                   as                                   TASK_SUB_CNTX_ID 
		, upper( NULLIF(TRIM( TASK_PND_RSN ),'') )           as                                       TASK_PND_RSN 
		, upper( TASK_USER_MOD_IND )                         as                                  TASK_USER_MOD_IND 
		, upper( TRIM( APP_DTL_LVL_CD ) )                    as                                     APP_DTL_LVL_CD 
		, ORG_UNT_ID                                         as                                         ORG_UNT_ID 
		, TASK_LNK_ID                                        as                                        TASK_LNK_ID 
		, USER_ID                                            as                                            USER_ID 
		, COND_ID                                            as                                            COND_ID 
		, FOLD_ID                                            as                                            FOLD_ID 
		, upper( TASK_COMT_REQD_IND )                        as                                 TASK_COMT_REQD_IND 
		, upper( NULLIF(TRIM( TASK_COMT ),'') )              as                                          TASK_COMT 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM 
		, HIST_END_DTM                                       as                                       HIST_END_DTM 
		FROM SRC_TH
            ),

LOGIC_TST as ( SELECT 
		  upper( TRIM( TASK_STS_TYP_NM ) )                   as                                    TASK_STS_TYP_NM 
		, upper( TRIM( TASK_STS_TYP_CD ) )                   as                                    TASK_STS_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		FROM SRC_TST
            ),

LOGIC_TPT as ( SELECT 
		  upper( TRIM( TASK_PRTY_TYP_NM ) )                  as                                   TASK_PRTY_TYP_NM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		, upper( TRIM( TASK_PRTY_TYP_CD ) )                  as                                   TASK_PRTY_TYP_CD 
		FROM SRC_TPT
            )

---- RENAME LAYER ----
,

RENAME_TH         as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID
		, TASK_ID                                            as                                            TASK_ID
		, TASK_TYP_CD                                        as                                        TASK_TYP_CD
		, TASK_CTG_APP_CNTX_XREF_ID                          as                          TASK_CTG_APP_CNTX_XREF_ID
		, TASK_STS_TYP_CD                                    as                                    TASK_STS_TYP_CD
		, TASK_PR_STS_TYP_CD                                 as                                 TASK_PR_STS_TYP_CD
		, TASK_PR_ESCL_DT                                    as                                    TASK_PR_ESCL_DT
		, TASK_ESCL_TO_SUPR_IND                              as                              TASK_ESCL_TO_SUPR_IND
		, TASK_NM                                            as                                            TASK_NM
		, TASK_NM_DRV_UPCS_NM                                as                                TASK_NM_DRV_UPCS_NM
		, TASK_DESC                                          as                                          TASK_DESC
		, TASK_TMPL_ID                                       as                                       TASK_TMPL_ID
		, TASK_DUE_DT                                        as                                        TASK_DUE_DT
		, TASK_DT                                            as                                            TASK_DT
		, TASK_PRTY_TYP_CD                                   as                                   TASK_PRTY_TYP_CD
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD
		, APP_SUB_CNTX_TYP_CD                                as                                APP_SUB_CNTX_TYP_CD
		, TASK_CNTX_ID                                       as                                       TASK_CNTX_ID
		, TASK_SUB_CNTX_ID                                   as                                   TASK_SUB_CNTX_ID
		, TASK_PND_RSN                                       as                                       TASK_PND_RSN
		, TASK_USER_MOD_IND                                  as                                  TASK_USER_MOD_IND
		, APP_DTL_LVL_CD                                     as                                     APP_DTL_LVL_CD
		, ORG_UNT_ID                                         as                                         ORG_UNT_ID
		, TASK_LNK_ID                                        as                                        TASK_LNK_ID
		, USER_ID                                            as                                            USER_ID
		, COND_ID                                            as                                            COND_ID
		, FOLD_ID                                            as                                            FOLD_ID
		, TASK_COMT_REQD_IND                                 as                                 TASK_COMT_REQD_IND
		, TASK_COMT                                          as                                          TASK_COMT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM
		, HIST_END_DTM                                       as                                       HIST_END_DTM 
				FROM     LOGIC_TH   ), 
RENAME_TST        as ( SELECT 
		  TASK_STS_TYP_NM                                    as                                    TASK_STS_TYP_NM
		, TASK_STS_TYP_CD                                    as                                TST_TASK_STS_TYP_CD
		, VOID_IND                                           as                                       TST_VOID_IND 
				FROM     LOGIC_TST   ), 
RENAME_TPT        as ( SELECT 
		  TASK_PRTY_TYP_NM                                   as                                   TASK_PRTY_TYP_NM
		, VOID_IND                                           as                                       TPT_VOID_IND
		, TASK_PRTY_TYP_CD                                   as                               TPT_TASK_PRTY_TYP_CD 
				FROM     LOGIC_TPT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_TH                             as ( SELECT * FROM    RENAME_TH   ),
FILTER_TST                            as ( SELECT * FROM    RENAME_TST 
                                            WHERE TST_VOID_IND = 'N'  ),
FILTER_TPT                            as ( SELECT * FROM    RENAME_TPT 
                                            WHERE TPT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

TH as ( SELECT * 
				FROM  FILTER_TH
				LEFT JOIN FILTER_TST ON  FILTER_TH.TASK_STS_TYP_CD =  FILTER_TST.TST_TASK_STS_TYP_CD 
				LEFT JOIN FILTER_TPT ON  FILTER_TH.TASK_PRTY_TYP_CD =  FILTER_TPT.TPT_TASK_PRTY_TYP_CD  )
SELECT 
		  HIST_ID
		, TASK_ID
		, TASK_TYP_CD
		, TASK_CTG_APP_CNTX_XREF_ID
		, TASK_STS_TYP_CD
		, TASK_STS_TYP_NM
		, TASK_PR_STS_TYP_CD
		, TASK_PR_ESCL_DT
		, TASK_ESCL_TO_SUPR_IND
		, TASK_NM
		, TASK_NM_DRV_UPCS_NM
		, TASK_DESC
		, TASK_TMPL_ID
		, TASK_DUE_DT
		, TASK_DT
		, TASK_PRTY_TYP_CD
		, TASK_PRTY_TYP_NM
		, APP_CNTX_TYP_CD
		, APP_SUB_CNTX_TYP_CD
		, TASK_CNTX_ID
		, TASK_SUB_CNTX_ID
		, TASK_PND_RSN
		, TASK_USER_MOD_IND
		, APP_DTL_LVL_CD
		, ORG_UNT_ID
		, TASK_LNK_ID
		, USER_ID
		, COND_ID
		, FOLD_ID
		, TASK_COMT_REQD_IND
		, TASK_COMT
		, AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM
		, VOID_IND
		, HIST_EFF_DTM
		, HIST_END_DTM 
		, NVL2(HIST_END_DTM,'N','Y') as CRNT_IND
FROM TH