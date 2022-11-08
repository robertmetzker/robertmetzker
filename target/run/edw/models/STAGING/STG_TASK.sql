

      create or replace  table DEV_EDW.STAGING.STG_TASK  as
      (---- SRC LAYER ----
WITH
SRC_TASK as ( SELECT *     from     DEV_VIEWS.PCMP.TASK ),
SRC_TT as ( SELECT *     from     DEV_VIEWS.PCMP.TASK_TYPE ),
SRC_TCACX as ( SELECT *     from     DEV_VIEWS.PCMP.TASK_CATEGORY_APP_CNTX_XREF ),
SRC_TCT as ( SELECT *     from     DEV_VIEWS.PCMP.TASK_CATEGORY_TYPE ),
SRC_TST as ( SELECT *     from     DEV_VIEWS.PCMP.TASK_STATUS_TYPE ),
SRC_TST2 as ( SELECT *     from     DEV_VIEWS.PCMP.TASK_STATUS_TYPE ),
SRC_TPT as ( SELECT *     from     DEV_VIEWS.PCMP.TASK_PRIORITY_TYPE ),
SRC_ACT as ( SELECT *     from     DEV_VIEWS.PCMP.APPLICATION_CONTEXT_TYPE ),
SRC_ASCT as ( SELECT *     from     DEV_VIEWS.PCMP.APPLICATION_SUB_CONTEXT_TYPE ),
SRC_TEMP as ( SELECT *     from     DEV_VIEWS.PCMP.TASK_TEMPLATE ),
//SRC_TASK as ( SELECT *     from     TASK) ,
//SRC_TT as ( SELECT *     from     TASK_TYPE) ,
//SRC_TCACX as ( SELECT *     from     TASK_CATEGORY_APP_CNTX_XREF) ,
//SRC_TCT as ( SELECT *     from     TASK_CATEGORY_TYPE) ,
//SRC_TST as ( SELECT *     from     TASK_STATUS_TYPE) ,
//SRC_TST2 as ( SELECT *     from     TASK_STATUS_TYPE) ,
//SRC_TPT as ( SELECT *     from     TASK_PRIORITY_TYPE) ,
//SRC_ACT as ( SELECT *     from     APPLICATION_CONTEXT_TYPE) ,
//SRC_ASCT as ( SELECT *     from     APPLICATION_SUB_CONTEXT_TYPE) ,
//SRC_TEMP as ( SELECT *     from     TASK_TEMPLATE) ,

---- LOGIC LAYER ----

LOGIC_TASK as ( SELECT 
		  TASK_ID                                            AS                                            TASK_ID 
		, upper( TRIM( TASK_TYP_CD ) )                       AS                                        TASK_TYP_CD 
		, TASK_CTG_APP_CNTX_XREF_ID                          AS                          TASK_CTG_APP_CNTX_XREF_ID 
		, upper( TRIM( TASK_STS_TYP_CD ) )                   AS                                    TASK_STS_TYP_CD 
		, upper( TRIM( TASK_PR_STS_TYP_CD ) )                AS                                 TASK_PR_STS_TYP_CD 
		, upper( TRIM( TASK_PRTY_TYP_CD ) )                  AS                                   TASK_PRTY_TYP_CD 
		, upper( TRIM( TASK_NM ) )                           AS                                            TASK_NM 
		, upper( TRIM( TASK_NM_DRV_UPCS_NM ) )               AS                                TASK_NM_DRV_UPCS_NM 
		, upper( TRIM( TASK_DESC ) )                         AS                                          TASK_DESC 
		, cast( TASK_DUE_DT as DATE )                        AS                                        TASK_DUE_DT 
		, cast( TASK_DT as DATE )                            AS                                            TASK_DT 
		, cast( TASK_PR_ESCL_DT as DATE )                    AS                                    TASK_PR_ESCL_DT 
		, upper( TASK_ESCL_TO_SUPR_IND )                     AS                              TASK_ESCL_TO_SUPR_IND 
		, TASK_CNTX_ID                                       AS                                       TASK_CNTX_ID 
		, TASK_SUB_CNTX_ID                                   AS                                   TASK_SUB_CNTX_ID 
		, upper( TRIM( APP_CNTX_TYP_CD ) )                   AS                                    APP_CNTX_TYP_CD 
		, upper( TRIM( APP_SUB_CNTX_TYP_CD ) )               AS                                APP_SUB_CNTX_TYP_CD 
		, upper( TRIM( TASK_PND_RSN ) )                      AS                                       TASK_PND_RSN 
		, upper( TASK_USER_MOD_IND )                         AS                                  TASK_USER_MOD_IND 
		, upper( TASK_COMT_REQD_IND )                        AS                                 TASK_COMT_REQD_IND 
		, upper( TRIM( TASK_COMT ) )                         AS                                          TASK_COMT 
		, TASK_TMPL_ID                                       AS                                       TASK_TMPL_ID 
		, upper( TRIM( APP_DTL_LVL_CD ) )                    AS                                     APP_DTL_LVL_CD 
		, ORG_UNT_ID                                         AS                                         ORG_UNT_ID 
		, TASK_LNK_ID                                        AS                                        TASK_LNK_ID 
		, USER_ID                                            AS                                            USER_ID 
		, COND_ID                                            AS                                            COND_ID 
		, FOLD_ID                                            AS                                            FOLD_ID 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_TASK
            ),
LOGIC_TT as ( SELECT 
		  upper( TRIM( TASK_TYP_NM ) )                       AS                                        TASK_TYP_NM 
		, upper( TRIM( TASK_TYP_CD ) )                       AS                                        TASK_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_TT
            ),
LOGIC_TCACX as ( SELECT 
		  upper( TRIM( TASK_CTG_TYP_CD ) )                   AS                                    TASK_CTG_TYP_CD 
		, TASK_CTG_APP_CNTX_XREF_ID                          AS                          TASK_CTG_APP_CNTX_XREF_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_TCACX
            ),
LOGIC_TCT as ( SELECT 
		  upper( TRIM( TASK_CTG_TYP_NM ) )                   AS                                    TASK_CTG_TYP_NM 
		, upper( TRIM( TASK_CTG_TYP_CD ) )                   AS                                    TASK_CTG_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_TCT
            ),
LOGIC_TST as ( SELECT 
		  upper( TRIM( TASK_STS_TYP_NM ) )                   AS                                    TASK_STS_TYP_NM 
		, upper( TRIM( TASK_STS_TYP_CD ) )                   AS                                    TASK_STS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_TST
            ),
LOGIC_TST2 as ( SELECT 
		  upper( TRIM( TASK_STS_TYP_NM ) )                   AS                                    TASK_STS_TYP_NM 
		, upper( TRIM( TASK_STS_TYP_CD ) )                   AS                                    TASK_STS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_TST2
            ),
LOGIC_TPT as ( SELECT 
		  upper( TRIM( TASK_PRTY_TYP_NM ) )                  AS                                   TASK_PRTY_TYP_NM 
		, upper( TRIM( TASK_PRTY_TYP_CD ) )                  AS                                   TASK_PRTY_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_TPT
            ),
LOGIC_ACT as ( SELECT 
		  upper( TRIM( APP_CNTX_TYP_NM ) )                   AS                                    APP_CNTX_TYP_NM 
		, upper( TRIM( APP_CNTX_TYP_CD ) )                   AS                                    APP_CNTX_TYP_CD 
		, upper( APP_CNTX_TYP_VOID_IND )                     AS                              APP_CNTX_TYP_VOID_IND 
		from SRC_ACT
            ),
LOGIC_ASCT as ( SELECT 
		  upper( TRIM( APP_SUB_CNTX_TYP_NM ) )               AS                                APP_SUB_CNTX_TYP_NM 
		, upper( TRIM( APP_SUB_CNTX_TYP_CD ) )               AS                                APP_SUB_CNTX_TYP_CD 
		, upper( APP_SUB_CNTX_TYP_VOID_IND )                 AS                          APP_SUB_CNTX_TYP_VOID_IND 
		from SRC_ASCT
            ),
LOGIC_TEMP as ( SELECT 
		  upper( TRIM( TASK_TMPL_TASK_NM ) )                 AS                                  TASK_TMPL_TASK_NM 
		, upper( TRIM( TASK_TMPL_TASK_DESC ) )               AS                                TASK_TMPL_TASK_DESC 
		, TASK_TMPL_ID                                       AS                                       TASK_TMPL_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_TEMP
            )

---- RENAME LAYER ----
,

RENAME_TASK as ( SELECT 
		  TASK_ID                                            as                                            TASK_ID
		, TASK_TYP_CD                                        as                                        TASK_TYP_CD
		, TASK_CTG_APP_CNTX_XREF_ID                          as                          TASK_CTG_APP_CNTX_XREF_ID
		, TASK_STS_TYP_CD                                    as                                    TASK_STS_TYP_CD
		, TASK_PR_STS_TYP_CD                                 as                                 TASK_PR_STS_TYP_CD
		, TASK_PRTY_TYP_CD                                   as                                   TASK_PRTY_TYP_CD
		, TASK_NM                                            as                                            TASK_NM
		, TASK_NM_DRV_UPCS_NM                                as                                TASK_NM_DRV_UPCS_NM
		, TASK_DESC                                          as                                          TASK_DESC
		, TASK_DUE_DT                                        as                                        TASK_DUE_DT
		, TASK_DT                                            as                                            TASK_DT
		, TASK_PR_ESCL_DT                                    as                                    TASK_PR_ESCL_DT
		, TASK_ESCL_TO_SUPR_IND                              as                              TASK_ESCL_TO_SUPR_IND
		, TASK_CNTX_ID                                       as                                       TASK_CNTX_ID
		, TASK_SUB_CNTX_ID                                   as                                   TASK_SUB_CNTX_ID
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD
		, APP_SUB_CNTX_TYP_CD                                as                                APP_SUB_CNTX_TYP_CD
		, TASK_PND_RSN                                       as                                       TASK_PND_RSN
		, TASK_USER_MOD_IND                                  as                                  TASK_USER_MOD_IND
		, TASK_COMT_REQD_IND                                 as                                 TASK_COMT_REQD_IND
		, TASK_COMT                                          as                                          TASK_COMT
		, TASK_TMPL_ID                                       as                                       TASK_TMPL_ID
		, APP_DTL_LVL_CD                                     as                                     APP_DTL_LVL_CD
		, ORG_UNT_ID                                         as                                         ORG_UNT_ID
		, TASK_LNK_ID                                        as                                        TASK_LNK_ID
		, USER_ID                                            as                                            USER_ID
		, COND_ID                                            as                                            COND_ID
		, FOLD_ID                                            as                                            FOLD_ID
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_TASK   ), 
RENAME_TT as ( SELECT 
		  TASK_TYP_NM                                        as                                        TASK_TYP_NM
		, TASK_TYP_CD                                        as                                     TT_TASK_TYP_CD
		, VOID_IND                                           as                                        TT_VOID_IND 
				FROM     LOGIC_TT   ), 
RENAME_TCACX as ( SELECT 
		  TASK_CTG_TYP_CD                                    as                                    TASK_CTG_TYP_CD
		, TASK_CTG_APP_CNTX_XREF_ID                          as                    TCACX_TASK_CTG_APP_CNTX_XREF_ID
		, VOID_IND                                           as                                     TCACX_VOID_IND 
				FROM     LOGIC_TCACX   ), 
RENAME_TCT as ( SELECT 
		  TASK_CTG_TYP_NM                                    as                                    TASK_CTG_TYP_NM
		, TASK_CTG_TYP_CD                                    as                                TCT_TASK_CTG_TYP_CD
		, VOID_IND                                           as                                       TCT_VOID_IND 
				FROM     LOGIC_TCT   ), 
RENAME_TST as ( SELECT 
		  TASK_STS_TYP_NM                                    as                                    TASK_STS_TYP_NM
		, TASK_STS_TYP_CD                                    as                                TST_TASK_STS_TYP_CD
		, VOID_IND                                           as                                       TST_VOID_IND 
				FROM     LOGIC_TST   ), 
RENAME_TST2 as ( SELECT 
		  TASK_STS_TYP_NM                                    as                                 TASK_PR_STS_TYP_NM
		, TASK_STS_TYP_CD                                    as                               TST2_TASK_STS_TYP_CD
		, VOID_IND                                           as                                      TST2_VOID_IND 
				FROM     LOGIC_TST2   ), 
RENAME_TPT as ( SELECT 
		  TASK_PRTY_TYP_NM                                   as                                   TASK_PRTY_TYP_NM
		, TASK_PRTY_TYP_CD                                   as                               TPT_TASK_PRTY_TYP_CD
		, VOID_IND                                           as                                       TPT_VOID_IND 
				FROM     LOGIC_TPT   ), 
RENAME_ACT as ( SELECT 
		  APP_CNTX_TYP_NM                                    as                                    APP_CNTX_TYP_NM
		, APP_CNTX_TYP_CD                                    as                                ACT_APP_CNTX_TYP_CD
		, APP_CNTX_TYP_VOID_IND                              as                              APP_CNTX_TYP_VOID_IND 
				FROM     LOGIC_ACT   ), 
RENAME_ASCT as ( SELECT 
		  APP_SUB_CNTX_TYP_NM                                as                                APP_SUB_CNTX_TYP_NM
		, APP_SUB_CNTX_TYP_CD                                as                           ASCT_APP_SUB_CNTX_TYP_CD
		, APP_SUB_CNTX_TYP_VOID_IND                          as                          APP_SUB_CNTX_TYP_VOID_IND 
				FROM     LOGIC_ASCT   ), 
RENAME_TEMP as ( SELECT 
		  TASK_TMPL_TASK_NM                                  as                                  TASK_TMPL_TASK_NM
		, TASK_TMPL_TASK_DESC                                as                                TASK_TMPL_TASK_DESC
		, TASK_TMPL_ID                                       as                                  TEMP_TASK_TMPL_ID
		, VOID_IND                                           as                                      TEMP_VOID_IND 
				FROM     LOGIC_TEMP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_TASK                           as ( SELECT * from    RENAME_TASK   ),
FILTER_TEMP                           as ( SELECT * from    RENAME_TEMP 
				WHERE TEMP_VOID_IND = 'N'  ),
FILTER_TT                             as ( SELECT * from    RENAME_TT 
				WHERE TT_VOID_IND = 'N'  ),
FILTER_TST                            as ( SELECT * from    RENAME_TST 
				WHERE TST_VOID_IND = 'N'  ),
FILTER_TST2                           as ( SELECT * from    RENAME_TST2 
				WHERE TST2_VOID_IND = 'N'  ),
FILTER_TPT                            as ( SELECT * from    RENAME_TPT 
				WHERE TPT_VOID_IND = 'N'  ),
FILTER_ACT                            as ( SELECT * from    RENAME_ACT 
				WHERE APP_CNTX_TYP_VOID_IND = 'N'  ),
FILTER_ASCT                           as ( SELECT * from    RENAME_ASCT 
				WHERE APP_SUB_CNTX_TYP_VOID_IND = 'N'  ),
FILTER_TCACX                          as ( SELECT * from    RENAME_TCACX 
				WHERE TCACX_VOID_IND = 'N'  ),
FILTER_TCT                            as ( SELECT * from    RENAME_TCT 
				WHERE TCT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

TCACX as ( SELECT * 
				FROM  FILTER_TCACX
				LEFT JOIN FILTER_TCT ON  FILTER_TCACX.TASK_CTG_TYP_CD =  FILTER_TCT.TCT_TASK_CTG_TYP_CD  ),
TASK as ( SELECT * 
				FROM  FILTER_TASK
				LEFT JOIN FILTER_TEMP ON  FILTER_TASK.TASK_TMPL_ID =  FILTER_TEMP.TEMP_TASK_TMPL_ID 
								LEFT JOIN FILTER_TT ON  FILTER_TASK.TASK_TYP_CD =  FILTER_TT.TT_TASK_TYP_CD 
								LEFT JOIN FILTER_TST ON  FILTER_TASK.TASK_STS_TYP_CD =  FILTER_TST.TST_TASK_STS_TYP_CD 
								LEFT JOIN FILTER_TST2 ON  FILTER_TASK.TASK_PR_STS_TYP_CD =  FILTER_TST2.TST2_TASK_STS_TYP_CD 
								LEFT JOIN FILTER_TPT ON  FILTER_TASK.TASK_PRTY_TYP_CD =  FILTER_TPT.TPT_TASK_PRTY_TYP_CD 
								LEFT JOIN FILTER_ACT ON  FILTER_TASK.APP_CNTX_TYP_CD =  FILTER_ACT.ACT_APP_CNTX_TYP_CD 
								LEFT JOIN FILTER_ASCT ON  FILTER_TASK.APP_SUB_CNTX_TYP_CD =  FILTER_ASCT.ASCT_APP_SUB_CNTX_TYP_CD 
						LEFT JOIN TCACX ON  FILTER_TASK.TASK_CTG_APP_CNTX_XREF_ID = TCACX.TCACX_TASK_CTG_APP_CNTX_XREF_ID  )
SELECT * 
from TASK
      );
    