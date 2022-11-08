

      create or replace  table DEV_EDW.STAGING.STG_ASSIGNMENT  as
      (---- SRC LAYER ----
WITH
SRC_A as ( SELECT *     from     DEV_VIEWS.PCMP.ASSIGNMENT ),
SRC_ACT as ( SELECT *     from     DEV_VIEWS.PCMP.APPLICATION_CONTEXT_TYPE ),
SRC_AST as ( SELECT *     from     DEV_VIEWS.PCMP.ASSIGNMENT_STATUS_TYPE ),
SRC_ALB as ( SELECT *     from     DEV_VIEWS.PCMP.ASSIGNMENT_LOAD_BAL_RL_TYP ),
SRC_FR as ( SELECT *     from     DEV_VIEWS.PCMP.FUNCTIONAL_ROLE ),
SRC_OU as ( SELECT *     from     DEV_VIEWS.PCMP.ORGANIZATIONAL_UNIT ),
//SRC_A as ( SELECT *     from     ASSIGNMENT) ,
//SRC_ACT as ( SELECT *     from     APPLICATION_CONTEXT_TYPE) ,
//SRC_AST as ( SELECT *     from     ASSIGNMENT_STATUS_TYPE) ,
//SRC_ALB as ( SELECT *     from     ASSIGNMENT_LOAD_BAL_RL_TYP) ,
//SRC_FR as ( SELECT *     from     FUNCTIONAL_ROLE) ,
//SRC_OU as ( SELECT *     from     ORGANIZATIONAL_UNIT) ,

---- LOGIC LAYER ----

LOGIC_A as ( SELECT 
		  ASGN_ID                                            AS                                            ASGN_ID 
		, upper( TRIM( APP_CNTX_TYP_CD ) )                   AS                                    APP_CNTX_TYP_CD 
		, ASGN_CNTX_ID                                       AS                                       ASGN_CNTX_ID 
		, upper( TRIM( ASGN_STS_TYP_CD ) )                   AS                                    ASGN_STS_TYP_CD 
		, upper( TRIM( ASGN_LD_BAL_RL_TYP_CD ) )             AS                              ASGN_LD_BAL_RL_TYP_CD 
		, upper( ASGN_PRI_OWNR_IND )                         AS                                  ASGN_PRI_OWNR_IND 
		, USER_ID                                            AS                                            USER_ID 
		, FNCT_ROLE_ID                                       AS                                       FNCT_ROLE_ID 
		, ORG_UNT_ID                                         AS                                         ORG_UNT_ID 
		, cast( ASGN_EFF_DT as DATE )                        AS                                        ASGN_EFF_DT 
		, cast( ASGN_END_DT as DATE )                        AS                                        ASGN_END_DT 
		, upper( ASGN_OVRRD_IND )                            AS                                     ASGN_OVRRD_IND 
		, upper( TRIM( ASGN_OVRRD_COMT ) )                   AS                                    ASGN_OVRRD_COMT 
		, upper( ASGN_DFLT_IND )                             AS                                      ASGN_DFLT_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM
				from SRC_A
            ),
LOGIC_ACT as ( SELECT 
		  upper( TRIM( APP_CNTX_TYP_NM ) )                   AS                                    APP_CNTX_TYP_NM 
		, upper( TRIM( APP_CNTX_TYP_CD ) )                   AS                                    APP_CNTX_TYP_CD 
		, upper( APP_CNTX_TYP_VOID_IND )                     AS                              APP_CNTX_TYP_VOID_IND 
		from SRC_ACT
            ),
LOGIC_AST as ( SELECT 
		  upper( TRIM( ASGN_STS_TYP_NM ) )                   AS                                    ASGN_STS_TYP_NM 
		, upper( TRIM( ASGN_STS_TYP_CD ) )                   AS                                    ASGN_STS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_AST
            ),
LOGIC_ALB as ( SELECT 
		  upper( TRIM( ASGN_LD_BAL_RL_TYP_NM ) )             AS                              ASGN_LD_BAL_RL_TYP_NM 
		, upper( TRIM( ASGN_LD_BAL_RL_TYP_CD ) )             AS                              ASGN_LD_BAL_RL_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_ALB
            ),
LOGIC_FR as ( SELECT 
		  upper( TRIM( FNCT_ROLE_NM ) )                      AS                                       FNCT_ROLE_NM 
		, cast( FNCT_ROLE_EFF_DT as DATE )                   AS                                   FNCT_ROLE_EFF_DT 
		, cast( FNCT_ROLE_END_DT as DATE )                   AS                                   FNCT_ROLE_END_DT 
		, FNCT_ROLE_ID                                       AS                                       FNCT_ROLE_ID 
		, upper( FNCT_ROLE_VOID_IND )                        AS                                 FNCT_ROLE_VOID_IND 
		from SRC_FR
            ),
LOGIC_OU as ( SELECT 
		  upper( TRIM( ORG_UNT_ABRV_NM ) )                   AS                                    ORG_UNT_ABRV_NM 
		, upper( TRIM( ORG_UNT_NM ) )                        AS                                         ORG_UNT_NM 
		, cast( ORG_UNT_EFF_DT as DATE )                     AS                                     ORG_UNT_EFF_DT 
		, cast( ORG_UNT_END_DT as DATE )                     AS                                     ORG_UNT_END_DT 
		, ORG_UNT_ID                                         AS                                         ORG_UNT_ID 
		, upper( ORG_UNT_VOID_IND )                          AS                                   ORG_UNT_VOID_IND 
		from SRC_OU
            )

---- RENAME LAYER ----
,

RENAME_A as ( SELECT 
		  ASGN_ID                                            as                                            ASGN_ID
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD
		, ASGN_CNTX_ID                                       as                                       ASGN_CNTX_ID
		, ASGN_STS_TYP_CD                                    as                                    ASGN_STS_TYP_CD
		, ASGN_LD_BAL_RL_TYP_CD                              as                              ASGN_LD_BAL_RL_TYP_CD
		, ASGN_PRI_OWNR_IND                                  as                                  ASGN_PRI_OWNR_IND
		, USER_ID                                            as                                            USER_ID
		, FNCT_ROLE_ID                                       as                                       FNCT_ROLE_ID
		, ORG_UNT_ID                                         as                                         ORG_UNT_ID
		, ASGN_EFF_DT                                        as                                        ASGN_EFF_DT
		, ASGN_END_DT                                        as                                        ASGN_END_DT
		, ASGN_OVRRD_IND                                     as                                     ASGN_OVRRD_IND
		, ASGN_OVRRD_COMT                                    as                                    ASGN_OVRRD_COMT
		, ASGN_DFLT_IND                                      as                                      ASGN_DFLT_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
						FROM     LOGIC_A   ), 
RENAME_ACT as ( SELECT 
		  APP_CNTX_TYP_NM                                    as                                    APP_CNTX_TYP_NM
		, APP_CNTX_TYP_CD                                    as                                ACT_APP_CNTX_TYP_CD
		, APP_CNTX_TYP_VOID_IND                              as                              APP_CNTX_TYP_VOID_IND 
				FROM     LOGIC_ACT   ), 
RENAME_AST as ( SELECT 
		  ASGN_STS_TYP_NM                                    as                                    ASGN_STS_TYP_NM
		, ASGN_STS_TYP_CD                                    as                                AST_ASGN_STS_TYP_CD
		, VOID_IND                                           as                                       AST_VOID_IND 
				FROM     LOGIC_AST   ), 
RENAME_ALB as ( SELECT 
		  ASGN_LD_BAL_RL_TYP_NM                              as                              ASGN_LD_BAL_RL_TYP_NM
		, ASGN_LD_BAL_RL_TYP_CD                              as                          ALB_ASGN_LD_BAL_RL_TYP_CD
		, VOID_IND                                           as                                       ALB_VOID_IND 
				FROM     LOGIC_ALB   ), 
RENAME_FR as ( SELECT 
		  FNCT_ROLE_NM                                       as                                       FNCT_ROLE_NM
		, FNCT_ROLE_EFF_DT                                   as                                   FNCT_ROLE_EFF_DT
		, FNCT_ROLE_END_DT                                   as                                   FNCT_ROLE_END_DT
		, FNCT_ROLE_ID                                       as                                    FR_FNCT_ROLE_ID
		, FNCT_ROLE_VOID_IND                                 as                                 FNCT_ROLE_VOID_IND 
				FROM     LOGIC_FR   ), 
RENAME_OU as ( SELECT 
		  ORG_UNT_ABRV_NM                                    as                                    ORG_UNT_ABRV_NM
		, ORG_UNT_NM                                         as                                         ORG_UNT_NM
		, ORG_UNT_EFF_DT                                     as                                     ORG_UNT_EFF_DT
		, ORG_UNT_END_DT                                     as                                     ORG_UNT_END_DT
		, ORG_UNT_ID                                         as                                      OU_ORG_UNT_ID
		, ORG_UNT_VOID_IND                                   as                                   ORG_UNT_VOID_IND 
				FROM     LOGIC_OU   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_A                              as ( SELECT * from    RENAME_A   ),
FILTER_ACT                            as ( SELECT * from    RENAME_ACT 
				WHERE APP_CNTX_TYP_VOID_IND = 'N'  ),
FILTER_AST                            as ( SELECT * from    RENAME_AST 
				WHERE AST_VOID_IND = 'N'  ),
FILTER_ALB                            as ( SELECT * from    RENAME_ALB 
				WHERE ALB_VOID_IND = 'N'  ),
FILTER_FR                             as ( SELECT * from    RENAME_FR 
				WHERE FNCT_ROLE_VOID_IND = 'N'  ),
FILTER_OU                             as ( SELECT * from    RENAME_OU 
				WHERE ORG_UNT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

A as ( SELECT * 
				FROM  FILTER_A
				LEFT JOIN FILTER_ACT ON  FILTER_A.APP_CNTX_TYP_CD =  FILTER_ACT.ACT_APP_CNTX_TYP_CD 
								LEFT JOIN FILTER_AST ON  FILTER_A.ASGN_STS_TYP_CD =  FILTER_AST.AST_ASGN_STS_TYP_CD 
								LEFT JOIN FILTER_ALB ON  FILTER_A.ASGN_LD_BAL_RL_TYP_CD =  FILTER_ALB.ALB_ASGN_LD_BAL_RL_TYP_CD 
								LEFT JOIN FILTER_FR ON  FILTER_A.FNCT_ROLE_ID =  FILTER_FR.FR_FNCT_ROLE_ID 
								LEFT JOIN FILTER_OU ON  FILTER_A.ORG_UNT_ID =  FILTER_OU.OU_ORG_UNT_ID )						
SELECT 
 ASGN_ID
,APP_CNTX_TYP_CD
,APP_CNTX_TYP_NM
,ASGN_CNTX_ID
,ASGN_STS_TYP_CD
,ASGN_STS_TYP_NM
,ASGN_LD_BAL_RL_TYP_CD
,ASGN_LD_BAL_RL_TYP_NM
,ASGN_PRI_OWNR_IND
,USER_ID
,FNCT_ROLE_ID
,FNCT_ROLE_NM
,FNCT_ROLE_EFF_DT
,FNCT_ROLE_END_DT
,FR_FNCT_ROLE_ID
,FNCT_ROLE_VOID_IND 
,ORG_UNT_ID
,ORG_UNT_ABRV_NM
,ORG_UNT_NM
,ORG_UNT_EFF_DT
,ORG_UNT_END_DT
,ASGN_EFF_DT
,ASGN_END_DT
,ASGN_OVRRD_IND
,ASGN_OVRRD_COMT
,ASGN_DFLT_IND
,AUDIT_USER_ID_CREA
,AUDIT_USER_CREA_DTM
,AUDIT_USER_ID_UPDT
,AUDIT_USER_UPDT_DTM 
, CASE WHEN  upper( ASGN_PRI_OWNR_IND )  = 'Y' 
            AND (ROW_NUMBER()OVER(PARTITION BY APP_CNTX_TYP_CD,ASGN_CNTX_ID, ASGN_PRI_OWNR_IND ORDER BY COALESCE(ASGN_END_DT::DATE, '12/31/2099') DESC, AUDIT_USER_CREA_DTM DESC, ASGN_ID DESC)) = 1
            THEN 'Y' ELSE 'N' END AS DRVD_PRI_OWNR_IND 
from A
      );
    