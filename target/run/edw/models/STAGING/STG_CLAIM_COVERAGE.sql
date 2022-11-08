

      create or replace  table DEV_EDW.STAGING.STG_CLAIM_COVERAGE  as
      (---- SRC LAYER ----
WITH
SRC_CC             as ( SELECT *     FROM      DEV_VIEWS.PCMP.CLAIM_COVERAGE ), 
SRC_CST            as ( SELECT *     FROM      DEV_VIEWS.PCMP.CLAIM_COVERAGE_STATUS_TYPE ),
SRC_COT            as ( SELECT *     FROM      DEV_VIEWS.PCMP.CLAIM_COVERAGE_OVERRIDE_TYPE ),
//SRC_CC             as ( SELECT *     FROM     CLAIM_COVERAGE) ,
//SRC_CST            as ( SELECT *     FROM     CLAIM_COVERAGE_STATUS_TYPE) ,
//SRC_COT            as ( SELECT *     FROM     CLAIM_COVERAGE_OVERRIDE_TYPE) ,

---- LOGIC LAYER ----


LOGIC_CC as ( SELECT 
		  CLM_COV_ID                                         as                                         CLM_COV_ID 
		, CLM_PTCP_INSRD_ID                                  as                                  CLM_PTCP_INSRD_ID 
		, upper( TRIM( CLM_COV_STS_TYP_CD ) )                as                                 CLM_COV_STS_TYP_CD 
		, upper( TRIM( CLM_COV_OVRRD_TYP_CD ) )              as                               CLM_COV_OVRRD_TYP_CD 
		, upper( TRIM( PLCY_DATA_SRC_TYP_CD ) )              as                               PLCY_DATA_SRC_TYP_CD 
		, RIOCP_ID                                           as                                           RIOCP_ID 
		, RIOCPL_ID                                          as                                          RIOCPL_ID 
		, RIOCPC_ID                                          as                                          RIOCPC_ID 
		, PLCY_SUM_DTL_ID                                    as                                    PLCY_SUM_DTL_ID 
		, PSRL_ID                                            as                                            PSRL_ID 
		, PLCY_SUM_COV_ID                                    as                                    PLCY_SUM_COV_ID 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		, upper( TRIM( CLS_CD_UNAVLBL_IND ) )                as                                 CLS_CD_UNAVLBL_IND 
		FROM SRC_CC
            ),

LOGIC_CST as ( SELECT 
		  upper( TRIM( CLM_COV_STS_TYP_NM ) )                as                                 CLM_COV_STS_TYP_NM 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		, upper( TRIM( CLM_COV_STS_TYP_CD ) )                as                                 CLM_COV_STS_TYP_CD 
		FROM SRC_CST
            ),

LOGIC_COT as ( SELECT 
		  upper( TRIM( CLM_COV_OVRRD_TYP_NM ) )              as                               CLM_COV_OVRRD_TYP_NM 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		, upper( TRIM( CLM_COV_OVRRD_TYP_CD ) )              as                               CLM_COV_OVRRD_TYP_CD 
		FROM SRC_COT
            )

---- RENAME LAYER ----
,

RENAME_CC         as ( SELECT 
		  CLM_COV_ID                                         as                                         CLM_COV_ID
		, CLM_PTCP_INSRD_ID                                  as                                  CLM_PTCP_INSRD_ID
		, CLM_COV_STS_TYP_CD                                 as                                 CLM_COV_STS_TYP_CD
		, CLM_COV_OVRRD_TYP_CD                               as                               CLM_COV_OVRRD_TYP_CD
		, PLCY_DATA_SRC_TYP_CD                               as                               PLCY_DATA_SRC_TYP_CD
		, RIOCP_ID                                           as                                           RIOCP_ID
		, RIOCPL_ID                                          as                                          RIOCPL_ID
		, RIOCPC_ID                                          as                                          RIOCPC_ID
		, PLCY_SUM_DTL_ID                                    as                                    PLCY_SUM_DTL_ID
		, PSRL_ID                                            as                                            PSRL_ID
		, PLCY_SUM_COV_ID                                    as                                    PLCY_SUM_COV_ID
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND
		, CLS_CD_UNAVLBL_IND                                 as                                 CLS_CD_UNAVLBL_IND 
				FROM     LOGIC_CC   ), 
RENAME_CST        as ( SELECT 
		  CLM_COV_STS_TYP_NM                                 as                                 CLM_COV_STS_TYP_NM
		, VOID_IND                                           as                                       CST_VOID_IND
		, CLM_COV_STS_TYP_CD                                 as                             CST_CLM_COV_STS_TYP_CD 
				FROM     LOGIC_CST   ), 
RENAME_COT        as ( SELECT 
		  CLM_COV_OVRRD_TYP_NM                               as                               CLM_COV_OVRRD_TYP_NM
		, VOID_IND                                           as                                       COT_VOID_IND
		, CLM_COV_OVRRD_TYP_CD                               as                           COT_CLM_COV_OVRRD_TYP_CD 
				FROM     LOGIC_COT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CC                             as ( SELECT * FROM    RENAME_CC   ),
FILTER_CST                            as ( SELECT * FROM    RENAME_CST 
                                            WHERE CST_VOID_IND ='N'  ),
FILTER_COT                            as ( SELECT * FROM    RENAME_COT 
                                            WHERE COT_VOID_IND ='N'  ),

---- JOIN LAYER ----

CC as ( SELECT * 
				FROM  FILTER_CC
				LEFT JOIN FILTER_CST ON  FILTER_CC.CLM_COV_STS_TYP_CD =  FILTER_CST.CST_CLM_COV_STS_TYP_CD 
								LEFT JOIN FILTER_COT ON  FILTER_CC.CLM_COV_OVRRD_TYP_CD =  FILTER_COT.COT_CLM_COV_OVRRD_TYP_CD  )
SELECT * 
FROM CC
      );
    