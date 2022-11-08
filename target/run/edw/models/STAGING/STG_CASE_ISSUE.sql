

      create or replace  table DEV_EDW.STAGING.STG_CASE_ISSUE  as
      (---- SRC LAYER ----
WITH
SRC_CI as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_ISSUE ),
SRC_CIT as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_ISSUE_TYPE ),
SRC_CIST as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_ISSUE_STATUS_TYPE ),
//SRC_CI as ( SELECT *     from     CASE_ISSUE) ,
//SRC_CIT as ( SELECT *     from     CASE_ISSUE_TYPE) ,
//SRC_CIST as ( SELECT *     from     CASE_ISSUE_STATUS_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CI as ( SELECT 
		  CASE_ISS_ID                                        AS                                        CASE_ISS_ID 
		, CASE_ID                                            AS                                            CASE_ID 
		, upper( TRIM( CASE_ISS_TYP_CD ) )                   AS                                    CASE_ISS_TYP_CD 
		, upper( TRIM( CASE_ISS_STS_TYP_CD ) )               AS                                CASE_ISS_STS_TYP_CD 
		, upper( TRIM( CASE_ISS_SUM_TXT ) )                  AS                                   CASE_ISS_SUM_TXT 
		, CASE_ISS_RSOL_AMT                                  AS                                  CASE_ISS_RSOL_AMT 
		, upper( TRIM( CASE_ISS_RSOL_TRM ) )                 AS                                  CASE_ISS_RSOL_TRM 
		, upper( CASE_ISS_PRI_IND )                          AS                                   CASE_ISS_PRI_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CI
            ),
LOGIC_CIT as ( SELECT 
		  upper( TRIM( CASE_ISS_TYP_NM ) )                   AS                                    CASE_ISS_TYP_NM 
		, upper( TRIM( CASE_ISS_TYP_CD ) )                   AS                                    CASE_ISS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CIT
            ),
LOGIC_CIST as ( SELECT 
		  upper( TRIM( CASE_ISS_STS_TYP_NM ) )               AS                                CASE_ISS_STS_TYP_NM 
		, upper( TRIM( CASE_ISS_STS_TYP_CD ) )               AS                                CASE_ISS_STS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CIST
            )

---- RENAME LAYER ----
,

RENAME_CI as ( SELECT 
		  CASE_ISS_ID                                        as                                        CASE_ISS_ID
		, CASE_ID                                            as                                            CASE_ID
		, CASE_ISS_TYP_CD                                    as                                    CASE_ISS_TYP_CD
		, CASE_ISS_STS_TYP_CD                                as                                CASE_ISS_STS_TYP_CD
		, CASE_ISS_SUM_TXT                                   as                                   CASE_ISS_SUM_TXT
		, CASE_ISS_RSOL_AMT                                  as                                  CASE_ISS_RSOL_AMT
		, CASE_ISS_RSOL_TRM                                  as                                  CASE_ISS_RSOL_TRM
		, CASE_ISS_PRI_IND                                   as                                   CASE_ISS_PRI_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CI   ), 
RENAME_CIT as ( SELECT 
		  CASE_ISS_TYP_NM                                    as                                    CASE_ISS_TYP_NM
		, CASE_ISS_TYP_CD                                    as                                CIT_CASE_ISS_TYP_CD
		, VOID_IND                                           as                                       CIT_VOID_IND 
				FROM     LOGIC_CIT   ), 
RENAME_CIST as ( SELECT 
		  CASE_ISS_STS_TYP_NM                                as                                CASE_ISS_STS_TYP_NM
		, CASE_ISS_STS_TYP_CD                                as                           CIST_CASE_ISS_STS_TYP_CD
		, VOID_IND                                           as                                      CIST_VOID_IND 
				FROM     LOGIC_CIST   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CI                             as ( SELECT * from    RENAME_CI   ),
FILTER_CIT                            as ( SELECT * from    RENAME_CIT 
				WHERE CIT_VOID_IND = 'N'  ),
FILTER_CIST                           as ( SELECT * from    RENAME_CIST 
				WHERE CIST_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CI as ( SELECT * 
				FROM  FILTER_CI
				LEFT JOIN FILTER_CIT ON  FILTER_CI.CASE_ISS_TYP_CD =  FILTER_CIT.CIT_CASE_ISS_TYP_CD 
						LEFT JOIN FILTER_CIST ON  FILTER_CI.CASE_ISS_STS_TYP_CD =  FILTER_CIST.CIST_CASE_ISS_STS_TYP_CD  )
SELECT * 
from CI
      );
    