

      create or replace  table DEV_EDW.STAGING.STG_CASE_SERVICE  as
      (---- SRC LAYER ----
WITH
SRC_CS as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_SERVICE ),
SRC_CST as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_SERVICE_TYPE ),
SRC_CSST as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_SERVICE_STATUS_TYPE ),
SRC_CSD as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_SERVICE_DETAIL ),
//SRC_CS as ( SELECT *     from     CASE_SERVICE) ,
//SRC_CST as ( SELECT *     from     CASE_SERVICE_TYPE) ,
//SRC_CSST as ( SELECT *     from     CASE_SERVICE_STATUS_TYPE) ,
//SRC_CSD as ( SELECT *     from     CASE_SERVICE_DETAIL) ,

---- LOGIC LAYER ----

LOGIC_CS as ( SELECT 
		  CASE_SERV_ID                                       AS                                       CASE_SERV_ID 
		, CASE_ID                                            AS                                            CASE_ID 
		, CASE_PTCP_ID                                       AS                                       CASE_PTCP_ID 
		, upper( TRIM( CASE_SERV_TYP_CD ) )                  AS                                   CASE_SERV_TYP_CD 
		, upper( TRIM( CASE_SERV_STS_TYP_CD ) )              AS                               CASE_SERV_STS_TYP_CD 
		, upper( TRIM( CASE_SERV_AUTH_NO ) )                 AS                                  CASE_SERV_AUTH_NO 
		, cast( CASE_SERV_AUTH_DT as DATE )                  AS                                  CASE_SERV_AUTH_DT 
		, CASE_SERV_APRV_NO_OF_UNT                           AS                           CASE_SERV_APRV_NO_OF_UNT 
		, CASE_SERV_APRV_AMT                                 AS                                 CASE_SERV_APRV_AMT 
		, CASE_SERV_DNY_NO_OF_UNT                            AS                            CASE_SERV_DNY_NO_OF_UNT 
		, CASE_SERV_DNY_AMT                                  AS                                  CASE_SERV_DNY_AMT 
		, upper( TRIM( CASE_SERV_REQS_COMT_TXT ) )           AS                            CASE_SERV_REQS_COMT_TXT 
		, upper( TRIM( CASE_SERV_RECM_COMT_TXT ) )           AS                            CASE_SERV_RECM_COMT_TXT 
		, upper( TRIM( CASE_SERV_APRV_COMT_TXT ) )           AS                            CASE_SERV_APRV_COMT_TXT 
		, cast( CASE_SERV_APRV_FR_DT as DATE )               AS                               CASE_SERV_APRV_FR_DT 
		, cast( CASE_SERV_APRV_TO_DT as DATE )               AS                               CASE_SERV_APRV_TO_DT 
		, CASE_SERV_UNT_PD                                   AS                                   CASE_SERV_UNT_PD 
		, CASE_SERV_AMT_PD                                   AS                                   CASE_SERV_AMT_PD 
		, upper( TRIM( CASE_RECORD_SOURCE_TYP_CD ) )         AS                          CASE_RECORD_SOURCE_TYP_CD 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CS
            ),
LOGIC_CST as ( SELECT 
		  upper( TRIM( CASE_SERV_TYP_NM ) )                  AS                                   CASE_SERV_TYP_NM 
		, upper( TRIM( CASE_SERV_TYP_CD ) )                  AS                                   CASE_SERV_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CST
            ),
LOGIC_CSST as ( SELECT 
		  upper( TRIM( CASE_SERV_STS_TYP_NM ) )              AS                               CASE_SERV_STS_TYP_NM 
		, upper( TRIM( CASE_SERV_STS_TYP_CD ) )              AS                               CASE_SERV_STS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CSST
            ),
LOGIC_CSD as ( SELECT 
		  upper( TRIM( CSD_CD_FR ) )                         AS                                          CSD_CD_FR 
		, upper( TRIM( CSD_CD_TO ) )                         AS                                          CSD_CD_TO 
		, upper( TRIM( CSD_FULL_DESC_DRV ) )                 AS                                  CSD_FULL_DESC_DRV 
		, CASE_SERV_ID                                       AS                                       CASE_SERV_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CSD
            )

---- RENAME LAYER ----
,

RENAME_CS as ( SELECT 
		  CASE_SERV_ID                                       as                                       CASE_SERV_ID
		, CASE_ID                                            as                                            CASE_ID
		, CASE_PTCP_ID                                       as                                       CASE_PTCP_ID
		, CASE_SERV_TYP_CD                                   as                                   CASE_SERV_TYP_CD
		, CASE_SERV_STS_TYP_CD                               as                               CASE_SERV_STS_TYP_CD
		, CASE_SERV_AUTH_NO                                  as                                  CASE_SERV_AUTH_NO
		, CASE_SERV_AUTH_DT                                  as                                  CASE_SERV_AUTH_DT
		, CASE_SERV_APRV_NO_OF_UNT                           as                           CASE_SERV_APRV_NO_OF_UNT
		, CASE_SERV_APRV_AMT                                 as                                 CASE_SERV_APRV_AMT
		, CASE_SERV_DNY_NO_OF_UNT                            as                            CASE_SERV_DNY_NO_OF_UNT
		, CASE_SERV_DNY_AMT                                  as                                  CASE_SERV_DNY_AMT
		, CASE_SERV_REQS_COMT_TXT                            as                            CASE_SERV_REQS_COMT_TXT
		, CASE_SERV_RECM_COMT_TXT                            as                            CASE_SERV_RECM_COMT_TXT
		, CASE_SERV_APRV_COMT_TXT                            as                            CASE_SERV_APRV_COMT_TXT
		, CASE_SERV_APRV_FR_DT                               as                               CASE_SERV_APRV_FR_DT
		, CASE_SERV_APRV_TO_DT                               as                               CASE_SERV_APRV_TO_DT
		, CASE_SERV_UNT_PD                                   as                                   CASE_SERV_UNT_PD
		, CASE_SERV_AMT_PD                                   as                                   CASE_SERV_AMT_PD
		, CASE_RECORD_SOURCE_TYP_CD                          as                          CASE_RECORD_SOURCE_TYP_CD
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CS   ), 
RENAME_CST as ( SELECT 
		  CASE_SERV_TYP_NM                                   as                                   CASE_SERV_TYP_NM
		, CASE_SERV_TYP_CD                                   as                               CST_CASE_SERV_TYP_CD
		, VOID_IND                                           as                                       CST_VOID_IND 
				FROM     LOGIC_CST   ), 
RENAME_CSST as ( SELECT 
		  CASE_SERV_STS_TYP_NM                               as                               CASE_SERV_STS_TYP_NM
		, CASE_SERV_STS_TYP_CD                               as                          CSST_CASE_SERV_STS_TYP_CD
		, VOID_IND                                           as                                      CSST_VOID_IND 
				FROM     LOGIC_CSST   ), 
RENAME_CSD as ( SELECT 
		  CSD_CD_FR                                          as                                          CSD_CD_FR
		, CSD_CD_TO                                          as                                          CSD_CD_TO
		, CSD_FULL_DESC_DRV                                  as                                  CSD_FULL_DESC_DRV
		, CASE_SERV_ID                                       as                                   CSD_CASE_SERV_ID
		, VOID_IND                                           as                                       CSD_VOID_IND 
				FROM     LOGIC_CSD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CS                             as ( SELECT * from    RENAME_CS   ),
FILTER_CST                            as ( SELECT * from    RENAME_CST 
				WHERE CST_VOID_IND = 'N'  ),
FILTER_CSST                           as ( SELECT * from    RENAME_CSST 
				WHERE CSST_VOID_IND = 'N'  ),
FILTER_CSD                            as ( SELECT * from    RENAME_CSD 
				WHERE CSD_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CS as ( SELECT * 
				FROM  FILTER_CS
				LEFT JOIN FILTER_CST ON  FILTER_CS.CASE_SERV_TYP_CD =  FILTER_CST.CST_CASE_SERV_TYP_CD 
						LEFT JOIN FILTER_CSST ON  FILTER_CS.CASE_SERV_STS_TYP_CD =  FILTER_CSST.CSST_CASE_SERV_STS_TYP_CD 
						LEFT JOIN FILTER_CSD ON  FILTER_CS.CASE_SERV_ID =  FILTER_CSD.CSD_CASE_SERV_ID  )
SELECT * 
from CS
      );
    