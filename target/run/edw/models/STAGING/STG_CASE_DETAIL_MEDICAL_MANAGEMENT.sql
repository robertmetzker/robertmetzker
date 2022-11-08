

      create or replace  table DEV_EDW.STAGING.STG_CASE_DETAIL_MEDICAL_MANAGEMENT  as
      (---- SRC LAYER ----
WITH
SRC_CDMM as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_DETAIL_MEDICAL_MANAGEMENT ),
SRC_CMMERT as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_MED_MANG_EXTRNL_RVW_TYP ),
//SRC_CDMM as ( SELECT *     from     CASE_DETAIL_MEDICAL_MANAGEMENT) ,
//SRC_CMMERT as ( SELECT *     from     CASE_MED_MANG_EXTRNL_RVW_TYP) ,

---- LOGIC LAYER ----

LOGIC_CDMM as ( SELECT 
		  CDMM_ID                                            AS                                            CDMM_ID 
		, CASE_ID                                            AS                                            CASE_ID 
		, upper( TRIM( CMMERT_CD ) )                         AS                                          CMMERT_CD 
		, upper( TRIM( CDMM_EXTRNL_RVW_COMT ) )              AS                               CDMM_EXTRNL_RVW_COMT 
		, cast( CDMM_RVW_REQS_DT as DATE )                   AS                                   CDMM_RVW_REQS_DT 
		, upper( TRIM( CDMM_RVW_REQS_BY_NM ) )               AS                                CDMM_RVW_REQS_BY_NM 
		, upper( CDMM_RVW_OUT_OF_STT_IND )                   AS                            CDMM_RVW_OUT_OF_STT_IND 
		, upper( TRIM( CDMM_PLN_PROC_DESC ) )                AS                                 CDMM_PLN_PROC_DESC 
		, cast( CDMM_PLN_PROC_DT as DATE )                   AS                                   CDMM_PLN_PROC_DT 
		, upper( TRIM( CDMM_DIAG_DESC ) )                    AS                                     CDMM_DIAG_DESC 
		, upper( TRIM( CDMM_PR_SRGY_DESC ) )                 AS                                  CDMM_PR_SRGY_DESC 
		, upper( TRIM( CDMM_PR_APPLCBL_SRGY_DESC ) )         AS                          CDMM_PR_APPLCBL_SRGY_DESC 
		, upper( TRIM( CDMM_HM_HLT_DME_DESC ) )              AS                               CDMM_HM_HLT_DME_DESC 
		, upper( CDMM_CLMT_HAS_ATTY_IND )                    AS                             CDMM_CLMT_HAS_ATTY_IND 
		, upper( CDMM_CAR_LGL_INVD_IN_CLM_IND )              AS                       CDMM_CAR_LGL_INVD_IN_CLM_IND 
		, upper( TRIM( CDMM_CAR_LGL_COMT ) )                 AS                                  CDMM_CAR_LGL_COMT 
		, upper( CDMM_CLM_LGL_DCSN_EXST_IND )                AS                         CDMM_CLM_LGL_DCSN_EXST_IND 
		, upper( TRIM( CDMM_CLM_LGL_DCSN_COMT ) )            AS                             CDMM_CLM_LGL_DCSN_COMT 
		, upper( TRIM( CDMM_RTNLE_FOR_DCSN_TXT ) )           AS                            CDMM_RTNLE_FOR_DCSN_TXT 
		, upper( TRIM( CDMM_UR_RN_CAR_MED_DIR_COMT ) )       AS                        CDMM_UR_RN_CAR_MED_DIR_COMT 
		, upper( CDMM_DCSN_APL_IND )                         AS                                  CDMM_DCSN_APL_IND 
		, upper( CDMM_DUP_REQS_IND )                         AS                                  CDMM_DUP_REQS_IND 
		, upper( CDMM_EXTRNL_RVW_AMND_DCSN_IND )             AS                      CDMM_EXTRNL_RVW_AMND_DCSN_IND 
		, CDMM_CERT_NO_OF_VIST                               AS                               CDMM_CERT_NO_OF_VIST 
		, upper( TRIM( CDMM_CERT_DUR_OF_TIME_DESC ) )        AS                         CDMM_CERT_DUR_OF_TIME_DESC 
		, upper( TRIM( CDMM_CERT_EXT_OF_TIME_DESC ) )        AS                         CDMM_CERT_EXT_OF_TIME_DESC 
		, upper( CDMM_CERT_OTPTNT_SERV_IND )                 AS                          CDMM_CERT_OTPTNT_SERV_IND 
		, CDMM_AUTH_NO_OF_MINT                               AS                               CDMM_AUTH_NO_OF_MINT 
		, upper( TRIM( CDMM_AUTH_SPNL_RGN_TXT ) )            AS                             CDMM_AUTH_SPNL_RGN_TXT 
		, upper( TRIM( CDMM_AUTH_SPNL_RGN_TYP_DESC ) )       AS                        CDMM_AUTH_SPNL_RGN_TYP_DESC 
		, upper( TRIM( CDMM_NON_CERT_DESC ) )                AS                                 CDMM_NON_CERT_DESC 
		, upper( TRIM( CDMM_NON_CERT_RTNLE_DESC ) )          AS                           CDMM_NON_CERT_RTNLE_DESC 
		, upper( CDMM_RTN_IND )                              AS                                       CDMM_RTN_IND 
		, CDMM_SENT_DTM                                      AS                                      CDMM_SENT_DTM 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CDMM
            ),
LOGIC_CMMERT as ( SELECT 
		  upper( TRIM( CMMERT_NM ) )                         AS                                          CMMERT_NM 
		, upper( TRIM( CMMERT_CD ) )                         AS                                          CMMERT_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CMMERT
            )

---- RENAME LAYER ----
,

RENAME_CDMM as ( SELECT 
		  CDMM_ID                                            as                                            CDMM_ID
		, CASE_ID                                            as                                            CASE_ID
		, CMMERT_CD                                          as                                          CMMERT_CD
		, CDMM_EXTRNL_RVW_COMT                               as                               CDMM_EXTRNL_RVW_COMT
		, CDMM_RVW_REQS_DT                                   as                                   CDMM_RVW_REQS_DT
		, CDMM_RVW_REQS_BY_NM                                as                                CDMM_RVW_REQS_BY_NM
		, CDMM_RVW_OUT_OF_STT_IND                            as                            CDMM_RVW_OUT_OF_STT_IND
		, CDMM_PLN_PROC_DESC                                 as                                 CDMM_PLN_PROC_DESC
		, CDMM_PLN_PROC_DT                                   as                                   CDMM_PLN_PROC_DT
		, CDMM_DIAG_DESC                                     as                                     CDMM_DIAG_DESC
		, CDMM_PR_SRGY_DESC                                  as                                  CDMM_PR_SRGY_DESC
		, CDMM_PR_APPLCBL_SRGY_DESC                          as                          CDMM_PR_APPLCBL_SRGY_DESC
		, CDMM_HM_HLT_DME_DESC                               as                               CDMM_HM_HLT_DME_DESC
		, CDMM_CLMT_HAS_ATTY_IND                             as                             CDMM_CLMT_HAS_ATTY_IND
		, CDMM_CAR_LGL_INVD_IN_CLM_IND                       as                       CDMM_CAR_LGL_INVD_IN_CLM_IND
		, CDMM_CAR_LGL_COMT                                  as                                  CDMM_CAR_LGL_COMT
		, CDMM_CLM_LGL_DCSN_EXST_IND                         as                         CDMM_CLM_LGL_DCSN_EXST_IND
		, CDMM_CLM_LGL_DCSN_COMT                             as                             CDMM_CLM_LGL_DCSN_COMT
		, CDMM_RTNLE_FOR_DCSN_TXT                            as                            CDMM_RTNLE_FOR_DCSN_TXT
		, CDMM_UR_RN_CAR_MED_DIR_COMT                        as                        CDMM_UR_RN_CAR_MED_DIR_COMT
		, CDMM_DCSN_APL_IND                                  as                                  CDMM_DCSN_APL_IND
		, CDMM_DUP_REQS_IND                                  as                                  CDMM_DUP_REQS_IND
		, CDMM_EXTRNL_RVW_AMND_DCSN_IND                      as                      CDMM_EXTRNL_RVW_AMND_DCSN_IND
		, CDMM_CERT_NO_OF_VIST                               as                               CDMM_CERT_NO_OF_VIST
		, CDMM_CERT_DUR_OF_TIME_DESC                         as                         CDMM_CERT_DUR_OF_TIME_DESC
		, CDMM_CERT_EXT_OF_TIME_DESC                         as                         CDMM_CERT_EXT_OF_TIME_DESC
		, CDMM_CERT_OTPTNT_SERV_IND                          as                          CDMM_CERT_OTPTNT_SERV_IND
		, CDMM_AUTH_NO_OF_MINT                               as                               CDMM_AUTH_NO_OF_MINT
		, CDMM_AUTH_SPNL_RGN_TXT                             as                             CDMM_AUTH_SPNL_RGN_TXT
		, CDMM_AUTH_SPNL_RGN_TYP_DESC                        as                        CDMM_AUTH_SPNL_RGN_TYP_DESC
		, CDMM_NON_CERT_DESC                                 as                                 CDMM_NON_CERT_DESC
		, CDMM_NON_CERT_RTNLE_DESC                           as                           CDMM_NON_CERT_RTNLE_DESC
		, CDMM_RTN_IND                                       as                                       CDMM_RTN_IND
		, CDMM_SENT_DTM                                      as                                      CDMM_SENT_DTM
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CDMM   ), 
RENAME_CMMERT as ( SELECT 
		  CMMERT_NM                                          as                                          CMMERT_NM
		, CMMERT_CD                                          as                                   CMMERT_CMMERT_CD
		, VOID_IND                                           as                                    CMMERT_VOID_IND 
				FROM     LOGIC_CMMERT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CDMM                           as ( SELECT * from    RENAME_CDMM   ),
FILTER_CMMERT                         as ( SELECT * from    RENAME_CMMERT 
				WHERE CMMERT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CDMM as ( SELECT * 
				FROM  FILTER_CDMM
				LEFT JOIN FILTER_CMMERT ON  FILTER_CDMM.CMMERT_CD =  FILTER_CMMERT.CMMERT_CMMERT_CD  )
SELECT * 
from CDMM
      );
    