---- SRC LAYER ----
WITH
SRC_PPAD as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD_AUDIT_DETAIL ),
SRC_BPPAD as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_POLICY_PERIOD_AUDIT_DETAIL ),
SRC_PAT as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_AUDIT_TYPE ),
SRC_PAST as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_AUDIT_SOURCE_TYPE ),
SRC_PAU as ( SELECT *     from     DEV_VIEWS.PCMP.PLCY_AUDT_UNDRWT_ALRT_STS_TYP ),
SRC_APT as ( SELECT *     from     DEV_VIEWS.PCMP.AUDIT_PROCESS_TYPE ),
//SRC_PPAD as ( SELECT *     from     POLICY_PERIOD_AUDIT_DETAIL) ,
//SRC_BPPAD as ( SELECT *     from     BWC_POLICY_PERIOD_AUDIT_DETAIL) ,
//SRC_PAT as ( SELECT *     from     POLICY_AUDIT_TYPE) ,
//SRC_PAST as ( SELECT *     from     POLICY_AUDIT_SOURCE_TYPE) ,
//SRC_PAU as ( SELECT *     from     PLCY_AUDT_UNDRWT_ALRT_STS_TYP) ,
//SRC_APT as ( SELECT *     from     AUDIT_PROCESS_TYPE) ,

---- LOGIC LAYER ----

LOGIC_PPAD as ( SELECT 
		  PLCY_PRD_AUDT_DTL_ID                               AS                               PLCY_PRD_AUDT_DTL_ID 
		, PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
		, upper( TRIM( PLCY_AUDT_TYP_CD ) )                  AS                                   PLCY_AUDT_TYP_CD 
		, PLCY_AUDT_SRC_TYP_ID                               AS                               PLCY_AUDT_SRC_TYP_ID 
		, cast( PLCY_PRD_AUDT_DTL_DUE_DT as DATE )           AS                           PLCY_PRD_AUDT_DTL_DUE_DT 
		, cast( PLCY_PRD_AUDT_DTL_RCVD_DT as DATE )          AS                          PLCY_PRD_AUDT_DTL_RCVD_DT 
		, cast( PLCY_PRD_AUDT_DTL_COMP_DT as DATE )          AS                          PLCY_PRD_AUDT_DTL_COMP_DT 
		, USER_ID                                            AS                                            USER_ID 
		, cast( PLCY_PRD_AUDT_DTL_APNT_FR_DT as DATE )       AS                       PLCY_PRD_AUDT_DTL_APNT_FR_DT 
		, cast( PLCY_PRD_AUDT_DTL_APNT_TO_DT as DATE )       AS                       PLCY_PRD_AUDT_DTL_APNT_TO_DT 
		, PLCY_PRD_AUDT_DTL_EXTRNL_CST                       AS                       PLCY_PRD_AUDT_DTL_EXTRNL_CST 
		, upper( PLCY_PRD_AUDT_DTL_UNPRD_IND )               AS                        PLCY_PRD_AUDT_DTL_UNPRD_IND 
		, upper( TRIM( PAUAST_CD ) )                         AS                                          PAUAST_CD 
		, upper( TRIM( AUDT_PRCS_TYP_CD ) )                  AS                                   AUDT_PRCS_TYP_CD 
		, PLCY_PRD_AUDT_DTL_VRF_TOT_AMT                      AS                      PLCY_PRD_AUDT_DTL_VRF_TOT_AMT 
		, upper( PPAD_UNPRD_PRCS_IND )                       AS                                PPAD_UNPRD_PRCS_IND 
		, upper( TRIM( PAY_TRK_SRC_TYP_CD ) )                AS                                 PAY_TRK_SRC_TYP_CD 
		, upper( TRIM( PPAD_PAY_TRK_SRC_TYP_NM ) )           AS                            PPAD_PAY_TRK_SRC_TYP_NM 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PPAD
            ),
LOGIC_BPPAD as ( SELECT 
		  cast( PLCY_PRD_AUDT_DTL_EFF_DT as DATE )           AS                           PLCY_PRD_AUDT_DTL_EFF_DT 
		, cast( PLCY_PRD_AUDT_DTL_END_DT as DATE )           AS                           PLCY_PRD_AUDT_DTL_END_DT 
		, PLCY_PRD_AUDT_DTL_ID                               AS                               PLCY_PRD_AUDT_DTL_ID 
		from SRC_BPPAD
            ),
LOGIC_PAT as ( SELECT 
		  upper( TRIM( PLCY_AUDT_TYP_NM ) )                  AS                                   PLCY_AUDT_TYP_NM 
		, upper( TRIM( PLCY_AUDT_TYP_CD ) )                  AS                                   PLCY_AUDT_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PAT
            ),
LOGIC_PAST as ( SELECT 
		  upper( TRIM( PLCY_AUDT_SRC_TYP_NM ) )              AS                               PLCY_AUDT_SRC_TYP_NM 
		, PLCY_AUDT_SRC_TYP_ID                               AS                               PLCY_AUDT_SRC_TYP_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PAST
            ),
LOGIC_PAU as ( SELECT 
		  upper( TRIM( PAUAST_NM ) )                         AS                                          PAUAST_NM 
		, upper( TRIM( PAUAST_CD ) )                         AS                                          PAUAST_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PAU
            ),
LOGIC_APT as ( SELECT 
		  upper( TRIM( AUDT_PRCS_TYP_NM ) )                  AS                                   AUDT_PRCS_TYP_NM 
		, upper( TRIM( AUDT_PRCS_TYP_CD ) )                  AS                                   AUDT_PRCS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_APT
            )

---- RENAME LAYER ----
,

RENAME_PPAD as ( SELECT 
		  PLCY_PRD_AUDT_DTL_ID                               as                               PLCY_PRD_AUDT_DTL_ID
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, PLCY_AUDT_TYP_CD                                   as                                   PLCY_AUDT_TYP_CD
		, PLCY_AUDT_SRC_TYP_ID                               as                               PLCY_AUDT_SRC_TYP_ID
		, PLCY_PRD_AUDT_DTL_DUE_DT                           as                           PLCY_PRD_AUDT_DTL_DUE_DT
		, PLCY_PRD_AUDT_DTL_RCVD_DT                          as                          PLCY_PRD_AUDT_DTL_RCVD_DT
		, PLCY_PRD_AUDT_DTL_COMP_DT                          as                          PLCY_PRD_AUDT_DTL_COMP_DT
		, USER_ID                                            as                                            USER_ID
		, PLCY_PRD_AUDT_DTL_APNT_FR_DT                       as                       PLCY_PRD_AUDT_DTL_APNT_FR_DT
		, PLCY_PRD_AUDT_DTL_APNT_TO_DT                       as                       PLCY_PRD_AUDT_DTL_APNT_TO_DT
		, PLCY_PRD_AUDT_DTL_EXTRNL_CST                       as                       PLCY_PRD_AUDT_DTL_EXTRNL_CST
		, PLCY_PRD_AUDT_DTL_UNPRD_IND                        as                        PLCY_PRD_AUDT_DTL_UNPRD_IND
		, PAUAST_CD                                          as                                          PAUAST_CD
		, AUDT_PRCS_TYP_CD                                   as                                   AUDT_PRCS_TYP_CD
		, PLCY_PRD_AUDT_DTL_VRF_TOT_AMT                      as                      PLCY_PRD_AUDT_DTL_VRF_TOT_AMT
		, PPAD_UNPRD_PRCS_IND                                as                                PPAD_UNPRD_PRCS_IND
		, PAY_TRK_SRC_TYP_CD                                 as                                 PAY_TRK_SRC_TYP_CD
		, PPAD_PAY_TRK_SRC_TYP_NM                            as                            PPAD_PAY_TRK_SRC_TYP_NM
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_PPAD   ), 
RENAME_BPPAD as ( SELECT 
		  PLCY_PRD_AUDT_DTL_EFF_DT                           as                           PLCY_PRD_AUDT_DTL_EFF_DT
		, PLCY_PRD_AUDT_DTL_END_DT                           as                           PLCY_PRD_AUDT_DTL_END_DT
		, PLCY_PRD_AUDT_DTL_ID                               as                         BPPAD_PLCY_PRD_AUDT_DTL_ID 
				FROM     LOGIC_BPPAD   ), 
RENAME_PAT as ( SELECT 
		  PLCY_AUDT_TYP_NM                                   as                                   PLCY_AUDT_TYP_NM
		, PLCY_AUDT_TYP_CD                                   as                               PAT_PLCY_AUDT_TYP_CD
		, VOID_IND                                           as                                       PAT_VOID_IND 
				FROM     LOGIC_PAT   ), 
RENAME_PAST as ( SELECT 
		  PLCY_AUDT_SRC_TYP_NM                               as                               PLCY_AUDT_SRC_TYP_NM
		, PLCY_AUDT_SRC_TYP_ID                               as                          PAST_PLCY_AUDT_SRC_TYP_ID
		, VOID_IND                                           as                                      PAST_VOID_IND 
				FROM     LOGIC_PAST   ), 
RENAME_PAU as ( SELECT 
		  PAUAST_NM                                          as                                          PAUAST_NM
		, PAUAST_CD                                          as                                      PAU_PAUAST_CD
		, VOID_IND                                           as                                       PAU_VOID_IND 
				FROM     LOGIC_PAU   ), 
RENAME_APT as ( SELECT 
		  AUDT_PRCS_TYP_NM                                   as                                   AUDT_PRCS_TYP_NM
		, AUDT_PRCS_TYP_CD                                   as                               APT_AUDT_PRCS_TYP_CD
		, VOID_IND                                           as                                       APT_VOID_IND 
				FROM     LOGIC_APT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PPAD                           as ( SELECT * from    RENAME_PPAD   ),
FILTER_BPPAD                          as ( SELECT * from    RENAME_BPPAD   ),
FILTER_PAT                            as ( SELECT * from    RENAME_PAT 
				WHERE PAT_VOID_IND = 'N'  ),
FILTER_PAST                           as ( SELECT * from    RENAME_PAST 
				WHERE PAST_VOID_IND = 'N'  ),
FILTER_APT                            as ( SELECT * from    RENAME_APT 
				WHERE APT_VOID_IND = 'N'  ),
FILTER_PAU                            as ( SELECT * from    RENAME_PAU 
				WHERE PAU_VOID_IND = 'N'  ),

---- JOIN LAYER ----

PPAD as ( SELECT * 
				FROM  FILTER_PPAD
				LEFT JOIN FILTER_BPPAD ON  FILTER_PPAD.PLCY_PRD_AUDT_DTL_ID =  FILTER_BPPAD.BPPAD_PLCY_PRD_AUDT_DTL_ID 
								LEFT JOIN FILTER_PAT ON  FILTER_PPAD.PLCY_AUDT_TYP_CD =  FILTER_PAT.PAT_PLCY_AUDT_TYP_CD 
								LEFT JOIN FILTER_PAST ON  FILTER_PPAD.PLCY_AUDT_SRC_TYP_ID =  FILTER_PAST.PAST_PLCY_AUDT_SRC_TYP_ID 
								LEFT JOIN FILTER_APT ON  FILTER_PPAD.AUDT_PRCS_TYP_CD =  FILTER_APT.APT_AUDT_PRCS_TYP_CD 
								LEFT JOIN FILTER_PAU ON  FILTER_PPAD.PAUAST_CD =  FILTER_PAU.PAU_PAUAST_CD  )
SELECT * 
from PPAD