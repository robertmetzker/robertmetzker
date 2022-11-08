---- SRC LAYER ----
WITH
SRC_PPASH as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD_AUDIT_STS_HIST ),
SRC_PAST as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_AUDIT_STATUS_TYPE ),
SRC_PASRT as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_AUDIT_STATUS_REASON_TYP ),
//SRC_PPASH as ( SELECT *     from     POLICY_PERIOD_AUDIT_STS_HIST) ,
//SRC_PAST as ( SELECT *     from     POLICY_AUDIT_STATUS_TYPE) ,
//SRC_PASRT as ( SELECT *     from     POLICY_AUDIT_STATUS_REASON_TYP) ,

---- LOGIC LAYER ----

LOGIC_PPASH as ( SELECT 
		  HIST_ID                                            AS                                            HIST_ID 
		, PLCY_PRD_AUDT_STS_ID                               AS                               PLCY_PRD_AUDT_STS_ID 
		, PLCY_PRD_AUDT_DTL_ID                               AS                               PLCY_PRD_AUDT_DTL_ID 
		, upper( TRIM( PLCY_AUDT_STS_TYP_CD ) )              AS                               PLCY_AUDT_STS_TYP_CD 
		, upper( TRIM( PLCY_AUDT_STS_RSN_TYP_CD ) )          AS                           PLCY_AUDT_STS_RSN_TYP_CD 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, HIST_EFF_DTM                                       AS                                       HIST_EFF_DTM 
		, HIST_END_DTM                                       AS                                       HIST_END_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		, upper( BRAC_AUDT_STS_COMP_IND )                    AS                             BRAC_AUDT_STS_COMP_IND 
		from SRC_PPASH
            ),
LOGIC_PAST as ( SELECT 
		  upper( TRIM( PLCY_AUDT_STS_TYP_NM ) )              AS                               PLCY_AUDT_STS_TYP_NM 
		, upper( TRIM( PLCY_AUDT_STS_TYP_CD ) )              AS                               PLCY_AUDT_STS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PAST
            ),
LOGIC_PASRT as ( SELECT 
		  upper( TRIM( PLCY_AUDT_STS_RSN_TYP_NM ) )          AS                           PLCY_AUDT_STS_RSN_TYP_NM 
		, upper( TRIM( PLCY_AUDT_STS_RSN_TYP_CD ) )          AS                           PLCY_AUDT_STS_RSN_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PASRT
            )

---- RENAME LAYER ----
,

RENAME_PPASH as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID
		, PLCY_PRD_AUDT_STS_ID                               as                               PLCY_PRD_AUDT_STS_ID
		, PLCY_PRD_AUDT_DTL_ID                               as                               PLCY_PRD_AUDT_DTL_ID
		, PLCY_AUDT_STS_TYP_CD                               as                               PLCY_AUDT_STS_TYP_CD
		, PLCY_AUDT_STS_RSN_TYP_CD                           as                           PLCY_AUDT_STS_RSN_TYP_CD
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM
		, HIST_END_DTM                                       as                                       HIST_END_DTM
		, VOID_IND                                           as                                           VOID_IND
		, BRAC_AUDT_STS_COMP_IND                             as                             BRAC_AUDT_STS_COMP_IND 
				FROM     LOGIC_PPASH   ), 
RENAME_PAST as ( SELECT 
		  PLCY_AUDT_STS_TYP_NM                               as                               PLCY_AUDT_STS_TYP_NM
		, PLCY_AUDT_STS_TYP_CD                               as                          PAST_PLCY_AUDT_STS_TYP_CD
		, VOID_IND                                           as                                      PAST_VOID_IND 
				FROM     LOGIC_PAST   ), 
RENAME_PASRT as ( SELECT 
		  PLCY_AUDT_STS_RSN_TYP_NM                           as                           PLCY_AUDT_STS_RSN_TYP_NM
		, PLCY_AUDT_STS_RSN_TYP_CD                           as                     PASRT_PLCY_AUDT_STS_RSN_TYP_CD
		, VOID_IND                                           as                                     PASRT_VOID_IND 
				FROM     LOGIC_PASRT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PPASH                          as ( SELECT * from    RENAME_PPASH   ),
FILTER_PAST                           as ( SELECT * from    RENAME_PAST 
				WHERE PAST_VOID_IND = 'N'  ),
FILTER_PASRT                          as ( SELECT * from    RENAME_PASRT 
				WHERE PASRT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

PPASH as ( SELECT * 
				FROM  FILTER_PPASH
				LEFT JOIN FILTER_PAST ON  FILTER_PPASH.PLCY_AUDT_STS_TYP_CD =  FILTER_PAST.PAST_PLCY_AUDT_STS_TYP_CD 
								LEFT JOIN FILTER_PASRT ON  FILTER_PPASH.PLCY_AUDT_STS_RSN_TYP_CD =  FILTER_PASRT.PASRT_PLCY_AUDT_STS_RSN_TYP_CD  )
SELECT * 
from PPASH