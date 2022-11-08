

      create or replace  table DEV_EDW.STAGING.STG_CASE_EVENT  as
      (---- SRC LAYER ----
WITH
SRC_CE as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_EVENT ),
SRC_CET as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_EVENT_TYPE ),
SRC_BCE as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_CASE_EVENT ),
//SRC_CE as ( SELECT *     from     CASE_EVENT) ,
//SRC_CET as ( SELECT *     from     CASE_EVENT_TYPE) ,
//SRC_BCE as ( SELECT *     from     BWC_CASE_EVENT) ,

---- LOGIC LAYER ----

LOGIC_CE as ( SELECT 
		  CASE_EVNT_ID                                       AS                                       CASE_EVNT_ID 
		, CASE_ID                                            AS                                            CASE_ID 
		, upper( TRIM( CASE_EVNT_TYP_CD ) )                  AS                                   CASE_EVNT_TYP_CD 
		, cast( CASE_EVNT_DUE_DT as DATE )                   AS                                   CASE_EVNT_DUE_DT 
		, cast( CASE_EVNT_COMP_DT as DATE )                  AS                                  CASE_EVNT_COMP_DT 
		, cast( CASE_EVNT_DOCM_REQS_DT as DATE )             AS                             CASE_EVNT_DOCM_REQS_DT 
		, cast( CASE_EVNT_DOCM_RECV_DT as DATE )             AS                             CASE_EVNT_DOCM_RECV_DT 
		, upper( TRIM( CASE_EVNT_COMT_TXT ) )                AS                                 CASE_EVNT_COMT_TXT 
		, USER_ID                                            AS                                            USER_ID 
		, ORG_UNT_ID                                         AS                                         ORG_UNT_ID 
		, CASE_PTCP_ID                                       AS                                       CASE_PTCP_ID 
		, upper( CASE_EVNT_SEND_TASK_RMND_IND )              AS                       CASE_EVNT_SEND_TASK_RMND_IND 
		, upper( CASE_EVNT_SEND_CNTX_OWNR_IND )              AS                       CASE_EVNT_SEND_CNTX_OWNR_IND 
		, cast( CASE_EVNT_FST_RMND_DT as DATE )              AS                              CASE_EVNT_FST_RMND_DT 
		, cast( CASE_EVNT_SCND_RMND_DT as DATE )             AS                             CASE_EVNT_SCND_RMND_DT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CE
            ),
LOGIC_CET as ( SELECT 
		  upper( TRIM( CASE_EVNT_TYP_NM ) )                  AS                                   CASE_EVNT_TYP_NM 
		, upper( TRIM( CASE_EVNT_TYP_CD ) )                  AS                                   CASE_EVNT_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CET
            ),
LOGIC_BCE as ( SELECT 
		  CASE_EVNT_ID                                       AS                                       CASE_EVNT_ID 
		, cast( CASE_EVNT_SENT_TO_IC_DT as DATE )            AS                            CASE_EVNT_SENT_TO_IC_DT 
		, DW_CHANGE_DT                                       AS                                       DW_CHANGE_DT 
		from SRC_BCE
            )

---- RENAME LAYER ----
,

RENAME_CE as ( SELECT 
		  CASE_EVNT_ID                                       as                                       CASE_EVNT_ID
		, CASE_ID                                            as                                            CASE_ID
		, CASE_EVNT_TYP_CD                                   as                                   CASE_EVNT_TYP_CD
		, CASE_EVNT_DUE_DT                                   as                                   CASE_EVNT_DUE_DT
		, CASE_EVNT_COMP_DT                                  as                                  CASE_EVNT_COMP_DT
		, CASE_EVNT_DOCM_REQS_DT                             as                             CASE_EVNT_DOCM_REQS_DT
		, CASE_EVNT_DOCM_RECV_DT                             as                             CASE_EVNT_DOCM_RECV_DT
		, CASE_EVNT_COMT_TXT                                 as                                 CASE_EVNT_COMT_TXT
		, USER_ID                                            as                                            USER_ID
		, ORG_UNT_ID                                         as                                         ORG_UNT_ID
		, CASE_PTCP_ID                                       as                                       CASE_PTCP_ID
		, CASE_EVNT_SEND_TASK_RMND_IND                       as                       CASE_EVNT_SEND_TASK_RMND_IND
		, CASE_EVNT_SEND_CNTX_OWNR_IND                       as                       CASE_EVNT_SEND_CNTX_OWNR_IND
		, CASE_EVNT_FST_RMND_DT                              as                              CASE_EVNT_FST_RMND_DT
		, CASE_EVNT_SCND_RMND_DT                             as                             CASE_EVNT_SCND_RMND_DT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CE   ), 
RENAME_CET as ( SELECT 
		  CASE_EVNT_TYP_NM                                   as                                   CASE_EVNT_TYP_NM
		, CASE_EVNT_TYP_CD                                   as                               CET_CASE_EVNT_TYP_CD
		, VOID_IND                                           as                                       CET_VOID_IND 
				FROM     LOGIC_CET   ), 
RENAME_BCE as ( SELECT 
		  CASE_EVNT_ID                                       as                                   BCE_CASE_EVNT_ID
		, CASE_EVNT_SENT_TO_IC_DT                            as                            CASE_EVNT_SENT_TO_IC_DT
		, DW_CHANGE_DT                                       as                                       DW_CHANGE_DT 
				FROM     LOGIC_BCE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CE                             as ( SELECT * from    RENAME_CE   ),
FILTER_BCE                            as ( SELECT * from    RENAME_BCE   ),
FILTER_CET                            as ( SELECT * from    RENAME_CET 
				WHERE CET_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CE as ( SELECT * 
				FROM  FILTER_CE
				INNER JOIN FILTER_BCE ON  FILTER_CE.CASE_EVNT_ID =  FILTER_BCE.BCE_CASE_EVNT_ID 
						LEFT JOIN FILTER_CET ON  FILTER_CE.CASE_EVNT_TYP_CD =  FILTER_CET.CET_CASE_EVNT_TYP_CD  )
SELECT * 
from CE
      );
    