

---- SRC LAYER ----
WITH
SRC_CLM as ( SELECT *     from     STAGING.DST_CLAIM_INVESTIGATION ),
//SRC_CLM as ( SELECT *     from     DST_CLAIM_INVESTIGATION) ,

---- LOGIC LAYER ----

LOGIC_CLM as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, CLM_NO                                             as                                             CLM_NO 
		, CLM_FST_DCSN_DATE                                  as                                  CLM_FST_DCSN_DATE 
		, JUR_TYP_CD                                         as                                         JUR_TYP_CD 
		, CLM_ACP_STS_IND                                    as                                    CLM_ACP_STS_IND 
		, MANUAL_IND                                         as                                         MANUAL_IND 
		, cast( ACP_START_DTM as DATE )                      as                                      ACP_START_DTM 
		, cast( ACP_END_DTM as DATE )                        as                                        ACP_END_DTM 
		, USER_DRV_UPCS_NM                                   as                                   USER_DRV_UPCS_NM 
		, USER_LGN_NM                                        as                                        USER_LGN_NM 
		, CLM_ACP_STS_SYS_IND                                as                                CLM_ACP_STS_SYS_IND 
		, CLM_ACP_PRCS_STS_RSN_TYP_CD                        as                        CLM_ACP_PRCS_STS_RSN_TYP_CD 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, CLM_ACP_PRCS_STS_RSN_TYP_NM                        as                        CLM_ACP_PRCS_STS_RSN_TYP_NM 
		, CLM_ACP_PRCS_STS_RSN_ACT                           as                           CLM_ACP_PRCS_STS_RSN_ACT 
		, CLM_ACP_PRCS_STS_RSN_DISP                          as                          CLM_ACP_PRCS_STS_RSN_DISP 
		from SRC_CLM
            )

---- RENAME LAYER ----
,

RENAME_CLM as ( SELECT 
          UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CLM_NO                                             as                                       CLAIM_NUMBER
		, CLM_FST_DCSN_DATE                                  as                          CLAIM_FIRST_DECISION_DATE
		, JUR_TYP_CD                                         as                             JURISDICTION_TYPE_CODE
		, CLM_ACP_STS_IND                                    as                               CLAIM_ACP_STATUS_IND
		, MANUAL_IND                                         as                        ACP_MANUAL_INTERVENTION_IND
		, ACP_START_DTM                                      as                                     ACP_START_DATE
		, ACP_END_DTM                                        as                                       ACP_END_DATE
		, USER_DRV_UPCS_NM                                   as                                        PRIMARY_CSS
		, USER_LGN_NM                                        as                                    USER_LOGIN_NAME
		, CLM_ACP_STS_SYS_IND                                as                        CLAIM_ACP_STATUS_SYSTEM_IND
		, CLM_ACP_PRCS_STS_RSN_TYP_CD                        as          CLAIM_ACP_PROCESS_STATUS_REASON_TYPE_CODE
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, CLM_ACP_PRCS_STS_RSN_TYP_NM                        as          CLAIM_ACP_PROCESS_STATUS_REASON_TYPE_NAME
		, CLM_ACP_PRCS_STS_RSN_ACT                           as         CLAIM_ACP_PROCESS_STATUS_REASON_ACTION_IND
		, CLM_ACP_PRCS_STS_RSN_DISP                          as        CLAIM_ACP_PROCESS_STATUS_REASON_DISPLAY_IND 
				FROM     LOGIC_CLM   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),

---- JOIN LAYER ----

 JOIN_CLM  as  ( SELECT * 
				FROM  FILTER_CLM )
 SELECT * FROM  JOIN_CLM