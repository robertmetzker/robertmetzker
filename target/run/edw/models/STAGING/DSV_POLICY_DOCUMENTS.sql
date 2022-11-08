
  create or replace  view DEV_EDW.STAGING.DSV_POLICY_DOCUMENTS  as (
    

---- SRC LAYER ----
WITH
SRC_PD             as ( SELECT *     FROM     STAGING.DST_POLICY_DOCUMENTS ),
//SRC_PD             as ( SELECT *     FROM     DST_POLICY_DOCUMENTS) ,

---- LOGIC LAYER ----


LOGIC_PD as ( SELECT 
		  DOCM_ID                                            as                                            DOCM_ID 
		, DOCM_NO                                            as                                            DOCM_NO 
		, AGRE_ID                                            as                                            AGRE_ID 
		, CUST_ID                                            as                                            CUST_ID 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, DOCM_TYP_VER_ID                                    as                                    DOCM_TYP_VER_ID 
		, DOCM_TYP_ID                                        as                                        DOCM_TYP_ID 
		, DOCM_TYP_VER_NO                                    as                                    DOCM_TYP_VER_NO 
		, DOCM_OCCR_TYP_CD                                   as                                   DOCM_OCCR_TYP_CD 
		, DOCM_OCCR_TYP_NM                                   as                                   DOCM_OCCR_TYP_NM 
		, DOCM_PPR_STK_TYP_CD                                as                                DOCM_PPR_STK_TYP_CD 
		, DOCM_PPR_STK_TYP_NM                                as                                DOCM_PPR_STK_TYP_NM 
		, DOCM_CTG_TYP_CD                                    as                                    DOCM_CTG_TYP_CD 
		, DOCM_CTG_TYP_NM                                    as                                    DOCM_CTG_TYP_NM 
		, DOCM_TYP_SRC_TYP_CD                                as                                DOCM_TYP_SRC_TYP_CD 
		, DOCM_TYP_SRC_TYP_NM                                as                                DOCM_TYP_SRC_TYP_NM 
		, DOCM_TYP_REF_NO                                    as                                    DOCM_TYP_REF_NO 
		, DOCM_TYP_NM                                        as                                        DOCM_TYP_NM 
		, DOCM_TYP_DESC                                      as                                      DOCM_TYP_DESC 
		, DOCM_TYP_VER_EFF_DT                                as                                DOCM_TYP_VER_EFF_DT 
		, DOCM_TYP_VER_END_DT                                as                                DOCM_TYP_VER_END_DT 
		, DOCM_TYP_VER_EVNT_DESC                             as                             DOCM_TYP_VER_EVNT_DESC 
		, DOCM_TYP_VER_TEMPL_FILE_NM                         as                         DOCM_TYP_VER_TEMPL_FILE_NM 
		, DOCM_TYP_VER_BTCH_PRNT_TYP                         as                         DOCM_TYP_VER_BTCH_PRNT_TYP 
		, DOCM_TYP_VER_CREA_LOCK_STS_IND                     as                     DOCM_TYP_VER_CREA_LOCK_STS_IND 
		, DOCM_TYP_VER_CREA_EXCP_IND                         as                         DOCM_TYP_VER_CREA_EXCP_IND 
		, DOCM_TYP_VER_DEL_EXCP_IND                          as                          DOCM_TYP_VER_DEL_EXCP_IND 
		, DOCM_TYP_VER_EXCP_MSG_TXT                          as                          DOCM_TYP_VER_EXCP_MSG_TXT 
		, DOCM_TYP_VER_DUP_IND                               as                               DOCM_TYP_VER_DUP_IND 
		, DOCM_TYP_VER_SYS_GEN_IND                           as                           DOCM_TYP_VER_SYS_GEN_IND 
		, DOCM_TYP_VER_SYS_GEN_ONLY_IND                      as                      DOCM_TYP_VER_SYS_GEN_ONLY_IND 
		, DOCM_TYP_VER_CLM_DCSN_IND                          as                          DOCM_TYP_VER_CLM_DCSN_IND 
		, DOCM_TYP_VER_CANC_PLCY_IND                         as                         DOCM_TYP_VER_CANC_PLCY_IND 
		, DOCM_TYP_VER_PRE_PRNT_ENCL_IND                     as                     DOCM_TYP_VER_PRE_PRNT_ENCL_IND 
		, DOCM_TYP_VER_LCL_DLVR_IND                          as                          DOCM_TYP_VER_LCL_DLVR_IND 
		, DOCM_TYP_VER_BTCH_PRNT_IND                         as                         DOCM_TYP_VER_BTCH_PRNT_IND 
		, DOCM_TYP_VER_USR_CAN_DEL_IND                       as                       DOCM_TYP_VER_USR_CAN_DEL_IND 
		, DOCM_TYP_VER_MULTI_RDR_VER_IND                     as                     DOCM_TYP_VER_MULTI_RDR_VER_IND 
		, DOCM_TYP_VER_PRNT_DUPLX_IND                        as                        DOCM_TYP_VER_PRNT_DUPLX_IND 
		, DOCM_TYP_VER_DOCM_MGMT_IMG_IND                     as                     DOCM_TYP_VER_DOCM_MGMT_IMG_IND 
		, DOCM_FLAT_CANC_SYS_GEN_TYP_CD                      as                      DOCM_FLAT_CANC_SYS_GEN_TYP_CD 
		, DOCM_TYP_VER_VOID_IND                              as                              DOCM_TYP_VER_VOID_IND 
		, DOCM_TARND_RTN_DT                                  as                                  DOCM_TARND_RTN_DT 
		, DOCM_TARND_EXPT_DT_DRV                             as                             DOCM_TARND_EXPT_DT_DRV 
		, DOCM_STT_TYP_CD                                    as                                    DOCM_STT_TYP_CD 
		, DOCM_STT_TYP_NM                                    as                                    DOCM_STT_TYP_NM 
		, DOCM_STT_TYP_EFF_DT                                as                                DOCM_STT_TYP_EFF_DT 
		, DOCM_STS_TYP_CD                                    as                                    DOCM_STS_TYP_CD 
		, DOCM_STS_TYP_NM                                    as                                    DOCM_STS_TYP_NM 
		, DOCM_STS_TYP_EFF_DT                                as                                DOCM_STS_TYP_EFF_DT 
		, DOCM_EFF_DT                                        as                                        DOCM_EFF_DT 
		, DOCM_END_DT                                        as                                        DOCM_END_DT 
		, DOCM_TARND_EXPT_DD                                 as                                 DOCM_TARND_EXPT_DD 
		, DOCM_PND_DT                                        as                                        DOCM_PND_DT 
		, DOCM_PND_STS_COMT_TXT                              as                              DOCM_PND_STS_COMT_TXT 
		, DOCM_CREA_EXCP_IND                                 as                                 DOCM_CREA_EXCP_IND 
		, DOCM_DEL_EXCP_IND                                  as                                  DOCM_DEL_EXCP_IND 
		, DOCM_EXCP_MSG_TXT                                  as                                  DOCM_EXCP_MSG_TXT 
		, DOCM_SYS_GEN_IND                                   as                                   DOCM_SYS_GEN_IND 
		, DOCM_LNK_NOTE_IND                                  as                                  DOCM_LNK_NOTE_IND 
		, DOCM_REPRNT_IND                                    as                                    DOCM_REPRNT_IND 
		, DOCM_OTHR_EXCP_IND                                 as                                 DOCM_OTHR_EXCP_IND 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, VOID_IND                                           as                                           VOID_IND 
		, PTCP_ID                                            as                                            PTCP_ID 
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD 
		, PTCP_TYP_NM                                        as                                        PTCP_TYP_NM 
		, AGRE_TYP_CD                                        as                                        AGRE_TYP_CD 
		, PLCY_NO                                            as                                            PLCY_NO 
		, PLCY_PLCY_PRD_ID                                   as                                   PLCY_PLCY_PRD_ID 
		, PLCY_CUST_ID                                       as                                       PLCY_CUST_ID 
		, CUST_NO                                            as                                            CUST_NO 
		, PLCY_PRD_PTCP_EFF_DATE                             as                             PLCY_PRD_PTCP_EFF_DATE 
		, PLCY_PRD_PTCP_END_DATE                             as                             PLCY_PRD_PTCP_END_DATE 
		, PLCY_PRD_PTCP_RN_IND                               as                               PLCY_PRD_PTCP_RN_IND 
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO 
		, PLCY_PRD_PTCP_INS_PRI_IND                          as                          PLCY_PRD_PTCP_INS_PRI_IND 
		, PLCY_PRD_PTCP_INS_FST_HIRE_DATE                    as                    PLCY_PRD_PTCP_INS_FST_HIRE_DATE 
		, INSRD_CERT_IND                                     as                                     INSRD_CERT_IND 
		, PPPIE_COV_IND                                      as                                      PPPIE_COV_IND 
		, COV_TYP_CD                                         as                                         COV_TYP_CD 
		, COV_TYP_NM                                         as                                         COV_TYP_NM 
		, TTL_TYP_CD                                         as                                         TTL_TYP_CD 
		, TTL_TYP_NM                                         as                                         TTL_TYP_NM 
		, EMPLR_REP_TYP_CD                                   as                                   EMPLR_REP_TYP_CD 
		, EMPLR_REP_TYP_NM                                   as                                   EMPLR_REP_TYP_NM 
		, PGRER_GRP_NO                                       as                                       PGRER_GRP_NO 
		, PTCP_NOTE_IND                                      as                                      PTCP_NOTE_IND 
		, PLCY_AUDIT_USER_CREA_DTM                                 as                                 PLCY_AUDIT_USER_CREA_DTM  
		, PLCY_AUDIT_USER_UPDT_DTM                                 as                                 PLCY_AUDIT_USER_UPDT_DTM 
		, CRNT_PLCY_PRD_PTCP_IND                             as                             CRNT_PLCY_PRD_PTCP_IND 
		, CR_USER_ID                                         as                                         CR_USER_ID 
		, CR_USER_LGN_NM                                     as                                     CR_USER_LGN_NM 
		, UP_USER_ID                                         as                                         UP_USER_ID 
		, UP_USER_LGN_NM                                     as                                     UP_USER_LGN_NM 
		FROM SRC_PD
            )

---- RENAME LAYER ----
,

RENAME_PD         as ( SELECT 
		  DOCM_ID                                            as                                            DOCM_ID
		, DOCM_NO                                            as                                            DOCM_NO
		, AGRE_ID                                            as                                            AGRE_ID
		, CUST_ID                                            as                                            CUST_ID
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, DOCM_TYP_VER_ID                                    as                                    DOCM_TYP_VER_ID
		, DOCM_TYP_ID                                        as                                        DOCM_TYP_ID
		, DOCM_TYP_VER_NO                                    as                                    DOCM_TYP_VER_NO
		, DOCM_OCCR_TYP_CD                                   as                                   DOCM_OCCR_TYP_CD
		, DOCM_OCCR_TYP_NM                                   as                                   DOCM_OCCR_TYP_NM
		, DOCM_PPR_STK_TYP_CD                                as                                DOCM_PPR_STK_TYP_CD
		, DOCM_PPR_STK_TYP_NM                                as                                DOCM_PPR_STK_TYP_NM
		, DOCM_CTG_TYP_CD                                    as                                    DOCM_CTG_TYP_CD
		, DOCM_CTG_TYP_NM                                    as                                    DOCM_CTG_TYP_NM
		, DOCM_TYP_SRC_TYP_CD                                as                                DOCM_TYP_SRC_TYP_CD
		, DOCM_TYP_SRC_TYP_NM                                as                                DOCM_TYP_SRC_TYP_NM
		, DOCM_TYP_REF_NO                                    as                                    DOCM_TYP_REF_NO
		, DOCM_TYP_NM                                        as                                        DOCM_TYP_NM
		, DOCM_TYP_DESC                                      as                                      DOCM_TYP_DESC
		, DOCM_TYP_VER_EFF_DT                                as                                DOCM_TYP_VER_EFF_DT
		, DOCM_TYP_VER_END_DT                                as                                DOCM_TYP_VER_END_DT
		, DOCM_TYP_VER_EVNT_DESC                             as                             DOCM_TYP_VER_EVNT_DESC
		, DOCM_TYP_VER_TEMPL_FILE_NM                         as                         DOCM_TYP_VER_TEMPL_FILE_NM
		, DOCM_TYP_VER_BTCH_PRNT_TYP                         as                         DOCM_TYP_VER_BTCH_PRNT_TYP
		, DOCM_TYP_VER_CREA_LOCK_STS_IND                     as                     DOCM_TYP_VER_CREA_LOCK_STS_IND
		, DOCM_TYP_VER_CREA_EXCP_IND                         as                         DOCM_TYP_VER_CREA_EXCP_IND
		, DOCM_TYP_VER_DEL_EXCP_IND                          as                          DOCM_TYP_VER_DEL_EXCP_IND
		, DOCM_TYP_VER_EXCP_MSG_TXT                          as                          DOCM_TYP_VER_EXCP_MSG_TXT
		, DOCM_TYP_VER_DUP_IND                               as                               DOCM_TYP_VER_DUP_IND
		, DOCM_TYP_VER_SYS_GEN_IND                           as                           DOCM_TYP_VER_SYS_GEN_IND
		, DOCM_TYP_VER_SYS_GEN_ONLY_IND                      as                      DOCM_TYP_VER_SYS_GEN_ONLY_IND
		, DOCM_TYP_VER_CLM_DCSN_IND                          as                          DOCM_TYP_VER_CLM_DCSN_IND
		, DOCM_TYP_VER_CANC_PLCY_IND                         as                         DOCM_TYP_VER_CANC_PLCY_IND
		, DOCM_TYP_VER_PRE_PRNT_ENCL_IND                     as                     DOCM_TYP_VER_PRE_PRNT_ENCL_IND
		, DOCM_TYP_VER_LCL_DLVR_IND                          as                          DOCM_TYP_VER_LCL_DLVR_IND
		, DOCM_TYP_VER_BTCH_PRNT_IND                         as                         DOCM_TYP_VER_BTCH_PRNT_IND
		, DOCM_TYP_VER_USR_CAN_DEL_IND                       as                       DOCM_TYP_VER_USR_CAN_DEL_IND
		, DOCM_TYP_VER_MULTI_RDR_VER_IND                     as                     DOCM_TYP_VER_MULTI_RDR_VER_IND
		, DOCM_TYP_VER_PRNT_DUPLX_IND                        as                        DOCM_TYP_VER_PRNT_DUPLX_IND
		, DOCM_TYP_VER_DOCM_MGMT_IMG_IND                     as                     DOCM_TYP_VER_DOCM_MGMT_IMG_IND
		, DOCM_FLAT_CANC_SYS_GEN_TYP_CD                      as                      DOCM_FLAT_CANC_SYS_GEN_TYP_CD
		, DOCM_TYP_VER_VOID_IND                              as                              DOCM_TYP_VER_VOID_IND
		, DOCM_TARND_RTN_DT                                  as                                  DOCM_TARND_RTN_DT
		, DOCM_TARND_EXPT_DT_DRV                             as                             DOCM_TARND_EXPT_DT_DRV
		, DOCM_STT_TYP_CD                                    as                                    DOCM_STT_TYP_CD
		, DOCM_STT_TYP_NM                                    as                                    DOCM_STT_TYP_NM
		, DOCM_STT_TYP_EFF_DT                                as                                DOCM_STT_TYP_EFF_DT
		, DOCM_STS_TYP_CD                                    as                                    DOCM_STS_TYP_CD
		, DOCM_STS_TYP_NM                                    as                                    DOCM_STS_TYP_NM
		, DOCM_STS_TYP_EFF_DT                                as                                DOCM_STS_TYP_EFF_DT
		, DOCM_EFF_DT                                        as                                        DOCM_EFF_DT
		, DOCM_END_DT                                        as                                        DOCM_END_DT
		, DOCM_TARND_EXPT_DD                                 as                                 DOCM_TARND_EXPT_DD
		, DOCM_PND_DT                                        as                                        DOCM_PND_DT
		, DOCM_PND_STS_COMT_TXT                              as                              DOCM_PND_STS_COMT_TXT
		, DOCM_CREA_EXCP_IND                                 as                                 DOCM_CREA_EXCP_IND
		, DOCM_DEL_EXCP_IND                                  as                                  DOCM_DEL_EXCP_IND
		, DOCM_EXCP_MSG_TXT                                  as                                  DOCM_EXCP_MSG_TXT
		, DOCM_SYS_GEN_IND                                   as                                   DOCM_SYS_GEN_IND
		, DOCM_LNK_NOTE_IND                                  as                                  DOCM_LNK_NOTE_IND
		, DOCM_REPRNT_IND                                    as                                    DOCM_REPRNT_IND
		, DOCM_OTHR_EXCP_IND                                 as                                 DOCM_OTHR_EXCP_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND
		, PTCP_ID                                            as                                            PTCP_ID
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD
		, PTCP_TYP_NM                                        as                                        PTCP_TYP_NM
		, AGRE_TYP_CD                                        as                                        AGRE_TYP_CD
		, PLCY_NO                                            as                                            PLCY_NO
		, PLCY_PLCY_PRD_ID                                   as                                   PLCY_PLCY_PRD_ID
		, PLCY_CUST_ID                                       as                                       PLCY_CUST_ID
		, CUST_NO                                            as                                            CUST_NO
		, PLCY_PRD_PTCP_EFF_DATE                             as                             PLCY_PRD_PTCP_EFF_DATE
		, PLCY_PRD_PTCP_END_DATE                             as                             PLCY_PRD_PTCP_END_DATE
		, PLCY_PRD_PTCP_RN_IND                               as                               PLCY_PRD_PTCP_RN_IND
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO
		, PLCY_PRD_PTCP_INS_PRI_IND                          as                          PLCY_PRD_PTCP_INS_PRI_IND
		, PLCY_PRD_PTCP_INS_FST_HIRE_DATE                    as                    PLCY_PRD_PTCP_INS_FST_HIRE_DATE
		, INSRD_CERT_IND                                     as                                     INSRD_CERT_IND
		, PPPIE_COV_IND                                      as                                      PPPIE_COV_IND
		, COV_TYP_CD                                         as                                         COV_TYP_CD
		, COV_TYP_NM                                         as                                         COV_TYP_NM
		, TTL_TYP_CD                                         as                                         TTL_TYP_CD
		, TTL_TYP_NM                                         as                                         TTL_TYP_NM
		, EMPLR_REP_TYP_CD                                   as                                   EMPLR_REP_TYP_CD
		, EMPLR_REP_TYP_NM                                   as                                   EMPLR_REP_TYP_NM
		, PGRER_GRP_NO                                       as                                       PGRER_GRP_NO
		, PTCP_NOTE_IND                                      as                                      PTCP_NOTE_IND
		, PLCY_AUDIT_USER_CREA_DTM                                 as                                 PLCY_AUDIT_USER_CREA_DTM  
		, PLCY_AUDIT_USER_UPDT_DTM                                 as                                 PLCY_AUDIT_USER_UPDT_DTM
		, CRNT_PLCY_PRD_PTCP_IND                             as                             CRNT_PLCY_PRD_PTCP_IND
		, CR_USER_ID                                         as                                         CR_USER_ID
		, CR_USER_LGN_NM                                     as                                     CR_USER_LGN_NM
		, UP_USER_ID                                         as                                         UP_USER_ID
		, UP_USER_LGN_NM                                     as                                     UP_USER_LGN_NM 
				FROM     LOGIC_PD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PD                             as ( SELECT * FROM    RENAME_PD   ),

---- JOIN LAYER ----

 JOIN_PD          as  ( SELECT * 
				FROM  FILTER_PD )
 SELECT * FROM  JOIN_PD
  );
