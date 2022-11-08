

---- SRC LAYER ----
WITH
SRC_DOC as ( SELECT *     from     STAGING.DST_CLAIM_DOCUMENTS ),
//SRC_DOC as ( SELECT *     from     DST_CLAIM_DOCUMENTS) ,

---- LOGIC LAYER ----

LOGIC_DOC as ( SELECT 
		  DOCM_ID                                            as                                            DOCM_ID 
		, DOCM_NO                                            as                                            DOCM_NO 
		, AGRE_ID                                            as                                            AGRE_ID 
		, CUST_ID                                            as                                            CUST_ID 
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
		, AUDIT_USER_CREA_DT                                 as                                 AUDIT_USER_CREA_DT 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DT                                 as                                 AUDIT_USER_UPDT_DT 
		, VOID_IND                                           as                                           VOID_IND 
		, CLM_AGRE_ID                                        as                                        CLM_AGRE_ID 
		, CLM_NO                                             as                                             CLM_NO 
		, CR_USER_LGN_NM                                     as                                     CR_USER_LGN_NM 
		, UP_USER_LGN_NM                                     as                                     UP_USER_LGN_NM 
		from SRC_DOC
            )

---- RENAME LAYER ----
,

RENAME_DOC as ( SELECT 
		  DOCM_ID                                            as                                            DOCM_ID
		, DOCM_NO                                            as                                            DOCM_NO
		, AGRE_ID                                            as                                            AGRE_ID
		, CUST_ID                                            as                                            CUST_ID
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
		, AUDIT_USER_CREA_DT                                 as                                 AUDIT_USER_CREA_DT
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DT                                 as                                 AUDIT_USER_UPDT_DT
		, VOID_IND                                           as                                           VOID_IND
		, CLM_AGRE_ID                                        as                                        CLM_AGRE_ID
		, CLM_NO                                             as                                             CLM_NO
		, CR_USER_LGN_NM                                     as                                     CR_USER_LGN_NM
		, UP_USER_LGN_NM                                     as                                     UP_USER_LGN_NM 
				FROM     LOGIC_DOC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DOC                            as ( SELECT * from    RENAME_DOC   ),

---- JOIN LAYER ----

 JOIN_DOC  as  ( SELECT * 
				FROM  FILTER_DOC )
 SELECT 
  DOCM_ID
, DOCM_NO
, AGRE_ID
, CUST_ID
, DOCM_TYP_VER_ID
, DOCM_TYP_ID
, DOCM_TYP_VER_NO
, DOCM_OCCR_TYP_CD
, DOCM_OCCR_TYP_NM
, DOCM_PPR_STK_TYP_CD
, DOCM_PPR_STK_TYP_NM
, DOCM_CTG_TYP_CD
, DOCM_CTG_TYP_NM
, DOCM_TYP_SRC_TYP_CD
, DOCM_TYP_SRC_TYP_NM
, DOCM_TYP_REF_NO
, DOCM_TYP_NM
, DOCM_TYP_DESC
, DOCM_TYP_VER_EFF_DT
, DOCM_TYP_VER_END_DT
, DOCM_TYP_VER_EVNT_DESC
, DOCM_TYP_VER_TEMPL_FILE_NM
, DOCM_TYP_VER_BTCH_PRNT_TYP
, DOCM_TYP_VER_CREA_LOCK_STS_IND
, DOCM_TYP_VER_CREA_EXCP_IND
, DOCM_TYP_VER_DEL_EXCP_IND
, DOCM_TYP_VER_EXCP_MSG_TXT
, DOCM_TYP_VER_DUP_IND
, DOCM_TYP_VER_SYS_GEN_IND
, DOCM_TYP_VER_SYS_GEN_ONLY_IND
, DOCM_TYP_VER_CLM_DCSN_IND
, DOCM_TYP_VER_CANC_PLCY_IND
, DOCM_TYP_VER_PRE_PRNT_ENCL_IND
, DOCM_TYP_VER_LCL_DLVR_IND
, DOCM_TYP_VER_BTCH_PRNT_IND
, DOCM_TYP_VER_USR_CAN_DEL_IND
, DOCM_TYP_VER_MULTI_RDR_VER_IND
, DOCM_TYP_VER_PRNT_DUPLX_IND
, DOCM_TYP_VER_DOCM_MGMT_IMG_IND
, DOCM_FLAT_CANC_SYS_GEN_TYP_CD
, DOCM_TYP_VER_VOID_IND
, DOCM_TARND_RTN_DT
, DOCM_TARND_EXPT_DT_DRV
, DOCM_STT_TYP_CD
, DOCM_STT_TYP_NM
, DOCM_STT_TYP_EFF_DT
, DOCM_STS_TYP_CD
, DOCM_STS_TYP_NM
, DOCM_STS_TYP_EFF_DT
, DOCM_EFF_DT
, DOCM_END_DT
, DOCM_TARND_EXPT_DD
, DOCM_PND_DT
, DOCM_PND_STS_COMT_TXT
, DOCM_CREA_EXCP_IND
, DOCM_DEL_EXCP_IND
, DOCM_EXCP_MSG_TXT
, DOCM_SYS_GEN_IND
, DOCM_LNK_NOTE_IND
, DOCM_REPRNT_IND
, DOCM_OTHR_EXCP_IND
, AUDIT_USER_ID_CREA
, AUDIT_USER_CREA_DT
, AUDIT_USER_ID_UPDT
, AUDIT_USER_UPDT_DT
, VOID_IND
, CLM_AGRE_ID
, CLM_NO
, CR_USER_LGN_NM
, UP_USER_LGN_NM
 FROM  JOIN_DOC