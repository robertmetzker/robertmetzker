

      create or replace  table DEV_EDW.STAGING.DST_CLAIM_DOCUMENTS  as
      (---- SRC LAYER ----
WITH
SRC_DOC as ( SELECT *     from     STAGING.STG_DOCUMENT ),
SRC_CLM as ( SELECT *     from     STAGING.STG_CLAIM ),
SRC_CR_USER as ( SELECT *     from     STAGING.STG_USERS ),
SRC_UP_USER as ( SELECT *     from     STAGING.STG_USERS ),
//SRC_DOC as ( SELECT *     from     STG_DOCUMENT) ,
//SRC_CLM as ( SELECT *     from     STG_CLAIM) ,
//SRC_CR_USER as ( SELECT *     from     STG_USERS) ,
//SRC_UP_USER as ( SELECT *     from     STG_USERS) ,

---- LOGIC LAYER ----

LOGIC_DOC as ( SELECT 
		  DOCM_ID                                            as                                            DOCM_ID 
		, DOCM_NO                                            as                                            DOCM_NO 
		, AGRE_ID                                            as                                            AGRE_ID 
		, CUST_ID                                            as                                            CUST_ID 
		, DOCM_TYP_VER_ID                                    as                                    DOCM_TYP_VER_ID 
		, DOCM_TYP_ID                                        as                                        DOCM_TYP_ID 
		, DOCM_TYP_VER_NO                                    as                                    DOCM_TYP_VER_NO 
		, TRIM( DOCM_OCCR_TYP_CD )                           as                                   DOCM_OCCR_TYP_CD 
		, TRIM( DOCM_OCCR_TYP_NM )                           as                                   DOCM_OCCR_TYP_NM 
		, TRIM( DOCM_PPR_STK_TYP_CD )                        as                                DOCM_PPR_STK_TYP_CD 
		, TRIM( DOCM_PPR_STK_TYP_NM )                        as                                DOCM_PPR_STK_TYP_NM 
		, TRIM( DOCM_CTG_TYP_CD )                            as                                    DOCM_CTG_TYP_CD 
		, TRIM( DOCM_CTG_TYP_NM )                            as                                    DOCM_CTG_TYP_NM 
		, TRIM( DOCM_TYP_SRC_TYP_CD )                        as                                DOCM_TYP_SRC_TYP_CD 
		, TRIM( DOCM_TYP_SRC_TYP_NM )                        as                                DOCM_TYP_SRC_TYP_NM 
		, TRIM( DOCM_TYP_REF_NO )                            as                                    DOCM_TYP_REF_NO 
		, TRIM( DOCM_TYP_NM )                                as                                        DOCM_TYP_NM 
		, TRIM( DOCM_TYP_DESC )                              as                                      DOCM_TYP_DESC 
		, DOCM_TYP_VER_EFF_DT                                as                                DOCM_TYP_VER_EFF_DT 
		, DOCM_TYP_VER_END_DT                                as                                DOCM_TYP_VER_END_DT 
		, TRIM( DOCM_TYP_VER_EVNT_DESC )                     as                             DOCM_TYP_VER_EVNT_DESC 
		, TRIM( DOCM_TYP_VER_TEMPL_FILE_NM )                 as                         DOCM_TYP_VER_TEMPL_FILE_NM 
		, TRIM( DOCM_TYP_VER_BTCH_PRNT_TYP )                 as                         DOCM_TYP_VER_BTCH_PRNT_TYP 
		, TRIM( DOCM_TYP_VER_CREA_LOCK_STS_IND )             as                     DOCM_TYP_VER_CREA_LOCK_STS_IND 
		, TRIM( DOCM_TYP_VER_CREA_EXCP_IND )                 as                         DOCM_TYP_VER_CREA_EXCP_IND 
		, TRIM( DOCM_TYP_VER_DEL_EXCP_IND )                  as                          DOCM_TYP_VER_DEL_EXCP_IND 
		, TRIM( DOCM_TYP_VER_EXCP_MSG_TXT )                  as                          DOCM_TYP_VER_EXCP_MSG_TXT 
		, TRIM( DOCM_TYP_VER_DUP_IND )                       as                               DOCM_TYP_VER_DUP_IND 
		, TRIM( DOCM_TYP_VER_SYS_GEN_IND )                   as                           DOCM_TYP_VER_SYS_GEN_IND 
		, TRIM( DOCM_TYP_VER_SYS_GEN_ONLY_IND )              as                      DOCM_TYP_VER_SYS_GEN_ONLY_IND 
		, TRIM( DOCM_TYP_VER_CLM_DCSN_IND )                  as                          DOCM_TYP_VER_CLM_DCSN_IND 
		, TRIM( DOCM_TYP_VER_CANC_PLCY_IND )                 as                         DOCM_TYP_VER_CANC_PLCY_IND 
		, TRIM( DOCM_TYP_VER_PRE_PRNT_ENCL_IND )             as                     DOCM_TYP_VER_PRE_PRNT_ENCL_IND 
		, TRIM( DOCM_TYP_VER_LCL_DLVR_IND )                  as                          DOCM_TYP_VER_LCL_DLVR_IND 
		, TRIM( DOCM_TYP_VER_BTCH_PRNT_IND )                 as                         DOCM_TYP_VER_BTCH_PRNT_IND 
		, TRIM( DOCM_TYP_VER_USR_CAN_DEL_IND )               as                       DOCM_TYP_VER_USR_CAN_DEL_IND 
		, TRIM( DOCM_TYP_VER_MULTI_RDR_VER_IND )             as                     DOCM_TYP_VER_MULTI_RDR_VER_IND 
		, TRIM( DOCM_TYP_VER_PRNT_DUPLX_IND )                as                        DOCM_TYP_VER_PRNT_DUPLX_IND 
		, TRIM( DOCM_TYP_VER_DOCM_MGMT_IMG_IND )             as                     DOCM_TYP_VER_DOCM_MGMT_IMG_IND 
		, TRIM( DOCM_FLAT_CANC_SYS_GEN_TYP_CD )              as                      DOCM_FLAT_CANC_SYS_GEN_TYP_CD 
		, TRIM( DOCM_TYP_VER_VOID_IND )                      as                              DOCM_TYP_VER_VOID_IND 
		, DOCM_TARND_RTN_DT                                  as                                  DOCM_TARND_RTN_DT 
		, DOCM_TARND_EXPT_DT_DRV                             as                             DOCM_TARND_EXPT_DT_DRV 
		, TRIM( DOCM_STT_TYP_CD )                            as                                    DOCM_STT_TYP_CD 
		, TRIM( DOCM_STT_TYP_NM )                            as                                    DOCM_STT_TYP_NM 
		, DOCM_STT_TYP_EFF_DT                                as                                DOCM_STT_TYP_EFF_DT 
		, TRIM( DOCM_STS_TYP_CD )                            as                                    DOCM_STS_TYP_CD 
		, TRIM( DOCM_STS_TYP_NM )                            as                                    DOCM_STS_TYP_NM 
		, DOCM_STS_TYP_EFF_DT                                as                                DOCM_STS_TYP_EFF_DT 
		, DOCM_EFF_DT                                        as                                        DOCM_EFF_DT 
		, DOCM_END_DT                                        as                                        DOCM_END_DT 
		, DOCM_TARND_EXPT_DD                                 as                                 DOCM_TARND_EXPT_DD 
		, DOCM_PND_DT                                        as                                        DOCM_PND_DT 
		, TRIM( DOCM_PND_STS_COMT_TXT )                      as                              DOCM_PND_STS_COMT_TXT 
		, TRIM( DOCM_CREA_EXCP_IND )                         as                                 DOCM_CREA_EXCP_IND 
		, TRIM( DOCM_DEL_EXCP_IND )                          as                                  DOCM_DEL_EXCP_IND 
		, TRIM( DOCM_EXCP_MSG_TXT )                          as                                  DOCM_EXCP_MSG_TXT 
		, TRIM( DOCM_SYS_GEN_IND )                           as                                   DOCM_SYS_GEN_IND 
		, TRIM( DOCM_LNK_NOTE_IND )                          as                                  DOCM_LNK_NOTE_IND 
		, TRIM( DOCM_REPRNT_IND )                            as                                    DOCM_REPRNT_IND 
		, TRIM( DOCM_OTHR_EXCP_IND )                         as                                 DOCM_OTHR_EXCP_IND 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, cast( AUDIT_USER_CREA_DTM as DATE )                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, cast( AUDIT_USER_UPDT_DTM as DATE )                as                                AUDIT_USER_UPDT_DTM 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_DOC
            ),
LOGIC_CLM as ( SELECT 
		  TRIM( AGRE_ID )                                    as                                            AGRE_ID 
		, TRIM( CLM_NO )                                     as                                             CLM_NO 
		from SRC_CLM
            ),
LOGIC_CR_USER as ( SELECT 
		  USER_ID                                            as                                            USER_ID 
		, TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
		from SRC_CR_USER
            ),
LOGIC_UP_USER as ( SELECT 
		  USER_ID                                            as                                            USER_ID 
		, TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
		from SRC_UP_USER
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
		, AUDIT_USER_CREA_DTM                                as                                 AUDIT_USER_CREA_DT
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                 AUDIT_USER_UPDT_DT
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_DOC   ), 
RENAME_CLM as ( SELECT 
		  AGRE_ID                                            as                                        CLM_AGRE_ID
		, CLM_NO                                             as                                             CLM_NO 
				FROM     LOGIC_CLM   ), 
RENAME_CR_USER as ( SELECT 
		  USER_ID                                            as                                         CR_USER_ID
		, USER_LGN_NM                                        as                                     CR_USER_LGN_NM 
				FROM     LOGIC_CR_USER   ), 
RENAME_UP_USER as ( SELECT 
		  USER_ID                                            as                                         UP_USER_ID
		, USER_LGN_NM                                        as                                     UP_USER_LGN_NM 
				FROM     LOGIC_UP_USER   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DOC                            as ( SELECT * from    RENAME_DOC   ),
FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),
FILTER_CR_USER                        as ( SELECT * from    RENAME_CR_USER   ),
FILTER_UP_USER                        as ( SELECT * from    RENAME_UP_USER   ),

---- JOIN LAYER ----

DOC as ( SELECT * 
				FROM  FILTER_DOC
				INNER JOIN FILTER_CLM ON  FILTER_DOC.AGRE_ID =  FILTER_CLM.CLM_AGRE_ID 
								LEFT JOIN FILTER_CR_USER ON  FILTER_DOC.AUDIT_USER_ID_CREA =  FILTER_CR_USER.CR_USER_ID 
								LEFT JOIN FILTER_UP_USER ON  FILTER_DOC.AUDIT_USER_ID_UPDT =  FILTER_UP_USER.UP_USER_ID  )
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
, CR_USER_ID
, CR_USER_LGN_NM
, UP_USER_ID
, UP_USER_LGN_NM
from DOC
      );
    