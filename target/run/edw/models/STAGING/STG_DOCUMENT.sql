

      create or replace  table DEV_EDW.STAGING.STG_DOCUMENT  as
      (---- SRC LAYER ----
WITH
SRC_DOC as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT ),
SRC_CLDX as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_DOCUMENT_XREF ),
SRC_CUDX as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_DOCUMENT_XREF ),
SRC_PPDX as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD_DOCUMENT_XREF ),
SRC_DTV as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT_TYPE_VERSION ),
SRC_DOT as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT_OCCURRENCE_TYPE ),
SRC_DPST as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT_PAPER_STOCK_TYPE ),
SRC_DT as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT_TYPE ),
SRC_DCT as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT_CATEGORY_TYPE ),
SRC_DTST as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT_TYPE_SOURCE_TYPE ),
SRC_DSTT as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT_STATE_TYPE ),
SRC_DSTS as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT_STATUS_TYPE ),
//SRC_DOC as ( SELECT *     from     DOCUMENT) ,
//SRC_CLDX as ( SELECT *     from     CLAIM_DOCUMENT_XREF) ,
//SRC_CUDX as ( SELECT *     from     CUSTOMER_DOCUMENT_XREF) ,
//SRC_PPDX as ( SELECT *     from     POLICY_PERIOD_DOCUMENT_XREF) ,
//SRC_DTV as ( SELECT *     from     DOCUMENT_TYPE_VERSION) ,
//SRC_DOT as ( SELECT *     from     DOCUMENT_OCCURRENCE_TYPE) ,
//SRC_DPST as ( SELECT *     from     DOCUMENT_PAPER_STOCK_TYPE) ,
//SRC_DT as ( SELECT *     from     DOCUMENT_TYPE) ,
//SRC_DCT as ( SELECT *     from     DOCUMENT_CATEGORY_TYPE) ,
//SRC_DTST as ( SELECT *     from     DOCUMENT_TYPE_SOURCE_TYPE) ,
//SRC_DSTT as ( SELECT *     from     DOCUMENT_STATE_TYPE) ,
//SRC_DSTS as ( SELECT *     from     DOCUMENT_STATUS_TYPE) ,

---- LOGIC LAYER ----

LOGIC_DOC as ( SELECT 
		  DOCM_ID                                            AS                                            DOCM_ID 
		, DOCM_NO                                            AS                                            DOCM_NO 
		, DOCM_TYP_VER_ID                                    AS                                    DOCM_TYP_VER_ID 
		, cast( DOCM_TARND_RTN_DT as DATE )                  AS                                  DOCM_TARND_RTN_DT 
		, cast( DOCM_TARND_EXPT_DT_DRV as DATE )             AS                             DOCM_TARND_EXPT_DT_DRV 
		, upper( TRIM( DOCM_STT_TYP_CD ) )                   AS                                    DOCM_STT_TYP_CD 
		, cast( DOCM_STT_TYP_EFF_DT as DATE )                AS                                DOCM_STT_TYP_EFF_DT 
		, upper( TRIM( DOCM_STS_TYP_CD ) )                   AS                                    DOCM_STS_TYP_CD 
		, cast( DOCM_STS_TYP_EFF_DT as DATE )                AS                                DOCM_STS_TYP_EFF_DT 
		, cast( DOCM_EFF_DT as DATE )                        AS                                        DOCM_EFF_DT 
		, cast( DOCM_END_DT as DATE )                        AS                                        DOCM_END_DT 
		, DOCM_TARND_EXPT_DD                                 AS                                 DOCM_TARND_EXPT_DD 
		, cast( DOCM_PND_DT as DATE )                        AS                                        DOCM_PND_DT 
		, upper( TRIM( DOCM_PND_STS_COMT_TXT ) )             AS                              DOCM_PND_STS_COMT_TXT 
		, upper( DOCM_CREA_EXCP_IND )                        AS                                 DOCM_CREA_EXCP_IND 
		, upper( DOCM_DEL_EXCP_IND )                         AS                                  DOCM_DEL_EXCP_IND 
		, upper( TRIM( DOCM_EXCP_MSG_TXT ) )                 AS                                  DOCM_EXCP_MSG_TXT 
		, upper( DOCM_SYS_GEN_IND )                          AS                                   DOCM_SYS_GEN_IND 
		, upper( DOCM_LNK_NOTE_IND )                         AS                                  DOCM_LNK_NOTE_IND 
		, upper( DOCM_REPRNT_IND )                           AS                                    DOCM_REPRNT_IND 
		, upper( DOCM_OTHR_EXCP_IND )                        AS                                 DOCM_OTHR_EXCP_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_DOC
            ),
LOGIC_CLDX as ( SELECT 
		  AGRE_ID                                            AS                                            AGRE_ID 
		, DOCM_ID                                            AS                                            DOCM_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CLDX
            ),
LOGIC_CUDX as ( SELECT 
		  CUST_ID                                            AS                                            CUST_ID 
		, DOCM_ID                                            AS                                            DOCM_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CUDX
            ),
LOGIC_PPDX as ( SELECT 
		  PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
		, DOCM_ID                                            AS                                            DOCM_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PPDX
            ),
LOGIC_DTV as ( SELECT 
		  DOCM_TYP_ID                                        AS                                        DOCM_TYP_ID 
		, DOCM_TYP_VER_NO                                    AS                                    DOCM_TYP_VER_NO 
		, upper( TRIM( DOCM_OCCR_TYP_CD ) )                  AS                                   DOCM_OCCR_TYP_CD 
		, upper( TRIM( DOCM_PPR_STK_TYP_CD ) )               AS                                DOCM_PPR_STK_TYP_CD 
		, cast( DOCM_TYP_VER_EFF_DT as DATE )                AS                                DOCM_TYP_VER_EFF_DT 
		, cast( DOCM_TYP_VER_END_DT as DATE )                AS                                DOCM_TYP_VER_END_DT 
		, upper( TRIM( DOCM_TYP_VER_EVNT_DESC ) )            AS                             DOCM_TYP_VER_EVNT_DESC 
		, upper( TRIM( DOCM_TYP_VER_TEMPL_FILE_NM ) )        AS                         DOCM_TYP_VER_TEMPL_FILE_NM 
		, upper( TRIM( DOCM_TYP_VER_BTCH_PRNT_TYP ) )        AS                         DOCM_TYP_VER_BTCH_PRNT_TYP 
		, upper( DOCM_TYP_VER_CREA_LOCK_STS_IND )            AS                     DOCM_TYP_VER_CREA_LOCK_STS_IND 
		, upper( DOCM_TYP_VER_CREA_EXCP_IND )                AS                         DOCM_TYP_VER_CREA_EXCP_IND 
		, upper( DOCM_TYP_VER_DEL_EXCP_IND )                 AS                          DOCM_TYP_VER_DEL_EXCP_IND 
		, upper( TRIM( DOCM_TYP_VER_EXCP_MSG_TXT ) )         AS                          DOCM_TYP_VER_EXCP_MSG_TXT 
		, upper( DOCM_TYP_VER_DUP_IND )                      AS                               DOCM_TYP_VER_DUP_IND 
		, upper( DOCM_TYP_VER_SYS_GEN_IND )                  AS                           DOCM_TYP_VER_SYS_GEN_IND 
		, upper( DOCM_TYP_VER_SYS_GEN_ONLY_IND )             AS                      DOCM_TYP_VER_SYS_GEN_ONLY_IND 
		, upper( DOCM_TYP_VER_CLM_DCSN_IND )                 AS                          DOCM_TYP_VER_CLM_DCSN_IND 
		, upper( DOCM_TYP_VER_CANC_PLCY_IND )                AS                         DOCM_TYP_VER_CANC_PLCY_IND 
		, upper( DOCM_TYP_VER_PRE_PRNT_ENCL_IND )            AS                     DOCM_TYP_VER_PRE_PRNT_ENCL_IND 
		, upper( DOCM_TYP_VER_LCL_DLVR_IND )                 AS                          DOCM_TYP_VER_LCL_DLVR_IND 
		, upper( DOCM_TYP_VER_BTCH_PRNT_IND )                AS                         DOCM_TYP_VER_BTCH_PRNT_IND 
		, upper( DOCM_TYP_VER_USR_CAN_DEL_IND )              AS                       DOCM_TYP_VER_USR_CAN_DEL_IND 
		, upper( DOCM_TYP_VER_MULTI_RDR_VER_IND )            AS                     DOCM_TYP_VER_MULTI_RDR_VER_IND 
		, upper( DOCM_TYP_VER_PRNT_DUPLX_IND )               AS                        DOCM_TYP_VER_PRNT_DUPLX_IND 
		, upper( DOCM_TYP_VER_DOCM_MGMT_IMG_IND )            AS                     DOCM_TYP_VER_DOCM_MGMT_IMG_IND 
		, upper( TRIM( DOCM_FLAT_CANC_SYS_GEN_TYP_CD ) )     AS                      DOCM_FLAT_CANC_SYS_GEN_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		, DOCM_TYP_VER_ID                                    AS                                    DOCM_TYP_VER_ID 
		from SRC_DTV
            ),
LOGIC_DOT as ( SELECT 
		  upper( TRIM( DOCM_OCCR_TYP_NM ) )                  AS                                   DOCM_OCCR_TYP_NM 
		, upper( TRIM( DOCM_OCCR_TYP_CD ) )                  AS                                   DOCM_OCCR_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_DOT
            ),
LOGIC_DPST as ( SELECT 
		  upper( TRIM( DOCM_PPR_STK_TYP_NM ) )               AS                                DOCM_PPR_STK_TYP_NM 
		, upper( TRIM( DOCM_PPR_STK_TYP_CD ) )               AS                                DOCM_PPR_STK_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_DPST
            ),
LOGIC_DT as ( SELECT 
		  upper( TRIM( DOCM_CTG_TYP_CD ) )                   AS                                    DOCM_CTG_TYP_CD 
		, upper( TRIM( DOCM_TYP_SRC_TYP_CD ) )               AS                                DOCM_TYP_SRC_TYP_CD 
		, upper( TRIM( DOCM_TYP_REF_NO ) )                   AS                                    DOCM_TYP_REF_NO 
		, upper( TRIM( DOCM_TYP_NM ) )                       AS                                        DOCM_TYP_NM 
		, upper( TRIM( DOCM_TYP_DESC ) )                     AS                                      DOCM_TYP_DESC 
		, DOCM_TYP_ID                                        AS                                        DOCM_TYP_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_DT
            ),
LOGIC_DCT as ( SELECT 
		  upper( TRIM( DOCM_CTG_TYP_NM ) )                   AS                                    DOCM_CTG_TYP_NM 
		, upper( TRIM( DOCM_CTG_TYP_CD ) )                   AS                                    DOCM_CTG_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_DCT
            ),
LOGIC_DTST as ( SELECT 
		  upper( TRIM( DOCM_TYP_SRC_TYP_NM ) )               AS                                DOCM_TYP_SRC_TYP_NM 
		, upper( TRIM( DOCM_TYP_SRC_TYP_CD ) )               AS                                DOCM_TYP_SRC_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_DTST
            ),
LOGIC_DSTT as ( SELECT 
		  upper( TRIM( DOCM_STT_TYP_NM ) )                   AS                                    DOCM_STT_TYP_NM 
		, upper( TRIM( DOCM_STT_TYP_CD ) )                   AS                                    DOCM_STT_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_DSTT
            ),
LOGIC_DSTS as ( SELECT 
		  upper( TRIM( DOCM_STS_TYP_NM ) )                   AS                                    DOCM_STS_TYP_NM 
		, upper( TRIM( DOCM_STS_TYP_CD ) )                   AS                                    DOCM_STS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_DSTS
            )

---- RENAME LAYER ----
,

RENAME_DOC as ( SELECT 
		  DOCM_ID                                            as                                            DOCM_ID
		, DOCM_NO                                            as                                            DOCM_NO
		, DOCM_TYP_VER_ID                                    as                                    DOCM_TYP_VER_ID
		, DOCM_TARND_RTN_DT                                  as                                  DOCM_TARND_RTN_DT
		, DOCM_TARND_EXPT_DT_DRV                             as                             DOCM_TARND_EXPT_DT_DRV
		, DOCM_STT_TYP_CD                                    as                                    DOCM_STT_TYP_CD
		, DOCM_STT_TYP_EFF_DT                                as                                DOCM_STT_TYP_EFF_DT
		, DOCM_STS_TYP_CD                                    as                                    DOCM_STS_TYP_CD
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
				FROM     LOGIC_DOC   ), 
RENAME_CLDX as ( SELECT 
		  AGRE_ID                                            as                                            AGRE_ID
		, DOCM_ID                                            as                                       CLDX_DOCM_ID
		, VOID_IND                                           as                                      CLDX_VOID_IND 
				FROM     LOGIC_CLDX   ), 
RENAME_CUDX as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID
		, DOCM_ID                                            as                                       CUDX_DOCM_ID
		, VOID_IND                                           as                                      CUDX_VOID_IND 
				FROM     LOGIC_CUDX   ), 
RENAME_PPDX as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, DOCM_ID                                            as                                       PPDX_DOCM_ID
		, VOID_IND                                           as                                      PPDX_VOID_IND 
				FROM     LOGIC_PPDX   ), 
RENAME_DTV as ( SELECT 
		  DOCM_TYP_ID                                        as                                        DOCM_TYP_ID
		, DOCM_TYP_VER_NO                                    as                                    DOCM_TYP_VER_NO
		, DOCM_OCCR_TYP_CD                                   as                                   DOCM_OCCR_TYP_CD
		, DOCM_PPR_STK_TYP_CD                                as                                DOCM_PPR_STK_TYP_CD
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
		, VOID_IND                                           as                              DOCM_TYP_VER_VOID_IND
		, DOCM_TYP_VER_ID                                    as                                DTV_DOCM_TYP_VER_ID 
				FROM     LOGIC_DTV   ), 
RENAME_DOT as ( SELECT 
		  DOCM_OCCR_TYP_NM                                   as                                   DOCM_OCCR_TYP_NM
		, DOCM_OCCR_TYP_CD                                   as                               DOT_DOCM_OCCR_TYP_CD
		, VOID_IND                                           as                                       DOT_VOID_IND 
				FROM     LOGIC_DOT   ), 
RENAME_DPST as ( SELECT 
		  DOCM_PPR_STK_TYP_NM                                as                                DOCM_PPR_STK_TYP_NM
		, DOCM_PPR_STK_TYP_CD                                as                           DPST_DOCM_PPR_STK_TYP_CD
		, VOID_IND                                           as                                      DPST_VOID_IND 
				FROM     LOGIC_DPST   ), 
RENAME_DT as ( SELECT 
		  DOCM_CTG_TYP_CD                                    as                                    DOCM_CTG_TYP_CD
		, DOCM_TYP_SRC_TYP_CD                                as                                DOCM_TYP_SRC_TYP_CD
		, DOCM_TYP_REF_NO                                    as                                    DOCM_TYP_REF_NO
		, DOCM_TYP_NM                                        as                                        DOCM_TYP_NM
		, DOCM_TYP_DESC                                      as                                      DOCM_TYP_DESC
		, DOCM_TYP_ID                                        as                                     DT_DOCM_TYP_ID
		, VOID_IND                                           as                                        DT_VOID_IND 
				FROM     LOGIC_DT   ), 
RENAME_DCT as ( SELECT 
		  DOCM_CTG_TYP_NM                                    as                                    DOCM_CTG_TYP_NM
		, DOCM_CTG_TYP_CD                                    as                                DCT_DOCM_CTG_TYP_CD
		, VOID_IND                                           as                                       DCT_VOID_IND 
				FROM     LOGIC_DCT   ), 
RENAME_DTST as ( SELECT 
		  DOCM_TYP_SRC_TYP_NM                                as                                DOCM_TYP_SRC_TYP_NM
		, DOCM_TYP_SRC_TYP_CD                                as                           DTST_DOCM_TYP_SRC_TYP_CD
		, VOID_IND                                           as                                      DTST_VOID_IND 
				FROM     LOGIC_DTST   ), 
RENAME_DSTT as ( SELECT 
		  DOCM_STT_TYP_NM                                    as                                    DOCM_STT_TYP_NM
		, DOCM_STT_TYP_CD                                    as                               DSTT_DOCM_STT_TYP_CD
		, VOID_IND                                           as                                      DSTT_VOID_IND 
				FROM     LOGIC_DSTT   ), 
RENAME_DSTS as ( SELECT 
		  DOCM_STS_TYP_NM                                    as                                    DOCM_STS_TYP_NM
		, DOCM_STS_TYP_CD                                    as                               DSTS_DOCM_STS_TYP_CD
		, VOID_IND                                           as                                      DSTS_VOID_IND 
				FROM     LOGIC_DSTS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DOC                            as ( SELECT * from    RENAME_DOC   ),
FILTER_CLDX                           as ( SELECT * from    RENAME_CLDX 
				WHERE CLDX_VOID_IND = 'N'  ),
FILTER_CUDX                           as ( SELECT * from    RENAME_CUDX 
				WHERE CUDX_VOID_IND = 'N'  ),
FILTER_PPDX                           as ( SELECT * from    RENAME_PPDX 
				WHERE PPDX_VOID_IND = 'N'  ),
FILTER_DTV                            as ( SELECT * from    RENAME_DTV   ),
FILTER_DT                             as ( SELECT * from    RENAME_DT 
				WHERE DT_VOID_IND = 'N'  ),
FILTER_DSTT                           as ( SELECT * from    RENAME_DSTT 
				WHERE DSTT_VOID_IND = 'N'  ),
FILTER_DSTS                           as ( SELECT * from    RENAME_DSTS 
				WHERE DSTS_VOID_IND = 'N'  ),
FILTER_DCT                            as ( SELECT * from    RENAME_DCT 
				WHERE DCT_VOID_IND = 'N'  ),
FILTER_DTST                           as ( SELECT * from    RENAME_DTST 
				WHERE DTST_VOID_IND = 'N'  ),
FILTER_DOT                            as ( SELECT * from    RENAME_DOT 
				WHERE DOT_VOID_IND = 'N'  ),
FILTER_DPST                           as ( SELECT * from    RENAME_DPST 
				WHERE DPST_VOID_IND = 'N'  ),

---- JOIN LAYER ----

DT as ( SELECT * 
				FROM  FILTER_DT
				LEFT JOIN FILTER_DCT ON  FILTER_DT.DOCM_CTG_TYP_CD =  FILTER_DCT.DCT_DOCM_CTG_TYP_CD 
								LEFT JOIN FILTER_DTST ON  FILTER_DT.DOCM_TYP_SRC_TYP_CD =  FILTER_DTST.DTST_DOCM_TYP_SRC_TYP_CD  ),
DTV as ( SELECT * 
				FROM  FILTER_DTV
				LEFT JOIN DT ON  FILTER_DTV.DOCM_TYP_ID = DT.DT_DOCM_TYP_ID 
						LEFT JOIN FILTER_DOT ON  FILTER_DTV.DOCM_OCCR_TYP_CD =  FILTER_DOT.DOT_DOCM_OCCR_TYP_CD 
								LEFT JOIN FILTER_DPST ON  FILTER_DTV.DOCM_PPR_STK_TYP_CD =  FILTER_DPST.DPST_DOCM_PPR_STK_TYP_CD  ),
DOC as ( SELECT * 
				FROM  FILTER_DOC
				LEFT JOIN FILTER_CLDX ON  FILTER_DOC.DOCM_ID =  FILTER_CLDX.CLDX_DOCM_ID 
								LEFT JOIN FILTER_CUDX ON  FILTER_DOC.DOCM_ID =  FILTER_CUDX.CUDX_DOCM_ID 
								LEFT JOIN FILTER_PPDX ON  FILTER_DOC.DOCM_ID =  FILTER_PPDX.PPDX_DOCM_ID 
						LEFT JOIN DTV ON  FILTER_DOC.DOCM_TYP_VER_ID = DTV.DTV_DOCM_TYP_VER_ID 
								LEFT JOIN FILTER_DSTT ON  FILTER_DOC.DOCM_STT_TYP_CD =  FILTER_DSTT.DSTT_DOCM_STT_TYP_CD 
								LEFT JOIN FILTER_DSTS ON  FILTER_DOC.DOCM_STS_TYP_CD =  FILTER_DSTS.DSTS_DOCM_STS_TYP_CD  )
SELECT * 
from DOC
      );
    