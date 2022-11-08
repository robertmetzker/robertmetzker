

      create or replace  table DEV_EDW.STAGING.STG_DOCUMENT_TYPE_VERSION  as
      (---- SRC LAYER ----
WITH
SRC_DTV as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT_TYPE_VERSION ),
//SRC_DTV as ( SELECT *     from     DOCUMENT_TYPE_VERSION) ,

---- LOGIC LAYER ----

LOGIC_DTV as ( SELECT 
		  DOCM_TYP_VER_ID                                    as                                    DOCM_TYP_VER_ID 
		, CNCURNCY_ID                                        as                                        CNCURNCY_ID 
		, DOCM_TYP_ID                                        as                                        DOCM_TYP_ID 
		, DOCM_TYP_VER_NO                                    as                                    DOCM_TYP_VER_NO 
		, upper( TRIM( DOCM_OCCR_TYP_CD ) )                  as                                   DOCM_OCCR_TYP_CD 
		, DOCM_DLVR_DVC_TYP_ID                               as                               DOCM_DLVR_DVC_TYP_ID 
		, CDCDCX_ID                                          as                                          CDCDCX_ID 
		, upper( TRIM( DOCM_PPR_STK_TYP_CD ) )               as                                DOCM_PPR_STK_TYP_CD 
		, cast( DOCM_TYP_VER_EFF_DT as DATE )                as                                DOCM_TYP_VER_EFF_DT 
		, cast( DOCM_TYP_VER_END_DT as DATE )                as                                DOCM_TYP_VER_END_DT 
		, upper( TRIM( DOCM_TYP_VER_EVNT_DESC ) )            as                             DOCM_TYP_VER_EVNT_DESC 
		, upper( TRIM( DOCM_TYP_VER_TEMPL_FILE_NM ) )        as                         DOCM_TYP_VER_TEMPL_FILE_NM 
		, upper( DOCM_TYP_VER_CREA_EXCP_IND )                as                         DOCM_TYP_VER_CREA_EXCP_IND 
		, upper( DOCM_TYP_VER_DEL_EXCP_IND )                 as                          DOCM_TYP_VER_DEL_EXCP_IND 
		, upper( TRIM( DOCM_TYP_VER_EXCP_MSG_TXT ) )         as                          DOCM_TYP_VER_EXCP_MSG_TXT 
		, upper( DOCM_TYP_VER_DUP_IND )                      as                               DOCM_TYP_VER_DUP_IND 
		, upper( DOCM_TYP_VER_SYS_GEN_IND )                  as                           DOCM_TYP_VER_SYS_GEN_IND 
		, upper( DOCM_TYP_VER_SYS_GEN_ONLY_IND )             as                      DOCM_TYP_VER_SYS_GEN_ONLY_IND 
		, upper( DOCM_TYP_VER_CLM_DCSN_IND )                 as                          DOCM_TYP_VER_CLM_DCSN_IND 
		, upper( DOCM_TYP_VER_CANC_PLCY_IND )                as                         DOCM_TYP_VER_CANC_PLCY_IND 
		, upper( DOCM_TYP_VER_PRE_PRNT_ENCL_IND )            as                     DOCM_TYP_VER_PRE_PRNT_ENCL_IND 
		, upper( DOCM_TYP_VER_LCL_DLVR_IND )                 as                          DOCM_TYP_VER_LCL_DLVR_IND 
		, upper( DOCM_TYP_VER_BTCH_PRNT_IND )                as                         DOCM_TYP_VER_BTCH_PRNT_IND 
		, upper( DOCM_TYP_VER_USR_CAN_DEL_IND )              as                       DOCM_TYP_VER_USR_CAN_DEL_IND 
		, upper( DOCM_TYP_VER_MULTI_RDR_VER_IND )            as                     DOCM_TYP_VER_MULTI_RDR_VER_IND 
		, upper( DOCM_TYP_VER_PRNT_DUPLX_IND )               as                        DOCM_TYP_VER_PRNT_DUPLX_IND 
		, upper( DOCM_TYP_VER_DOCM_MGMT_IMG_IND )            as                     DOCM_TYP_VER_DOCM_MGMT_IMG_IND 
		, INOW_DOCM_TYP_ID                                   as                                   INOW_DOCM_TYP_ID 
		, upper( TRIM( DOCM_TYP_VER_REF_NO_SUFX ) )          as                           DOCM_TYP_VER_REF_NO_SUFX 
		, upper( TRIM( DOCM_FLAT_CANC_SYS_GEN_TYP_CD ) )     as                      DOCM_FLAT_CANC_SYS_GEN_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		, upper( TRIM( DOCM_TYP_VER_BTCH_PRNT_TYP ) )        as                         DOCM_TYP_VER_BTCH_PRNT_TYP 
		, upper( DOCM_TYP_VER_CREA_LOCK_STS_IND )            as                     DOCM_TYP_VER_CREA_LOCK_STS_IND 
		, upper( TRIM( ESIGN_TEMPL_NM ) )                    as                                     ESIGN_TEMPL_NM 
		, upper( DOCM_TYP_VER_INTERACTIVE_IND )              as                       DOCM_TYP_VER_INTERACTIVE_IND 
		from SRC_DTV
            )

---- RENAME LAYER ----
,

RENAME_DTV as ( SELECT 
		  DOCM_TYP_VER_ID                                    as                                    DOCM_TYP_VER_ID
		, CNCURNCY_ID                                        as                                        CNCURNCY_ID
		, DOCM_TYP_ID                                        as                                        DOCM_TYP_ID
		, DOCM_TYP_VER_NO                                    as                                    DOCM_TYP_VER_NO
		, DOCM_OCCR_TYP_CD                                   as                                   DOCM_OCCR_TYP_CD
		, DOCM_DLVR_DVC_TYP_ID                               as                               DOCM_DLVR_DVC_TYP_ID
		, CDCDCX_ID                                          as                                          CDCDCX_ID
		, DOCM_PPR_STK_TYP_CD                                as                                DOCM_PPR_STK_TYP_CD
		, DOCM_TYP_VER_EFF_DT                                as                                DOCM_TYP_VER_EFF_DT
		, DOCM_TYP_VER_END_DT                                as                                DOCM_TYP_VER_END_DT
		, DOCM_TYP_VER_EVNT_DESC                             as                             DOCM_TYP_VER_EVNT_DESC
		, DOCM_TYP_VER_TEMPL_FILE_NM                         as                         DOCM_TYP_VER_TEMPL_FILE_NM
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
		, INOW_DOCM_TYP_ID                                   as                                   INOW_DOCM_TYP_ID
		, DOCM_TYP_VER_REF_NO_SUFX                           as                           DOCM_TYP_VER_REF_NO_SUFX
		, DOCM_FLAT_CANC_SYS_GEN_TYP_CD                      as                      DOCM_FLAT_CANC_SYS_GEN_TYP_CD
		, VOID_IND                                           as                                           VOID_IND
		, DOCM_TYP_VER_BTCH_PRNT_TYP                         as                         DOCM_TYP_VER_BTCH_PRNT_TYP
		, DOCM_TYP_VER_CREA_LOCK_STS_IND                     as                     DOCM_TYP_VER_CREA_LOCK_STS_IND
		, ESIGN_TEMPL_NM                                     as                                     ESIGN_TEMPL_NM
		, DOCM_TYP_VER_INTERACTIVE_IND                       as                       DOCM_TYP_VER_INTERACTIVE_IND 
				FROM     LOGIC_DTV   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DTV                            as ( SELECT * from    RENAME_DTV 
                                            WHERE VOID_IND = 'N'  ),

---- JOIN LAYER ----

 JOIN_DTV  as  ( SELECT * 
				FROM  FILTER_DTV )
 SELECT 
  DOCM_TYP_VER_ID
, CNCURNCY_ID
, DOCM_TYP_ID
, DOCM_TYP_VER_NO
, DOCM_OCCR_TYP_CD
, DOCM_DLVR_DVC_TYP_ID
, CDCDCX_ID
, DOCM_PPR_STK_TYP_CD
, DOCM_TYP_VER_EFF_DT
, DOCM_TYP_VER_END_DT
, DOCM_TYP_VER_EVNT_DESC
, DOCM_TYP_VER_TEMPL_FILE_NM
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
, INOW_DOCM_TYP_ID
, DOCM_TYP_VER_REF_NO_SUFX
, DOCM_FLAT_CANC_SYS_GEN_TYP_CD
, VOID_IND
, DOCM_TYP_VER_BTCH_PRNT_TYP
, DOCM_TYP_VER_CREA_LOCK_STS_IND
, ESIGN_TEMPL_NM
, DOCM_TYP_VER_INTERACTIVE_IND
FROM  JOIN_DTV
      );
    