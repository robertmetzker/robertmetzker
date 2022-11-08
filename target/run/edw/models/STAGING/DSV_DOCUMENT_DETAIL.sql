
  create or replace  view DEV_EDW.STAGING.DSV_DOCUMENT_DETAIL  as (
    

---- SRC LAYER ----
WITH
SRC_DOC            as ( SELECT *     FROM     STAGING.DST_DOCUMENT_DETAIL ),
//SRC_DOC            as ( SELECT *     FROM     DST_DOCUMENT_DETAIL) ,

---- LOGIC LAYER ----


LOGIC_DOC as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, DOCM_STT_TYP_CD                                    as                                    DOCM_STT_TYP_CD 
		, DOCM_STT_TYP_NM                                    as                                    DOCM_STT_TYP_NM 
		, DOCM_STS_TYP_CD                                    as                                    DOCM_STS_TYP_CD 
		, DOCM_STS_TYP_NM                                    as                                    DOCM_STS_TYP_NM 
		, DOCM_SYS_GEN_IND                                   as                                   DOCM_SYS_GEN_IND 
		, DOCM_TYP_VER_DUP_IND                               as                               DOCM_TYP_VER_DUP_IND 
		, DOCM_TYP_VER_SYS_GEN_IND                           as                           DOCM_TYP_VER_SYS_GEN_IND 
		, DOCM_TYP_VER_SYS_GEN_ONLY_IND                      as                      DOCM_TYP_VER_SYS_GEN_ONLY_IND 
		, DOCM_TYP_VER_CANC_PLCY_IND                         as                         DOCM_TYP_VER_CANC_PLCY_IND 
		, DOCM_TYP_VER_PRE_PRNT_ENCL_IND                     as                     DOCM_TYP_VER_PRE_PRNT_ENCL_IND 
		, DOCM_TYP_VER_BTCH_PRNT_IND                         as                         DOCM_TYP_VER_BTCH_PRNT_IND 
		, DOCM_TYP_VER_USR_CAN_DEL_IND                       as                       DOCM_TYP_VER_USR_CAN_DEL_IND 
		, DOCM_TYP_VER_MULTI_RDR_VER_IND                     as                     DOCM_TYP_VER_MULTI_RDR_VER_IND 
		, DOCM_TYP_VER_PRNT_DUPLX_IND                        as                        DOCM_TYP_VER_PRNT_DUPLX_IND 
		FROM SRC_DOC
            )

---- RENAME LAYER ----
,

RENAME_DOC        as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, DOCM_STT_TYP_CD                                    as                                DOCUMENT_STATE_CODE
		, DOCM_STT_TYP_NM                                    as                                DOCUMENT_STATE_DESC
		, DOCM_STS_TYP_CD                                    as                               DOCUMENT_STATUS_CODE
		, DOCM_STS_TYP_NM                                    as                               DOCUMENT_STATUS_DESC
		, DOCM_SYS_GEN_IND                                   as                      DOCUMENT_SYSTEM_GENERATED_IND
		, DOCM_TYP_VER_DUP_IND                               as                DOCUMENT_TYPE_VERSION_DUPLICATE_IND
		, DOCM_TYP_VER_SYS_GEN_IND                           as         DOCUMENT_TYPE_VERSION_SYSTEM_GENERATED_IND
		, DOCM_TYP_VER_SYS_GEN_ONLY_IND                      as    DOCUMENT_TYPE_VERSION_SYSTEM_GENERATED_ONLY_IND
		, DOCM_TYP_VER_CANC_PLCY_IND                         as            DOCUMENT_TYPE_VERSION_CANCEL_POLICY_IND
		, DOCM_TYP_VER_PRE_PRNT_ENCL_IND                     as      DOCUMENT_TYPE_VERSION_PRE_PRINT_ENCLOSURE_IND
		, DOCM_TYP_VER_BTCH_PRNT_IND                         as                           DOCUMENT_BATCH_PRINT_IND
		, DOCM_TYP_VER_USR_CAN_DEL_IND                       as                       DOCUMENT_USER_CAN_DELETE_IND
		, DOCM_TYP_VER_MULTI_RDR_VER_IND                     as             DOCUMENT_MULTIPLE_RENDERED_VERSION_IND
		, DOCM_TYP_VER_PRNT_DUPLX_IND                        as                          DOCUMENT_PRINT_DUPLEX_IND 
				FROM     LOGIC_DOC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DOC                            as ( SELECT * FROM    RENAME_DOC   ),

---- JOIN LAYER ----

 JOIN_DOC         as  ( SELECT * 
				FROM  FILTER_DOC )
 SELECT * FROM  JOIN_DOC
  );
