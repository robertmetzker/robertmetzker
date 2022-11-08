

---- SRC LAYER ----
WITH
SRC_CTS as ( SELECT *     from     STAGING.DST_CLAIM_TYPE_STATUS ),
//SRC_CTS as ( SELECT *     from     DST_CLAIM_TYPE_STATUS) ,

---- LOGIC LAYER ----


LOGIC_CTS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD 
		, CLAIM_TYPE_CHANGE_OVER_IND                         as                         CLAIM_TYPE_CHANGE_OVER_IND
		, CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD 
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD 
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD 
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM 
		, CLM_STT_TYP_NM                                     as                                     CLM_STT_TYP_NM 
		, CLM_STS_TYP_NM                                     as                                     CLM_STS_TYP_NM 
		, CLM_TRANS_RSN_TYP_NM                               as                               CLM_TRANS_RSN_TYP_NM 
		, CLAIM_WEB_STATUS_CODE                              as                              CLAIM_WEB_STATUS_CODE 
		, CLAIM_WEB_STATUS_DESC                              as                              CLAIM_WEB_STATUS_DESC 
		, CLAIM_WEB_STATUS_COMMENT                           as                           CLAIM_WEB_STATUS_COMMENT 
		from SRC_CTS
            )

---- RENAME LAYER ----
,

RENAME_CTS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CLM_TYP_CD                                         as                                    CLAIM_TYPE_CODE
		, CLAIM_TYPE_CHANGE_OVER_IND                         as                         CLAIM_TYPE_CHANGE_OVER_IND
		, CLM_STT_TYP_CD                                     as                                   CLAIM_STATE_CODE
		, CLM_STS_TYP_CD                                     as                                  CLAIM_STATUS_CODE
		, CLM_TRANS_RSN_TYP_CD                               as                           CLAIM_STATUS_REASON_CODE
		, CLM_TYP_NM                                         as                                    CLAIM_TYPE_DESC
		, CLM_STT_TYP_NM                                     as                                   CLAIM_STATE_DESC
		, CLM_STS_TYP_NM                                     as                                  CLAIM_STATUS_DESC
		, CLM_TRANS_RSN_TYP_NM                               as                           CLAIM_STATUS_REASON_DESC
		, CLAIM_WEB_STATUS_CODE                              as                              CLAIM_WEB_STATUS_CODE
		, CLAIM_WEB_STATUS_DESC                              as                              CLAIM_WEB_STATUS_DESC
		, CLAIM_WEB_STATUS_COMMENT                           as                           CLAIM_WEB_STATUS_COMMENT 
				FROM     LOGIC_CTS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CTS                            as ( SELECT * from    RENAME_CTS   ),

---- JOIN LAYER ----

 JOIN_CTS  as  ( SELECT * 
				FROM  FILTER_CTS )
 SELECT 
 UNIQUE_ID_KEY
,CLAIM_TYPE_CODE
,CLAIM_TYPE_CHANGE_OVER_IND
,CLAIM_STATE_CODE
,CLAIM_STATUS_CODE
,CLAIM_STATUS_REASON_CODE
,CLAIM_TYPE_DESC
,CLAIM_STATE_DESC
,CLAIM_STATUS_DESC
,CLAIM_STATUS_REASON_DESC
,CLAIM_WEB_STATUS_CODE
,CLAIM_WEB_STATUS_DESC
,CLAIM_WEB_STATUS_COMMENT
 FROM  JOIN_CTS