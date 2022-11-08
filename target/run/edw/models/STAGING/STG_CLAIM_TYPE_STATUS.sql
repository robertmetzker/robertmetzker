

      create or replace  table DEV_EDW.STAGING.STG_CLAIM_TYPE_STATUS  as
      (---- SRC LAYER ----
WITH
SRC_CLM_TYP as ( SELECT *     from    DEV_VIEWS.PCMP.CLAIM_TYPE ), 
SRC_CLM_STT_TYP as ( SELECT *     from    DEV_VIEWS.PCMP.CLAIM_STATE_TYPE ), 
SRC_CLM_STTS_TYP as ( SELECT *     from    DEV_VIEWS.PCMP.CLAIM_STATUS_TYPE ), 
SRC_CLM_TRNS_RSN_TYP as ( SELECT *     from    DEV_VIEWS.PCMP.CLAIM_TRANSITION_REASON_TYPE ), 
//SRC_CLM_TYP as ( SELECT *     from     CLAIM_TYPE) ,
//SRC_CLM_STT_TYP as ( SELECT *     from     CLAIM_STATE_TYPE) ,
//SRC_CLM_STTS_TYP as ( SELECT *     from     CLAIM_STATUS_TYPE) ,
//SRC_CLM_TRNS_RSN_TYP as ( SELECT *     from     CLAIM_TRANSITION_REASON_TYPE) ,

---- LOGIC LAYER ----


LOGIC_CLM_TYP as ( SELECT 
		  upper( TRIM( CLM_TYP_CD ) )                        as                                         CLM_TYP_CD 
		, upper( TRIM( CLM_TYP_NM ) )                        as                                         CLM_TYP_NM 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		from SRC_CLM_TYP
            ),

LOGIC_CLM_STT_TYP as ( SELECT 
		  upper( TRIM( CLM_STT_TYP_CD ) )                    as                                     CLM_STT_TYP_CD 
		, upper( TRIM( CLM_STT_TYP_NM ) )                    as                                     CLM_STT_TYP_NM 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		from SRC_CLM_STT_TYP
            ),

LOGIC_CLM_STTS_TYP as ( SELECT 
		  upper( TRIM( CLM_STS_TYP_CD ) )                    as                                     CLM_STS_TYP_CD 
		, upper( TRIM( CLM_STS_TYP_NM ) )                    as                                     CLM_STS_TYP_NM 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		from SRC_CLM_STTS_TYP
            ),

LOGIC_CLM_TRNS_RSN_TYP as ( SELECT 
		  upper( TRIM( CLM_TRANS_RSN_TYP_CD ) )              as                               CLM_TRANS_RSN_TYP_CD 
		, upper( TRIM( CLM_TRANS_RSN_TYP_NM ) )              as                               CLM_TRANS_RSN_TYP_NM 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		from SRC_CLM_TRNS_RSN_TYP
            )

---- RENAME LAYER ----
,

RENAME_CLM_TYP as ( SELECT 
		  CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM
		, VOID_IND                                           as                                   CLM_TYP_VOID_IND 
				FROM     LOGIC_CLM_TYP   ), 
RENAME_CLM_STT_TYP as ( SELECT 
		  CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD
		, CLM_STT_TYP_NM                                     as                                     CLM_STT_TYP_NM
		, VOID_IND                                           as                               CLM_STT_TYP_VOID_IND 
				FROM     LOGIC_CLM_STT_TYP   ), 
RENAME_CLM_STTS_TYP as ( SELECT 
		  CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD
		, CLM_STS_TYP_NM                                     as                                     CLM_STS_TYP_NM
		, VOID_IND                                           as                              CLM_STTS_TYP_VOID_IND 
				FROM     LOGIC_CLM_STTS_TYP   ), 
RENAME_CLM_TRNS_RSN_TYP as ( SELECT 
		  CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD
		, CLM_TRANS_RSN_TYP_NM                               as                               CLM_TRANS_RSN_TYP_NM
		, VOID_IND                                           as                          CLM_TRNS_RSN_TYP_VOID_IND 
				FROM     LOGIC_CLM_TRNS_RSN_TYP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CLM_TYP                        as ( SELECT * from    RENAME_CLM_TYP 
                                            WHERE CLM_TYP_VOID_IND ='N'  ),
FILTER_CLM_STT_TYP                    as ( SELECT * from    RENAME_CLM_STT_TYP 
                                            WHERE CLM_STT_TYP_VOID_IND ='N'  ),
FILTER_CLM_STTS_TYP                   as ( SELECT * from    RENAME_CLM_STTS_TYP 
                                            WHERE CLM_STTS_TYP_VOID_IND ='N'  ),
FILTER_CLM_TRNS_RSN_TYP               as ( SELECT * from    RENAME_CLM_TRNS_RSN_TYP 
                                            WHERE (CLM_TRNS_RSN_TYP_VOID_IND ='N' OR CLM_TRANS_RSN_TYP_CD = 'NOCOV') ),

---- JOIN LAYER ----

 JOIN_CLM_TRNS_RSN_TYP  as  ( SELECT * 
				FROM  FILTER_CLM_TRNS_RSN_TYP 
				CROSS JOIN FILTER_CLM_STT_TYP, FILTER_CLM_STTS_TYP, FILTER_CLM_TYP)
 SELECT 
  CLM_TYP_CD
, CLM_STT_TYP_CD
, CLM_STS_TYP_CD
, CLM_TRANS_RSN_TYP_CD
, CLM_TYP_NM
, CLM_STT_TYP_NM
, CLM_STS_TYP_NM
, CLM_TRANS_RSN_TYP_NM
FROM  JOIN_CLM_TRNS_RSN_TYP
      );
    