
  create or replace  view DEV_EDW.STAGING.DSV_PAYMENT_REQUEST_CODE  as (
    

---- SRC LAYER ----
WITH
SRC_PR             as ( SELECT *     FROM     STAGING.DST_PAYMENT_REQUEST_CODE ),
//SRC_PR             as ( SELECT *     FROM     DST_PAYMENT_REQUEST) ,

---- LOGIC LAYER ----


LOGIC_PR as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, PAY_MEDA_PREF_TYP_CD                               as                               PAY_MEDA_PREF_TYP_CD 
		, PAY_REQS_TYP_CD                                    as                                    PAY_REQS_TYP_CD 
		, PAY_REQS_STT_TYP_CD                                as                                PAY_REQS_STT_TYP_CD 
		, PAY_REQS_STS_TYP_CD                                as                                PAY_REQS_STS_TYP_CD 
		, PAY_REQS_STS_RSN_TYP_CD                            as                            PAY_REQS_STS_RSN_TYP_CD 
		, PAY_MEDA_PREF_TYP_NM                               as                               PAY_MEDA_PREF_TYP_NM 
		, PAY_REQS_TYP_NM                                    as                                    PAY_REQS_TYP_NM 
		, PAY_REQS_STT_TYP_NM                                as                                PAY_REQS_STT_TYP_NM 
		, PAY_REQS_STS_TYP_NM                                as                                PAY_REQS_STS_TYP_NM 
		, PAY_REQS_STS_RSN_TYP_NM                            as                            PAY_REQS_STS_RSN_TYP_NM 
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD 
		, APP_SUB_CNTX_TYP_CD                                as                                APP_SUB_CNTX_TYP_CD 
		, APP_SUB_CNTX_TYP_NM                                as                                APP_SUB_CNTX_TYP_NM 
		FROM SRC_PR
            )

---- RENAME LAYER ----
,

RENAME_PR         as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, PAY_MEDA_PREF_TYP_CD                               as                     PAY_MEDIA_PREFERENCE_TYPE_CODE
		, PAY_REQS_TYP_CD                                    as                              PAY_REQUEST_TYPE_CODE
		, PAY_REQS_STT_TYP_CD                                as                        PAY_REQUEST_STATE_TYPE_CODE
		, PAY_REQS_STS_TYP_CD                                as                       PAY_REQUEST_STATUS_TYPE_CODE
		, PAY_REQS_STS_RSN_TYP_CD                            as                PAY_REQUEST_STATUS_REASON_TYPE_CODE
		, PAY_MEDA_PREF_TYP_NM                               as                     PAY_MEDIA_PREFERENCE_TYPE_DESC
		, PAY_REQS_TYP_NM                                    as                              PAY_REQUEST_TYPE_DESC
		, PAY_REQS_STT_TYP_NM                                as                        PAY_REQUEST_STATE_TYPE_DESC
		, PAY_REQS_STS_TYP_NM                                as                       PAY_REQUEST_STATUS_TYPE_DESC
		, PAY_REQS_STS_RSN_TYP_NM                            as                PAY_REQUEST_STATUS_REASON_TYPE_DESC
		, APP_CNTX_TYP_CD                                    as                                  CONTEXT_TYPE_CODE
		, APP_SUB_CNTX_TYP_CD                                as                              SUB_CONTEXT_TYPE_CODE
		, APP_SUB_CNTX_TYP_NM                                as                              SUB_CONTEXT_TYPE_DESC 
				FROM     LOGIC_PR   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PR                             as ( SELECT * FROM    RENAME_PR   ),

---- JOIN LAYER ----

 JOIN_PR          as  ( SELECT * 
				FROM  FILTER_PR )
 SELECT * FROM  JOIN_PR
  );
