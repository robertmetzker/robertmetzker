

      create or replace  table DEV_EDW.STAGING.STG_PAYMENT_REQUEST_CODE  as
      (---- SRC LAYER ----
WITH
SRC_PT             as ( SELECT *     FROM     DEV_VIEWS.PCMP.PAYMENT_MEDIA_PREFERENCE_TYPE ),
SRC_PRT            as ( SELECT *     FROM     DEV_VIEWS.PCMP.PAYMENT_REQUEST_TYPE ),
SRC_ASCT           as ( SELECT *     FROM     DEV_VIEWS.PCMP.APPLICATION_SUB_CONTEXT_TYPE ),
SRC_PRS            as ( SELECT *     FROM     DEV_VIEWS.PCMP.PAYMENT_REQUEST_STATUS_TYPE ),
SRC_PRST           as ( SELECT *     FROM     DEV_VIEWS.PCMP.PAYMENT_REQUEST_STATE_TYPE ),
SRC_PRSRT          as ( SELECT *     FROM     DEV_VIEWS.PCMP.PAYMENT_REQUEST_STATUS_RSN_TYP ),
//SRC_PT             as ( SELECT *     FROM     PAYMENT_MEDIA_PREFERENCE_TYPE) ,
//SRC_PRT            as ( SELECT *     FROM     PAYMENT_REQUEST_TYPE) ,
//SRC_ASCT           as ( SELECT *     FROM     APPLICATION_SUB_CONTEXT_TYPE) ,
//SRC_PRS            as ( SELECT *     FROM     PAYMENT_REQUEST_STATUS_TYPE) ,
//SRC_PRST           as ( SELECT *     FROM     PAYMENT_REQUEST_STATE_TYPE) ,
//SRC_PRSRT          as ( SELECT *     FROM     PAYMENT_REQUEST_STATUS_RSN_TYP) ,

---- LOGIC LAYER ----


LOGIC_PT as ( SELECT 
		  upper( TRIM( PAY_MEDA_PREF_TYP_CD ) )              as                               PAY_MEDA_PREF_TYP_CD 
		, upper( TRIM( PAY_MEDA_PREF_TYP_NM ) )              as                               PAY_MEDA_PREF_TYP_NM 
		FROM SRC_PT
            ),

LOGIC_PRT as ( SELECT 
		  upper( TRIM( PAY_REQS_TYP_CD ) )                   as                                    PAY_REQS_TYP_CD 
		, upper( TRIM( PAY_REQS_TYP_NM ) )                   as                                    PAY_REQS_TYP_NM 
		, upper( TRIM( APP_CNTX_TYP_CD ) )                   as                                    APP_CNTX_TYP_CD 
		, upper( TRIM( APP_SUB_CNTX_TYP_CD ) )               as                                APP_SUB_CNTX_TYP_CD 
		FROM SRC_PRT
            ),

LOGIC_ASCT as ( SELECT 
		  upper( TRIM( APP_SUB_CNTX_TYP_NM ) )               as                                APP_SUB_CNTX_TYP_NM 
		, upper( TRIM( APP_SUB_CNTX_TYP_CD ) )               as                                APP_SUB_CNTX_TYP_CD 
		FROM SRC_ASCT
            ),

LOGIC_PRS as ( SELECT 
		  upper( TRIM( PAY_REQS_STT_TYP_CD ) )               as                                PAY_REQS_STT_TYP_CD 
		, upper( TRIM( PAY_REQS_STS_TYP_CD ) )               as                                PAY_REQS_STS_TYP_CD 
		, upper( TRIM( PAY_REQS_STS_TYP_NM ) )               as                                PAY_REQS_STS_TYP_NM 
		FROM SRC_PRS
            ),

LOGIC_PRST as ( SELECT 
		  upper( TRIM( PAY_REQS_STT_TYP_NM ) )               as                                PAY_REQS_STT_TYP_NM 
		, upper( TRIM( PAY_REQS_STT_TYP_CD ) )               as                                PAY_REQS_STT_TYP_CD 
		FROM SRC_PRST
            ),

LOGIC_PRSRT as ( SELECT 
		  upper( TRIM( PAY_REQS_STS_RSN_TYP_CD ) )           as                            PAY_REQS_STS_RSN_TYP_CD 
		, upper( TRIM( PAY_REQS_STS_RSN_TYP_NM ) )           as                            PAY_REQS_STS_RSN_TYP_NM 
		FROM SRC_PRSRT
            )

---- RENAME LAYER ----
,

RENAME_PT         as ( SELECT 
		  PAY_MEDA_PREF_TYP_CD                               as                               PAY_MEDA_PREF_TYP_CD
		, PAY_MEDA_PREF_TYP_NM                               as                               PAY_MEDA_PREF_TYP_NM 
				FROM     LOGIC_PT   ), 
RENAME_PRT        as ( SELECT 
		  PAY_REQS_TYP_CD                                    as                                    PAY_REQS_TYP_CD
		, PAY_REQS_TYP_NM                                    as                                    PAY_REQS_TYP_NM
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD
		, APP_SUB_CNTX_TYP_CD                                as                                APP_SUB_CNTX_TYP_CD 
				FROM     LOGIC_PRT   ), 
RENAME_ASCT       as ( SELECT 
		  APP_SUB_CNTX_TYP_NM                                as                                APP_SUB_CNTX_TYP_NM
		, APP_SUB_CNTX_TYP_CD                                as                           ASCT_APP_SUB_CNTX_TYP_CD 
				FROM     LOGIC_ASCT   ), 
RENAME_PRS        as ( SELECT 
		  PAY_REQS_STT_TYP_CD                                as                                PAY_REQS_STT_TYP_CD
		, PAY_REQS_STS_TYP_CD                                as                                PAY_REQS_STS_TYP_CD
		, PAY_REQS_STS_TYP_NM                                as                                PAY_REQS_STS_TYP_NM 
				FROM     LOGIC_PRS   ), 
RENAME_PRST       as ( SELECT 
		  PAY_REQS_STT_TYP_NM                                as                                PAY_REQS_STT_TYP_NM
		, PAY_REQS_STT_TYP_CD                                as                           PRST_PAY_REQS_STT_TYP_CD 
				FROM     LOGIC_PRST   ), 
RENAME_PRSRT      as ( SELECT 
		  PAY_REQS_STS_RSN_TYP_CD                            as                            PAY_REQS_STS_RSN_TYP_CD
		, PAY_REQS_STS_RSN_TYP_NM                            as                            PAY_REQS_STS_RSN_TYP_NM 
				FROM     LOGIC_PRSRT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PT                             as ( SELECT * FROM    RENAME_PT   ),
FILTER_PRT                            as ( SELECT * FROM    RENAME_PRT   ),
FILTER_ASCT                           as ( SELECT * FROM    RENAME_ASCT   ),
FILTER_PRS                            as ( SELECT * FROM    RENAME_PRS   ),
FILTER_PRST                           as ( SELECT * FROM    RENAME_PRST   ),
FILTER_PRSRT                          as ( SELECT * FROM    RENAME_PRSRT   ),

---- JOIN LAYER ----


PRT as ( SELECT * 
				FROM  FILTER_PRT
				LEFT JOIN  FILTER_ASCT ON  FILTER_PRT.APP_SUB_CNTX_TYP_CD =  FILTER_ASCT.ASCT_APP_SUB_CNTX_TYP_CD  ),
PRS as ( SELECT * 
				FROM  FILTER_PRS
				LEFT JOIN  FILTER_PRST ON  FILTER_PRS.PAY_REQS_STT_TYP_CD =  FILTER_PRST.PRST_PAY_REQS_STT_TYP_CD  )
, PR AS( SELECT * FROM FILTER_PT 
 FULL OUTER JOIN PRT ON 1=1
 FULL OUTER JOIN PRS ON 1=1
 FULL OUTER JOIN FILTER_PRSRT ON 1=1)

SELECT DISTINCT
		  PAY_MEDA_PREF_TYP_CD
		, PAY_MEDA_PREF_TYP_NM
		, PAY_REQS_TYP_CD
		, PAY_REQS_TYP_NM
		, APP_CNTX_TYP_CD
		, APP_SUB_CNTX_TYP_CD
		, APP_SUB_CNTX_TYP_NM
		, PAY_REQS_STT_TYP_CD
		, PAY_REQS_STT_TYP_NM
		, PAY_REQS_STS_TYP_CD
		, PAY_REQS_STS_TYP_NM
		, PAY_REQS_STS_RSN_TYP_CD
		, PAY_REQS_STS_RSN_TYP_NM 
FROM PR
      );
    