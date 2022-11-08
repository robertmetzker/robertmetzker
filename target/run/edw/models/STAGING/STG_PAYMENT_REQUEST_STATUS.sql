

      create or replace  table DEV_EDW.STAGING.STG_PAYMENT_REQUEST_STATUS  as
      (---- SRC LAYER ----
WITH
SRC_PRS as ( SELECT *     from    DEV_VIEWS.PCMP.PAYMENT_REQUEST_STATUS ),
SRC_PRST as ( SELECT *     from    DEV_VIEWS.PCMP.PAYMENT_REQUEST_STATUS_TYPE ),
SRC_PRSTT as ( SELECT *     from    DEV_VIEWS.PCMP.PAYMENT_REQUEST_STATE_TYPE ),
SRC_PRSRT as ( SELECT *     from    DEV_VIEWS.PCMP.PAYMENT_REQUEST_STATUS_RSN_TYP ),
//SRC_PRS as ( SELECT *     from     PAYMENT_REQUEST_STATUS) ,
//SRC_PRST as ( SELECT *     from     PAYMENT_REQUEST_STATUS_TYPE) ,
//SRC_PRSTT as ( SELECT *     from     PAYMENT_REQUEST_STATE_TYPE) ,
//SRC_PRSRT as ( SELECT *     from     PAYMENT_REQUEST_STATUS_RSN_TYP) ,

---- LOGIC LAYER ----


LOGIC_PRS as ( SELECT 
		  PAY_REQS_STS_ID                                    as                                    PAY_REQS_STS_ID 
		, PAY_REQS_ID                                        as                                        PAY_REQS_ID 
		, upper( TRIM( PAY_REQS_STS_TYP_CD ) )               as                                PAY_REQS_STS_TYP_CD 
		, cast( PAY_REQS_STS_EFF_DT as DATE )                as                                PAY_REQS_STS_EFF_DT 
		, cast( PAY_REQS_STS_END_DT as DATE )                as                                PAY_REQS_STS_END_DT 
		, upper( TRIM( PAY_REQS_STS_COMT ) )                 as                                  PAY_REQS_STS_COMT 
		, upper( TRIM( PAY_REQS_STS_RSN_TYP_CD ) )           as                            PAY_REQS_STS_RSN_TYP_CD 
		, cast( PAY_REQS_STS_STOP_PAY_DT as DATE )           as                           PAY_REQS_STS_STOP_PAY_DT 
		, upper( TRIM( PAY_REQS_STS_STOP_PAY_NO ) )          as                           PAY_REQS_STS_STOP_PAY_NO 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, upper( AUDIT_USER_CREA_DTM )                       as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, upper( AUDIT_USER_UPDT_DTM )                       as                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_PRS
            ),

LOGIC_PRST as ( SELECT 
		  upper( TRIM( PAY_REQS_STT_TYP_CD ) )               as                                PAY_REQS_STT_TYP_CD 
		, upper( TRIM( PAY_REQS_STS_TYP_NM ) )               as                                PAY_REQS_STS_TYP_NM 
		, upper( TRIM( PAY_REQS_STS_TYP_CD ) )               as                                PAY_REQS_STS_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_PRST
            ),

LOGIC_PRSTT as ( SELECT 
		  upper( TRIM( PAY_REQS_STT_TYP_NM ) )               as                                PAY_REQS_STT_TYP_NM 
		, upper( TRIM( PAY_REQS_STT_TYP_CD ) )               as                                PAY_REQS_STT_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_PRSTT
            ),

LOGIC_PRSRT as ( SELECT 
		  upper( TRIM( PAY_REQS_STS_RSN_TYP_NM ) )           as                            PAY_REQS_STS_RSN_TYP_NM 
		, upper( TRIM( PAY_REQS_STS_RSN_TYP_CD ) )           as                            PAY_REQS_STS_RSN_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_PRSRT
            )

---- RENAME LAYER ----
,

RENAME_PRS as ( SELECT 
		  PAY_REQS_STS_ID                                    as                                    PAY_REQS_STS_ID
		, PAY_REQS_ID                                        as                                        PAY_REQS_ID
		, PAY_REQS_STS_TYP_CD                                as                                PAY_REQS_STS_TYP_CD
		, PAY_REQS_STS_EFF_DT                                as                                PAY_REQS_STS_EFF_DT
		, PAY_REQS_STS_END_DT                                as                                PAY_REQS_STS_END_DT
		, PAY_REQS_STS_COMT                                  as                                  PAY_REQS_STS_COMT
		, PAY_REQS_STS_RSN_TYP_CD                            as                            PAY_REQS_STS_RSN_TYP_CD
		, PAY_REQS_STS_STOP_PAY_DT                           as                           PAY_REQS_STS_STOP_PAY_DT
		, PAY_REQS_STS_STOP_PAY_NO                           as                           PAY_REQS_STS_STOP_PAY_NO
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_PRS   ), 
RENAME_PRST as ( SELECT 
		  PAY_REQS_STT_TYP_CD                                as                                PAY_REQS_STT_TYP_CD
		, PAY_REQS_STS_TYP_NM                                as                                PAY_REQS_STS_TYP_NM
		, PAY_REQS_STS_TYP_CD                                as                           PRST_PAY_REQS_STS_TYP_CD
		, VOID_IND                                           as                                      PRST_VOID_IND 
				FROM     LOGIC_PRST   ), 
RENAME_PRSTT as ( SELECT 
		  PAY_REQS_STT_TYP_NM                                as                                PAY_REQS_STT_TYP_NM
		, PAY_REQS_STT_TYP_CD                                as                          PRSTT_PAY_REQS_STT_TYP_CD
		, VOID_IND                                           as                                     PRSTT_VOID_IND 
				FROM     LOGIC_PRSTT   ), 
RENAME_PRSRT as ( SELECT 
		  PAY_REQS_STS_RSN_TYP_NM                            as                            PAY_REQS_STS_RSN_TYP_NM
		, PAY_REQS_STS_RSN_TYP_CD                            as                      PRSRT_PAY_REQS_STS_RSN_TYP_CD
		, VOID_IND                                           as                                     PRSRT_VOID_IND 
				FROM     LOGIC_PRSRT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PRS                            as ( SELECT * from    RENAME_PRS   ),
FILTER_PRST                           as ( SELECT * from    RENAME_PRST 
                                            WHERE PRST_VOID_IND = 'N'  ),
FILTER_PRSTT                          as ( SELECT * from    RENAME_PRSTT 
                                            WHERE PRSTT_VOID_IND = 'N'  ),
FILTER_PRSRT                          as ( SELECT * from    RENAME_PRSRT 
                                            WHERE PRSRT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

PRST as ( SELECT * 
				FROM  FILTER_PRST
				LEFT JOIN FILTER_PRSTT ON  FILTER_PRST.PAY_REQS_STT_TYP_CD =  FILTER_PRSTT.PRSTT_PAY_REQS_STT_TYP_CD  ),
PRS as ( SELECT * 
				FROM  FILTER_PRS
				LEFT JOIN PRST ON  FILTER_PRS.PAY_REQS_STS_TYP_CD = PRST.PRST_PAY_REQS_STS_TYP_CD 
						LEFT JOIN FILTER_PRSRT ON  FILTER_PRS.PAY_REQS_STS_RSN_TYP_CD =  FILTER_PRSRT.PRSRT_PAY_REQS_STS_RSN_TYP_CD  )
SELECT * 
from PRS
      );
    