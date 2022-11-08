---- SRC LAYER ----
WITH
SRC_PR             as ( SELECT *     FROM     STAGING.STG_PAYMENT_REQUEST_CODE ),
//SRC_PR             as ( SELECT *     FROM     STG_PAYMENT_REQUEST) ,

---- LOGIC LAYER ----


LOGIC_PR as ( SELECT 
		  TRIM( PAY_MEDA_PREF_TYP_CD )                       as                               PAY_MEDA_PREF_TYP_CD 
		, TRIM( PAY_REQS_TYP_CD )                            as                                    PAY_REQS_TYP_CD 
		, TRIM( PAY_REQS_STT_TYP_CD )                        as                                PAY_REQS_STT_TYP_CD 
		, TRIM( PAY_REQS_STS_TYP_CD )                        as                                PAY_REQS_STS_TYP_CD 
		, TRIM( PAY_REQS_STS_RSN_TYP_CD )                    as                            PAY_REQS_STS_RSN_TYP_CD 
		, TRIM( PAY_MEDA_PREF_TYP_NM )                       as                               PAY_MEDA_PREF_TYP_NM 
		, TRIM( PAY_REQS_TYP_NM )                            as                                    PAY_REQS_TYP_NM 
		, TRIM( APP_CNTX_TYP_CD )                            as                                    APP_CNTX_TYP_CD 
		, TRIM( APP_SUB_CNTX_TYP_CD )                        as                                APP_SUB_CNTX_TYP_CD 
		, TRIM( APP_SUB_CNTX_TYP_NM )                        as                                APP_SUB_CNTX_TYP_NM 
		, TRIM( PAY_REQS_STT_TYP_NM )                        as                                PAY_REQS_STT_TYP_NM 
		, TRIM( PAY_REQS_STS_TYP_NM )                        as                                PAY_REQS_STS_TYP_NM 
		, TRIM( PAY_REQS_STS_RSN_TYP_NM )                    as                            PAY_REQS_STS_RSN_TYP_NM 
		FROM SRC_PR
            )

---- RENAME LAYER ----
,

RENAME_PR         as ( SELECT 
		  PAY_MEDA_PREF_TYP_CD                               as                               PAY_MEDA_PREF_TYP_CD
		, PAY_REQS_TYP_CD                                    as                                    PAY_REQS_TYP_CD
		, PAY_REQS_STT_TYP_CD                                as                                PAY_REQS_STT_TYP_CD
		, PAY_REQS_STS_TYP_CD                                as                                PAY_REQS_STS_TYP_CD
		, PAY_REQS_STS_RSN_TYP_CD                            as                            PAY_REQS_STS_RSN_TYP_CD
		, PAY_MEDA_PREF_TYP_NM                               as                               PAY_MEDA_PREF_TYP_NM
		, PAY_REQS_TYP_NM                                    as                                    PAY_REQS_TYP_NM
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD
		, APP_SUB_CNTX_TYP_CD                                as                                APP_SUB_CNTX_TYP_CD
		, APP_SUB_CNTX_TYP_NM                                as                                APP_SUB_CNTX_TYP_NM
		, PAY_REQS_STT_TYP_NM                                as                                PAY_REQS_STT_TYP_NM
		, PAY_REQS_STS_TYP_NM                                as                                PAY_REQS_STS_TYP_NM
		, PAY_REQS_STS_RSN_TYP_NM                            as                            PAY_REQS_STS_RSN_TYP_NM 
				FROM     LOGIC_PR   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PR                             as ( SELECT * FROM    RENAME_PR   ),

---- JOIN LAYER ----

 JOIN_PR          as  ( SELECT * 
				FROM  FILTER_PR ),
------ETL LAYER------------
ETL AS(SELECT 
md5(cast(
    
    coalesce(cast(PAY_MEDA_PREF_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(PAY_REQS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(PAY_REQS_STT_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(PAY_REQS_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(PAY_REQS_STS_RSN_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
,PAY_MEDA_PREF_TYP_CD
,PAY_REQS_TYP_CD
,PAY_REQS_STT_TYP_CD
,PAY_REQS_STS_TYP_CD
,PAY_REQS_STS_RSN_TYP_CD
,PAY_MEDA_PREF_TYP_NM
,PAY_REQS_TYP_NM
,APP_CNTX_TYP_CD
,APP_SUB_CNTX_TYP_CD
,APP_SUB_CNTX_TYP_NM
,PAY_REQS_STT_TYP_NM
,PAY_REQS_STS_TYP_NM
,PAY_REQS_STS_RSN_TYP_NM
FROM JOIN_PR)

SELECT * FROM ETL