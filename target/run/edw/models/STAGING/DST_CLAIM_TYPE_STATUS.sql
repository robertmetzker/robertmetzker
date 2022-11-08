

      create or replace  table DEV_EDW.STAGING.DST_CLAIM_TYPE_STATUS  as
      (---- SRC LAYER ----
WITH
SRC_CTS as ( SELECT *     from     STAGING.STG_CLAIM_TYPE_STATUS ),
SRC_CC as ( SELECT *     from     STAGING.STG_CORESUITE_CONVERSION_CLAIM_STATUS ),
//SRC_CTS as ( SELECT *     from     STG_CLAIM_TYPE_STATUS) ,
//SRC_CC as ( SELECT *     from     STG_CORESUITE_CONVERSION_CLAIM_STATUS) ,
SRC_CLAIM_TYPE_CHNG_IND as (select 'Y' as CLAIM_TYPE_CHANGE_OVER_IND UNION SELECT 'N' as CLAIM_TYPE_CHANGE_OVER_IND ),

---- LOGIC LAYER ----


LOGIC_CTS as ( SELECT 
		  TRIM( CLM_TYP_CD )                                 as                                         CLM_TYP_CD 
		, TRIM( CLM_STT_TYP_CD )                             as                                     CLM_STT_TYP_CD 
		, TRIM( CLM_STS_TYP_CD )                             as                                     CLM_STS_TYP_CD 
		, TRIM( CLM_TRANS_RSN_TYP_CD )                       as                               CLM_TRANS_RSN_TYP_CD 
		, TRIM( CLM_TYP_NM )                                 as                                         CLM_TYP_NM 
		, TRIM( CLM_STT_TYP_NM )                             as                                     CLM_STT_TYP_NM 
		, TRIM( CLM_STS_TYP_NM )                             as                                     CLM_STS_TYP_NM 
		, TRIM( CLM_TRANS_RSN_TYP_NM )                       as                               CLM_TRANS_RSN_TYP_NM 
		from SRC_CTS
            ),	

LOGIC_CC as ( SELECT 
		  TRIM( CLAIM_WEB_STATUS_CODE )                      as                              CLAIM_WEB_STATUS_CODE 
		, TRIM( CLAIM_WEB_STATUS_DESC )                      as                              CLAIM_WEB_STATUS_DESC 
		, TRIM( CLAIM_WEB_STATUS_COMMENT )                   as                           CLAIM_WEB_STATUS_COMMENT 
		, TRIM( CLAIM_STATE_CODE )                           as                                   CLAIM_STATE_CODE 
		, TRIM( CLAIM_STATUS_CODE )                          as                                  CLAIM_STATUS_CODE 
		, TRIM( CLAIM_STATUS_REASON_CODE )                   as                           CLAIM_STATUS_REASON_CODE 
		, TRIM( CLAIM_TYPE_CODE )                            as                                    CLAIM_TYPE_CODE 
		from SRC_CC
            ),

LOGIC_CLAIM_TYPE_CHNG_IND as ( SELECT 
	      CLAIM_TYPE_CHANGE_OVER_IND                         as                         CLAIM_TYPE_CHANGE_OVER_IND 
		from SRC_CLAIM_TYPE_CHNG_IND
            )   

---- RENAME LAYER ----
,
RENAME_CTS as ( SELECT 
		  CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM
		, CLM_STT_TYP_NM                                     as                                     CLM_STT_TYP_NM
		, CLM_STS_TYP_NM                                     as                                     CLM_STS_TYP_NM
		, CLM_TRANS_RSN_TYP_NM                               as                               CLM_TRANS_RSN_TYP_NM 
				FROM     LOGIC_CTS   ), 
RENAME_CC as ( SELECT 
		  CLAIM_WEB_STATUS_CODE                              as                              CLAIM_WEB_STATUS_CODE
		, CLAIM_WEB_STATUS_DESC                              as                              CLAIM_WEB_STATUS_DESC
		, CLAIM_WEB_STATUS_COMMENT                           as                           CLAIM_WEB_STATUS_COMMENT
		, CLAIM_STATE_CODE                                   as                                   CLAIM_STATE_CODE
		, CLAIM_STATUS_CODE                                  as                                  CLAIM_STATUS_CODE
		, CLAIM_STATUS_REASON_CODE                           as                           CLAIM_STATUS_REASON_CODE
		, CLAIM_TYPE_CODE                                    as                                    CLAIM_TYPE_CODE 
				FROM     LOGIC_CC   ),
RENAME_CLAIM_TYPE_CHNG_IND as ( SELECT 
		  CLAIM_TYPE_CHANGE_OVER_IND                         as                         CLAIM_TYPE_CHANGE_OVER_IND 
				FROM     LOGIC_CLAIM_TYPE_CHNG_IND   )
---- FILTER LAYER (uses aliases) ----
,
FILTER_CTS                            as ( SELECT * from    RENAME_CTS   ),
FILTER_CC                             as ( SELECT * from    RENAME_CC   ),
FILTER_CLAIM_TYPE_CHNG_IND            as ( SELECT * from    RENAME_CLAIM_TYPE_CHNG_IND ),

---- JOIN LAYER ----

JOIN_CTS as ( SELECT * 
				FROM  FILTER_CTS
				LEFT JOIN FILTER_CC ON  FILTER_CTS.CLM_STT_TYP_CD =  FILTER_CC.CLAIM_STATE_CODE 
                          AND FILTER_CTS.CLM_STS_TYP_CD =  FILTER_CC.CLAIM_STATUS_CODE 
                          AND FILTER_CTS.CLM_TRANS_RSN_TYP_CD =  FILTER_CC.CLAIM_STATUS_REASON_CODE
                          AND FILTER_CTS.CLM_TYP_CD =  FILTER_CC.CLAIM_TYPE_CODE ),
/*
---- UNION LAYER ----
--- Based on Requirement from DA, UNION duplicates one record into 2 records with CLM_TYP_CD and "NULL" 
UNION_CTS as ( 
  SELECT CLM_TYP_CD 
--		, 'Y' as CLAIM_TYPE_CHANGE_OVER_IND
		, CLM_STT_TYP_CD
		, CLM_STS_TYP_CD
		, CLM_TRANS_RSN_TYP_CD
		, CLM_TYP_NM
		, CLM_STT_TYP_NM
		, CLM_STS_TYP_NM
		, CLM_TRANS_RSN_TYP_NM
		, CLAIM_WEB_STATUS_CODE
		, CLAIM_WEB_STATUS_DESC
		, CLAIM_WEB_STATUS_COMMENT
		, CLAIM_STATE_CODE
		, CLAIM_STATUS_CODE
		, CLAIM_STATUS_REASON_CODE
		, CLAIM_TYPE_CODE
  FROM  JOIN_CTS            
  UNION
  SELECT 'NULL' AS CLM_TYP_CD
 --       , 'N' as CLAIM_TYPE_CHANGE_OVER_IND
        , CLM_STT_TYP_CD
		, CLM_STS_TYP_CD
		, CLM_TRANS_RSN_TYP_CD
		, 'NULL' AS CLM_TYP_NM
		, CLM_STT_TYP_NM
		, CLM_STS_TYP_NM
		, CLM_TRANS_RSN_TYP_NM
		, CLAIM_WEB_STATUS_CODE
		, CLAIM_WEB_STATUS_DESC
		, CLAIM_WEB_STATUS_COMMENT
		, CLAIM_STATE_CODE
		, CLAIM_STATUS_CODE
		, CLAIM_STATUS_REASON_CODE
		, CLAIM_TYPE_CODE   
  FROM JOIN_CTS ), */

------ETL LAYER----

ETL as (SELECT * FROM JOIN_CTS CROSS JOIN FILTER_CLAIM_TYPE_CHNG_IND),

FINAL_ETL as (SELECT   
   CLM_TYP_CD
 , CLAIM_TYPE_CHANGE_OVER_IND
 , CLM_STT_TYP_CD
 , CLM_STS_TYP_CD
 , CLM_TRANS_RSN_TYP_CD
 , CLM_TYP_NM
 , CLM_STT_TYP_NM
 , CLM_STS_TYP_NM
 , CLM_TRANS_RSN_TYP_NM
 , CLAIM_WEB_STATUS_CODE
 , CLAIM_WEB_STATUS_DESC
 , CLAIM_WEB_STATUS_COMMENT
 ,CLAIM_STATE_CODE
,CLAIM_STATUS_CODE
,CLAIM_STATUS_REASON_CODE
,CLAIM_TYPE_CODE
from ETL
UNION
SELECT DISTINCT
NULL
, CLAIM_TYPE_CHANGE_OVER_IND
, CLM_STT_TYP_CD
, CLM_STS_TYP_CD
, CLM_TRANS_RSN_TYP_CD
,NULL
,CLM_STT_TYP_NM
,CLM_STS_TYP_NM
,CLM_TRANS_RSN_TYP_NM
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
from ETL
UNION
SELECT DISTINCT
CLM_TYP_CD
, NULL
, NULL
, NULL
, NULL
,CLM_TYP_NM
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
from ETL
UNION
SELECT DISTINCT
CLM_TYP_CD
, null
, CLM_STT_TYP_CD
, CLM_STS_TYP_CD
, CLM_TRANS_RSN_TYP_CD
,CLM_TYP_NM
,CLM_STT_TYP_NM
,CLM_STS_TYP_NM
,CLM_TRANS_RSN_TYP_NM
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
from ETL
UNION
SELECT DISTINCT
null
, null
, CLM_STT_TYP_CD
, CLM_STS_TYP_CD
, CLM_TRANS_RSN_TYP_CD
,null
,CLM_STT_TYP_NM
,CLM_STS_TYP_NM
,CLM_TRANS_RSN_TYP_NM
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
from ETL
)


SELECT DISTINCT
md5(cast(
    
    coalesce(cast(CLM_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_TYPE_CHANGE_OVER_IND as 
    varchar
), '') || '-' || coalesce(cast(CLM_STT_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_TRANS_RSN_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) As UNIQUE_ID_KEY
,CLM_TYP_CD
,CLAIM_TYPE_CHANGE_OVER_IND
,CLM_STT_TYP_CD
,CLM_STS_TYP_CD
,CLM_TRANS_RSN_TYP_CD
,CLM_TYP_NM
,CLM_STT_TYP_NM
,CLM_STS_TYP_NM
,CLM_TRANS_RSN_TYP_NM
,CLAIM_WEB_STATUS_CODE
,CLAIM_WEB_STATUS_DESC
,CLAIM_WEB_STATUS_COMMENT
,CLAIM_STATE_CODE
,CLAIM_STATUS_CODE
,CLAIM_STATUS_REASON_CODE
,CLAIM_TYPE_CODE
FROM FINAL_ETL
      );
    