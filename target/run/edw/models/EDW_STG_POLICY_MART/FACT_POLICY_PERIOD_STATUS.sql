

      create or replace  table DEV_EDW.EDW_STG_POLICY_MART.FACT_POLICY_PERIOD_STATUS  as
      (

---- SRC LAYER ----
WITH
SRC_PPSH as ( SELECT *     from     STAGING.DSV_POLICY_PERIOD_STATUS_HISTORY ),
SRC_EMP as ( SELECT *     from     EDW_STAGING_DIM.DIM_EMPLOYER ),
//SRC_PPSH as ( SELECT *     from     DSV_POLICY_PERIOD_STATUS_HISTORY) ,
//SRC_EMP as ( SELECT *     from     DIM_EMPLOYER) ,

---- LOGIC LAYER ----

LOGIC_PPSH as ( SELECT 
		  PLCY_NO                                            as                                            PLCY_NO 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( PLCY_TYP_CODE, PLCY_STS_TYP_CD,PLCY_STS_RSN_TYP_CD, POLICY_ACTIVE_IND ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(PLCY_TYP_CODE as 
    varchar
), '') || '-' || coalesce(cast(PLCY_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(PLCY_STS_RSN_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(POLICY_ACTIVE_IND as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                             POLICY_STANDING_HKEY    
		, CASE WHEN STATUS_EFF_DT is null then '-1' 
			WHEN STATUS_EFF_DT < '1901-01-01' then '-2' 
			WHEN STATUS_EFF_DT > '2099-12-31' then '-3' 
			ELSE regexp_replace( STATUS_EFF_DT, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                      STATUS_EFF_DT 
        , STATUS_EFF_DT as STATUS_EFF_DT1
		, CASE WHEN PLCY_STS_TRANS_DT is null then '-1' 
			WHEN PLCY_STS_TRANS_DT < '1901-01-01' then '-2' 
			WHEN PLCY_STS_TRANS_DT > '2099-12-31' then '-3' 
			ELSE regexp_replace( PLCY_STS_TRANS_DT, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                  PLCY_STS_TRANS_DT 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( PLCY_PRD_EFF_DATE, PLCY_PRD_END_DATE,PEC_POLICY_IND, NEW_POLICY_IND, POLICY_PERIOD_DESC ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(PLCY_PRD_EFF_DATE as 
    varchar
), '') || '-' || coalesce(cast(PLCY_PRD_END_DATE as 
    varchar
), '') || '-' || coalesce(cast(PEC_POLICY_IND as 
    varchar
), '') || '-' || coalesce(cast(NEW_POLICY_IND as 
    varchar
), '') || '-' || coalesce(cast(POLICY_PERIOD_DESC as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                              as                                POLICY_PERIOD_HKEY
		, CASE WHEN STATUS_END_DT is null then '-1' 
			WHEN STATUS_END_DT < '1901-01-01' then '-2' 
			WHEN STATUS_END_DT > '2099-12-31' then '-3' 
			ELSE regexp_replace( STATUS_END_DT, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                      STATUS_END_DT 
		, COVERED_DAYS                                       as                                       COVERED_DAYS 
		, NON_COVERED_DAYS                                   as                                   NON_COVERED_DAYS 
		, LAPSED_DAYS                                        as                                        LAPSED_DAYS 
		, PLCY_PRD_EFF_DATE                                  as                                  PLCY_PRD_EFF_DATE 
		, PLCY_PRD_END_DATE                                  as                                  PLCY_PRD_END_DATE 
		, CUST_NO                                            as                                            CUST_NO 
		, PLCY_TYP_CODE                                      as                                      PLCY_TYP_CODE 
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD 
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD 
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND 
		, PEC_POLICY_IND                                     as                                     PEC_POLICY_IND 
		, NEW_POLICY_IND                                     as                                     NEW_POLICY_IND 
		, POLICY_PERIOD_DESC                                 as                                 POLICY_PERIOD_DESC 
		, PLCY_PRD_EFF_DATE                                  as                                        PPRE_EFF_DT
		from SRC_PPSH
            ),
LOGIC_EMP as ( SELECT
          EMPLOYER_HKEY                                      as                                      EMPLOYER_HKEY 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER 
		from SRC_EMP
            )

---- RENAME LAYER ----
,

RENAME_PPSH as ( SELECT 
		  PLCY_NO                                            as                                      POLICY_NUMBER
	    , POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY 
		, STATUS_EFF_DT                                      as                   POLICY_STATUS_EFFECTIVE_DATE_KEY
		, STATUS_EFF_DT1                                     as                                      STATUS_EFF_DT
		, PLCY_STS_TRANS_DT                                  as                 POLICY_STATUS_TRANSACTION_DATE_KEY
		, POLICY_PERIOD_HKEY                                 as                                 POLICY_PERIOD_HKEY
		, STATUS_END_DT                                      as                         POLICY_STATUS_END_DATE_KEY
		, COVERED_DAYS                                       as                                       COVERED_DAYS
		, NON_COVERED_DAYS                                   as                                   NON_COVERED_DAYS
		, LAPSED_DAYS                                        as                                        LAPSED_DAYS
		, PLCY_PRD_EFF_DATE                                  as                                  PLCY_PRD_EFF_DATE
		, PLCY_PRD_END_DATE                                  as                                  PLCY_PRD_END_DATE
		, CUST_NO                                            as                                            CUST_NO
		, PLCY_TYP_CODE                                      as                                      PLCY_TYP_CODE
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND
		, PEC_POLICY_IND                                     as                                     PEC_POLICY_IND
		, NEW_POLICY_IND                                     as                                     NEW_POLICY_IND
		, POLICY_PERIOD_DESC                                 as                                 POLICY_PERIOD_DESC 
		, PPRE_EFF_DT                                        as                                        PPRE_EFF_DT
				FROM     LOGIC_PPSH   ), 
RENAME_EMP as ( SELECT 
		  EMPLOYER_HKEY                                      as                                      EMPLOYER_HKEY
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER 
				FROM     LOGIC_EMP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PPSH                           as ( SELECT * from    RENAME_PPSH   ),
FILTER_EMP                            as ( SELECT * from    RENAME_EMP   ),

---- JOIN LAYER ----

PPSH as ( SELECT * 
				FROM  FILTER_PPSH
				LEFT JOIN FILTER_EMP ON  COALESCE(FILTER_PPSH.CUST_NO,'99999') =  FILTER_EMP.CUSTOMER_NUMBER 
				 AND FILTER_PPSH.STATUS_EFF_DT BETWEEN FILTER_EMP.RECORD_EFFECTIVE_DATE AND COALESCE(FILTER_EMP.RECORD_END_DATE,'2099-12-31')  )
SELECT 
		  POLICY_NUMBER
		, POLICY_STANDING_HKEY 
		, POLICY_STATUS_EFFECTIVE_DATE_KEY
		, POLICY_STATUS_TRANSACTION_DATE_KEY
		, POLICY_PERIOD_HKEY
		, POLICY_STATUS_END_DATE_KEY
		, COALESCE(EMPLOYER_HKEY, md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
)) ) AS EMPLOYER_HKEY
		, COVERED_DAYS
		, NON_COVERED_DAYS
		, LAPSED_DAYS 
		, CURRENT_TIMESTAMP AS  LOAD_DATETIME
		, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
		, 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
from PPSH
      );
    