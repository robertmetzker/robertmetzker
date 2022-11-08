

      create or replace  table DEV_EDW.EDW_STG_POLICY_MART.FACT_WRITTEN_PREMIUM  as
      (

---- SRC LAYER ----
WITH
SRC_EMP            as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_EMPLOYER ),
SRC_WP             as ( SELECT *     FROM     STAGING.DSV_WRITTEN_PREMIUM ),
//SRC_EMP            as ( SELECT *     FROM     DIM_EMPLOYER) ,
//SRC_WP             as ( SELECT *     FROM     DSV_WRITTEN_PREMIUM) ,

---- LOGIC LAYER ----


LOGIC_EMP as ( SELECT 
		  EMPLOYER_HKEY                                      as                                        EMPLOYER_HKEY 
		, CUSTOMER_NUMBER                                    as                                      CUSTOMER_NUMBER
	    , RECORD_EFFECTIVE_DATE                              as                                RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                      RECORD_END_DATE
		FROM SRC_EMP
            ),

LOGIC_WP as ( SELECT POLICY_PERIOD_EFFECTIVE_DATE || ' - ' || POLICY_PERIOD_END_DATE AS POLICY_PERIOD_DESC,
		CASE WHEN nullif(array_to_string(array_construct_compact(POLICY_PERIOD_EFFECTIVE_DATE,POLICY_PERIOD_END_DATE,PEC_POLICY_IND,NEW_POLICY_IND, POLICY_PERIOD_DESC ),''), '') is NULL  
				THEN MD5( '99999' ) ELSE 
		 md5(cast(
    
    coalesce(cast(POLICY_PERIOD_EFFECTIVE_DATE as 
    varchar
), '') || '-' || coalesce(cast(POLICY_PERIOD_END_DATE as 
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
                END                                          as                                    POLICY_PERIOD_HKEY 
		, CASE WHEN COVERAGE_EFFECTIVE_DATE is null then '-1' 
			WHEN COVERAGE_EFFECTIVE_DATE < '1901-01-01' then '-2' 
			WHEN COVERAGE_EFFECTIVE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( COVERAGE_EFFECTIVE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                            COVERAGE_EFFECTIVE_DATE_KEY 
		, CASE WHEN COVERAGE_EFFECTIVE_DATE is null then '-1' 
			WHEN COVERAGE_EFFECTIVE_DATE < '1901-01-01' then '-2' 
			WHEN COVERAGE_EFFECTIVE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( COVERAGE_EFFECTIVE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                  COVERAGE_END_DATE_KEY 
		, CASE WHEN COVERED_INDIVIDUAL_CUSTOMER_NUMBER = EMPLOYER_CUSTOMER_NUMBER THEN MD5( '88888' )
               WHEN COVERED_INDIVIDUAL_CUSTOMER_NUMBER <> EMPLOYER_CUSTOMER_NUMBER AND  COVERED_INDIVIDUAL_CUSTOMER_NUMBER IS NOT NULL
			   THEN md5(cast(
    
    coalesce(cast(COVERED_INDIVIDUAL_CUSTOMER_NUMBER as 
    varchar
), '')

 as 
    varchar
))
			   ELSE  MD5( '99999' )                                    
                END                                          as                       COVERED_INDIVIDUAL_CUSTOMER_HKEY 
		, CASE WHEN nullif(array_to_string(array_construct_compact(PREMIUM_CALCULATION_TYPE_DESC,CURRENT_PREMIUM_CALCULATION_IND,EXPOSURE_TYPE_CODE,EXPOSURE_AUDIT_TYPE_CODE ),''), '') is NULL  
				THEN MD5( '99999' ) ELSE 
		 md5(cast(
    
    coalesce(cast(PREMIUM_CALCULATION_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(CURRENT_PREMIUM_CALCULATION_IND as 
    varchar
), '') || '-' || coalesce(cast(EXPOSURE_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(EXPOSURE_AUDIT_TYPE_CODE as 
    varchar
), '')

 as 
    varchar
))                                     
                END                                          as                               PREMIUM_CALCULATION_TYPE_HKEY 				
		, CASE WHEN PREMIUM_CALCULATION_EFFECTIVE_DATE is null then '-1' 
			WHEN PREMIUM_CALCULATION_EFFECTIVE_DATE < '1901-01-01' then '-2' 
			WHEN PREMIUM_CALCULATION_EFFECTIVE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( PREMIUM_CALCULATION_EFFECTIVE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                           PREMIUM_CALCULATED_DATE_KEY 
		, CASE WHEN nullif(array_to_string(array_construct_compact(WRITTEN_PREMIUM_ELEMENT_CODE, WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE, WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE ),''), '') is NULL  
				THEN MD5( '99999' ) ELSE 
		 md5(cast(
    
    coalesce(cast(WRITTEN_PREMIUM_ELEMENT_CODE as 
    varchar
), '') || '-' || coalesce(cast(WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE as 
    varchar
), '') || '-' || coalesce(cast(WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE as 
    varchar
), '')

 as 
    varchar
))                                     
                END                                          as                         WRITTEN_PREMIUM_ELEMENT_HKEY 
        , CASE WHEN POLICY_TYPE_CODE IS NOT NULL and POLICY_STATUS_CODE <> 'N/A' THEN  md5(cast(
    
    coalesce(cast(POLICY_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(POLICY_STATUS_CODE as 
    varchar
), '') || '-' || coalesce(cast(STATUS_REASON_CODE as 
    varchar
), '') || '-' || coalesce(cast(POLICY_ACTIVE_IND as 
    varchar
), '')

 as 
    varchar
))
               WHEN POLICY_TYPE_CODE = 'MIF' and POLICY_STATUS_CODE  = 'N/A' THEN MD5( '99991' )
	           WHEN POLICY_TYPE_CODE = 'PES' and POLICY_STATUS_CODE  = 'N/A' THEN MD5( '99992' )
	           WHEN POLICY_TYPE_CODE = 'PEC' and POLICY_STATUS_CODE  = 'N/A' THEN MD5( '99993' )
	           WHEN POLICY_TYPE_CODE = 'BL'  and POLICY_STATUS_CODE  = 'N/A' THEN MD5( '99994' )
	           WHEN POLICY_TYPE_CODE = 'SI'  and POLICY_STATUS_CODE  = 'N/A' THEN MD5( '99995' )
	           WHEN POLICY_TYPE_CODE = 'PA'  and POLICY_STATUS_CODE  = 'N/A' THEN MD5( '99996' )
	           ELSE  MD5( '99999' ) END                    as                                   POLICY_STANDING_HKEY 
		, CASE WHEN nullif(array_to_string(array_construct_compact(INDIVIDUAL_COVERAGE_TYPE_DESC, INDIVIDUAL_COVERAGE_TITLE, INDIVIDUAL_COVERAGE_INCLUSION_IND ),''), '') is NULL  
				THEN MD5( '99999' ) ELSE 
		 md5(cast(
    
    coalesce(cast(INDIVIDUAL_COVERAGE_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(INDIVIDUAL_COVERAGE_TITLE as 
    varchar
), '') || '-' || coalesce(cast(INDIVIDUAL_COVERAGE_INCLUSION_IND as 
    varchar
), '')

 as 
    varchar
))       
                END                                            as                           INDIVIDUAL_COVERAGE_HKEY 
		, CASE WHEN nullif(array_to_string(array_construct_compact(RATING_PLAN_CODE ),''), '') is NULL  
				THEN MD5( '99999' ) ELSE 
		 md5(cast(
    
    coalesce(cast(RATING_PLAN_CODE as 
    varchar
), '')

 as 
    varchar
))       
                END                                           as                              RATING_PLAN_TYPE_HKEY 
		, CASE WHEN nullif(array_to_string(array_construct_compact(CREATE_USER_LOGIN_NAME ),''), '') is NULL  
				THEN MD5( '99999' ) ELSE 
		 md5(cast(
    
    coalesce(cast(CREATE_USER_LOGIN_NAME as 
    varchar
), '')

 as 
    varchar
))       
                END                                           as                              UNDERWRITER_USER_HKEY 
		, CASE WHEN nullif(array_to_string(array_construct_compact(INDUSTRY_GROUP_CODE ),''), '') is NULL  
				THEN MD5( '99999' ) ELSE 
		 md5(cast(
    
    coalesce(cast(INDUSTRY_GROUP_CODE as 
    varchar
), '')

 as 
    varchar
))    
               END                                           as                                INDUSTRY_GROUP_HKEY 
		, CASE WHEN POLICY_STATUS_EFFECTIVE_DATE is null then '-1' 
			WHEN POLICY_STATUS_EFFECTIVE_DATE < '1901-01-01' then '-2' 
			WHEN POLICY_STATUS_EFFECTIVE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( POLICY_STATUS_EFFECTIVE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                   POLICY_STATUS_EFFECTIVE_DATE_KEY 
		, CASE WHEN LAST_AUDIT_DATE is null then '-1' 
			WHEN LAST_AUDIT_DATE < '1901-01-01' then '-2' 
			WHEN LAST_AUDIT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( LAST_AUDIT_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                LAST_AUDIT_DATE_KEY 															 			   
		, POLICY_NUMBER                                      as                                      POLICY_NUMBER 
		, EXPOSURE_EMPLOYEE_COUNT                            as                            EXPOSURE_EMPLOYEE_COUNT 
		, EXPOSURE_AMOUNT                                    as                                    EXPOSURE_AMOUNT 
		, EMPLOYER_CUSTOMER_NUMBER                           as                           EMPLOYER_CUSTOMER_NUMBER 
		, MANUAL_CLASS_BASE_RATE                             as                             MANUAL_CLASS_BASE_RATE 
		, BASE_PREMIUM_AMOUNT                                as                                BASE_PREMIUM_AMOUNT 
		, RATING_PLAN_RATE                                   as                                   RATING_PLAN_RATE 
		, PURE_PREMIUM_RATE                                  as                                  PURE_PREMIUM_RATE 
		, PURE_PREMIUM_AMOUNT                                as                                PURE_PREMIUM_AMOUNT 
		, BWC_ASSESSMENT_RATE                                as                                BWC_ASSESSMENT_RATE 
		, BWC_ASSESSMENT_FEE                                 as                                 BWC_ASSESSMENT_FEE 
		, IC_ASSESSMENT_RATE                                 as                                 IC_ASSESSMENT_RATE 
		, IC_ASSESSMENT_FEE                                  as                                  IC_ASSESSMENT_FEE 
		, DWRF_RATE                                          as                                          DWRF_RATE 
		, DWRF_FEE                                           as                                           DWRF_FEE 
		, DWRF_II_RATE                                       as                                       DWRF_II_RATE 
		, DWRF_II_FEE                                        as                                        DWRF_II_FEE 
		, WRITTEN_PREMIUM_AMOUNT                             as                             WRITTEN_PREMIUM_AMOUNT 
		, WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE             as             WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE 
		, PREMIUM_CALCULATION_EFFECTIVE_DATE                 as                 PREMIUM_CALCULATION_EFFECTIVE_DATE 
		FROM SRC_WP
            )

---- RENAME LAYER ----
,

RENAME_EMP        as ( SELECT 
		  EMPLOYER_HKEY                                      as                                      EMPLOYER_HKEY
	    , CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER
	    , RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE
				FROM     LOGIC_EMP   ), 
RENAME_WP         as ( SELECT 
		  POLICY_PERIOD_HKEY                                 as                                 POLICY_PERIOD_HKEY
		, COVERAGE_EFFECTIVE_DATE_KEY                        as                        COVERAGE_EFFECTIVE_DATE_KEY
		, COVERAGE_END_DATE_KEY                              as                              COVERAGE_END_DATE_KEY
		, COVERED_INDIVIDUAL_CUSTOMER_HKEY                   as                   COVERED_INDIVIDUAL_CUSTOMER_HKEY
		, PREMIUM_CALCULATION_TYPE_HKEY                      as                      PREMIUM_CALCULATION_TYPE_HKEY
		-- , PREMIUM_CALCULATION_TYPE_HKEY                      as                      PREMIUM_CALCULATION_TYPE_HKEY
		, PREMIUM_CALCULATED_DATE_KEY                        as                        PREMIUM_CALCULATED_DATE_KEY
		, WRITTEN_PREMIUM_ELEMENT_HKEY                       as                       WRITTEN_PREMIUM_ELEMENT_HKEY
		, POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY
		, INDIVIDUAL_COVERAGE_HKEY                           as                           INDIVIDUAL_COVERAGE_HKEY
		, RATING_PLAN_TYPE_HKEY                              as                              RATING_PLAN_TYPE_HKEY
		, UNDERWRITER_USER_HKEY                              as                              UNDERWRITER_USER_HKEY
		, INDUSTRY_GROUP_HKEY                                as                                INDUSTRY_GROUP_HKEY
		, POLICY_STATUS_EFFECTIVE_DATE_KEY                   as                   POLICY_STATUS_EFFECTIVE_DATE_KEY
		, LAST_AUDIT_DATE_KEY                                as                                LAST_AUDIT_DATE_KEY
		, POLICY_NUMBER                                      as                                      POLICY_NUMBER
		, EXPOSURE_EMPLOYEE_COUNT                            as                            EXPOSURE_EMPLOYEE_COUNT
		, EXPOSURE_AMOUNT                                    as                                    EXPOSURE_AMOUNT
		, EMPLOYER_CUSTOMER_NUMBER                           as                           EMPLOYER_CUSTOMER_NUMBER 
		, MANUAL_CLASS_BASE_RATE                             as                             MANUAL_CLASS_BASE_RATE
		, BASE_PREMIUM_AMOUNT                                as                                BASE_PREMIUM_AMOUNT
		, RATING_PLAN_RATE                                   as                                   RATING_PLAN_RATE
		, PURE_PREMIUM_RATE                                  as                                  PURE_PREMIUM_RATE
		, PURE_PREMIUM_AMOUNT                                as                                PURE_PREMIUM_AMOUNT
		, BWC_ASSESSMENT_RATE                                as                                BWC_ASSESSMENT_RATE
		, BWC_ASSESSMENT_FEE                                 as                                 BWC_ASSESSMENT_FEE
		, IC_ASSESSMENT_RATE                                 as                                 IC_ASSESSMENT_RATE
		, IC_ASSESSMENT_FEE                                  as                                  IC_ASSESSMENT_FEE
		, DWRF_RATE                                          as                                          DWRF_RATE
		, DWRF_FEE                                           as                                           DWRF_FEE
		, DWRF_II_RATE                                       as                                       DWRF_II_RATE
		, DWRF_II_FEE                                        as                                        DWRF_II_FEE
		, WRITTEN_PREMIUM_AMOUNT                             as                             WRITTEN_PREMIUM_AMOUNT
		, WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE             as             WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE
		, PREMIUM_CALCULATION_EFFECTIVE_DATE                 as                 PREMIUM_CALCULATION_EFFECTIVE_DATE
				FROM     LOGIC_WP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_WP                             as ( SELECT * FROM    RENAME_WP   ),
FILTER_EMP                            as ( SELECT * FROM    RENAME_EMP   ),

---- JOIN LAYER ----


WP as ( SELECT * 
				FROM  FILTER_WP
				INNER JOIN FILTER_EMP ON   coalesce( FILTER_WP.EMPLOYER_CUSTOMER_NUMBER, '99999') =  FILTER_EMP.CUSTOMER_NUMBER AND FILTER_WP.PREMIUM_CALCULATION_EFFECTIVE_DATE BETWEEN FILTER_EMP.RECORD_EFFECTIVE_DATE AND COALESCE(FILTER_EMP.RECORD_END_DATE, '2999-12-31')  )

---- ETL LAYER -----------

SELECT 
  EMPLOYER_HKEY
, POLICY_PERIOD_HKEY 
, COVERAGE_EFFECTIVE_DATE_KEY
, COVERAGE_END_DATE_KEY
, COVERED_INDIVIDUAL_CUSTOMER_HKEY
, PREMIUM_CALCULATION_TYPE_HKEY
, PREMIUM_CALCULATED_DATE_KEY
, WRITTEN_PREMIUM_ELEMENT_HKEY
, POLICY_STANDING_HKEY
, INDIVIDUAL_COVERAGE_HKEY
, RATING_PLAN_TYPE_HKEY
, UNDERWRITER_USER_HKEY 
, INDUSTRY_GROUP_HKEY
, POLICY_STATUS_EFFECTIVE_DATE_KEY
, LAST_AUDIT_DATE_KEY
, POLICY_NUMBER
, EXPOSURE_EMPLOYEE_COUNT
, EXPOSURE_AMOUNT
, MANUAL_CLASS_BASE_RATE
, BASE_PREMIUM_AMOUNT
, RATING_PLAN_RATE
, PURE_PREMIUM_RATE
, PURE_PREMIUM_AMOUNT
, BWC_ASSESSMENT_RATE
, BWC_ASSESSMENT_FEE
, IC_ASSESSMENT_RATE
, IC_ASSESSMENT_FEE
, DWRF_RATE
, DWRF_FEE
, DWRF_II_RATE
, DWRF_II_FEE
, WRITTEN_PREMIUM_AMOUNT
, CURRENT_TIMESTAMP AS LOAD_DATETIME
, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME 
, 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
FROM  WP
      );
    