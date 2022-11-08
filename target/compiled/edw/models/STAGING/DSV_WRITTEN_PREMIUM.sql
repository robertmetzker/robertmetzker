

---- SRC LAYER ----
WITH
SRC_WP             as ( SELECT *     FROM     STAGING.DST_WRITTEN_PREMIUM ),
//SRC_WP             as ( SELECT *     FROM     DST_WRITTEN_PREMIUM) ,

---- LOGIC LAYER ----


LOGIC_WP as ( SELECT 
		  PLCY_NO                                            as                                            PLCY_NO 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT 
		, PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT 
		, EMPLOYER_CUSTOMER_NUMBER                           as                           EMPLOYER_CUSTOMER_NUMBER 
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE 
		, POLICY_TYPE_DESC                                   as                                   POLICY_TYPE_DESC 
		, PEC_POLICY_IND                                     as                                     PEC_POLICY_IND 
		, NEW_POLICY_IND                                     as                                     NEW_POLICY_IND 
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD 
		, PLCY_STS_TYP_NM                                    as                                    PLCY_STS_TYP_NM 
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD 
		, PLCY_STS_RSN_TYP_NM                                as                                PLCY_STS_RSN_TYP_NM 
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND 
		, PLCY_STS_RSN_TRANS_DT                              as                              PLCY_STS_RSN_TRANS_DT 
		, POLICY_AUDIT_COMPLETE_DATE                          as                        POLICY_AUDIT_COMPLETE_DATE 
		, COVERED_INDIVIDUAL_CUSTOMER_NUMBER                 as                 COVERED_INDIVIDUAL_CUSTOMER_NUMBER 
		, WC_COV_PREM_EFF_DT                                 as                                 WC_COV_PREM_EFF_DT 
		, WC_COV_PREM_END_DT                                 as                                 WC_COV_PREM_END_DT 
		, CREATE_USER_LOGIN_NAME                             as                             CREATE_USER_LOGIN_NAME 
		, CREATE_USER_NAME                                   as                                   CREATE_USER_NAME 
		, RATING_PLAN_CODE                                   as                                   RATING_PLAN_CODE 
		, RATING_PLAN_RATE                                   as                                   RATING_PLAN_RATE 
		, EMPLOYEE_COUNT                                     as                                     EMPLOYEE_COUNT 
		, EXPOSURE_AMOUNT                                    as                                    EXPOSURE_AMOUNT 
		, CLASS_CODE_BASE_RATE                               as                               CLASS_CODE_BASE_RATE 
		, BASE_PREMIUM_AMOUNT                                as                                BASE_PREMIUM_AMOUNT 
		, PURE_PREMIUM_RATE                                  as                                  PURE_PREMIUM_RATE 
		, PURE_PREMIUM_AMOUNT                                as                                PURE_PREMIUM_AMOUNT 
		, BWC_ASSESSMENT_FEE_RATE                            as                            BWC_ASSESSMENT_FEE_RATE 
		, BWC_ASSESSMENT_FEE_AMOUNT                          as                          BWC_ASSESSMENT_FEE_AMOUNT 
		, IC_ASSESSMENT_FEE_RATE                             as                             IC_ASSESSMENT_FEE_RATE 
		, IC_ASSESSMENT_FEE_AMOUNT                           as                           IC_ASSESSMENT_FEE_AMOUNT 
		, DWRF_FEE_RATE                                      as                                      DWRF_FEE_RATE 
		, DWRF_FEE_AMOUNT                                    as                                    DWRF_FEE_AMOUNT 
		, DWRF_II_FEE_RATE                                   as                                   DWRF_II_FEE_RATE 
		, DWRF_II_FEE_AMOUNT                                 as                                 DWRF_II_FEE_AMOUNT 
		, TOTAL_DERIVED_PREMIUM_AMOUNT                       as                       TOTAL_DERIVED_PREMIUM_AMOUNT 
		, COV_TYP_CD                                         as                                         COV_TYP_CD 
		, COV_TYP_NM                                         as                                         COV_TYP_NM 
		, TTL_TYP_CD                                         as                                         TTL_TYP_CD 
		, TTL_TYP_NM                                         as                                         TTL_TYP_NM 
		, PPPIE_COV_IND                                      as                                      PPPIE_COV_IND 
		, WRITTEN_PREMIUM_ELEMENT_CODE                       as                       WRITTEN_PREMIUM_ELEMENT_CODE 
		, WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE                as                WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE 
		, WRITTEN_PREMIUM_ELEMENT_DESC                       as                       WRITTEN_PREMIUM_ELEMENT_DESC 
		, WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE             as             WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE 
		, WRITTEN_PREMIUM_ELEMENT_END_DATE                   as                   WRITTEN_PREMIUM_ELEMENT_END_DATE 
		, PREMIUM_CALCULATION_EFFECTIVE_DATE                 as                 PREMIUM_CALCULATION_EFFECTIVE_DATE 
		, PREMIUM_CALCULATION_END_DATE                       as                       PREMIUM_CALCULATION_END_DATE 
		, PREMIUM_CALCULATION_TYPE_DESC                      as                      PREMIUM_CALCULATION_TYPE_DESC 
		, CURRENT_PREMIUM_CALCULATION_IND                    as                    CURRENT_PREMIUM_CALCULATION_IND 
		, SIC_TYP_CD                                         as                                         SIC_TYP_CD 
		, PREM_TYP_CD                                        as                                        PREM_TYP_CD 
		, PREM_TYP_NM                                        as                                        PREM_TYP_NM 
		, PLCY_AUDT_TYP_CD                                   as                                   PLCY_AUDT_TYP_CD 
		, PLCY_AUDT_TYP_NM                                   as                                   PLCY_AUDT_TYP_NM 
		FROM SRC_WP
            )

---- RENAME LAYER ----
,

RENAME_WP         as ( SELECT 
		  PLCY_NO                                            as                                      POLICY_NUMBER
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, PLCY_PRD_EFF_DT                                    as                       POLICY_PERIOD_EFFECTIVE_DATE
		, PLCY_PRD_END_DT                                    as                             POLICY_PERIOD_END_DATE
		, EMPLOYER_CUSTOMER_NUMBER                           as                           EMPLOYER_CUSTOMER_NUMBER
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE
		, POLICY_TYPE_DESC                                   as                                   POLICY_TYPE_DESC
		, PEC_POLICY_IND                                     as                                     PEC_POLICY_IND
		, NEW_POLICY_IND                                     as                                     NEW_POLICY_IND
		, PLCY_STS_TYP_CD                                    as                                 POLICY_STATUS_CODE
		, PLCY_STS_TYP_NM                                    as                                 POLICY_STATUS_DESC
		, PLCY_STS_RSN_TYP_CD                                as                                 STATUS_REASON_CODE
		, PLCY_STS_RSN_TYP_NM                                as                                 STATUS_REASON_DESC
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND
		, PLCY_STS_RSN_TRANS_DT                              as                       POLICY_STATUS_EFFECTIVE_DATE
		, POLICY_AUDIT_COMPLETE_DATE                          as                                   LAST_AUDIT_DATE
		, COVERED_INDIVIDUAL_CUSTOMER_NUMBER                 as                 COVERED_INDIVIDUAL_CUSTOMER_NUMBER
		, WC_COV_PREM_EFF_DT                                 as                            COVERAGE_EFFECTIVE_DATE
		, WC_COV_PREM_END_DT                                 as                                  COVERAGE_END_DATE
		, CREATE_USER_LOGIN_NAME                             as                             CREATE_USER_LOGIN_NAME
		, CREATE_USER_NAME                                   as                                   CREATE_USER_NAME
		, RATING_PLAN_CODE                                   as                                   RATING_PLAN_CODE
		, EMPLOYEE_COUNT                                     as                            EXPOSURE_EMPLOYEE_COUNT
		, RATING_PLAN_RATE                                   as                                   RATING_PLAN_RATE
		, EXPOSURE_AMOUNT                                    as                                    EXPOSURE_AMOUNT
		, CLASS_CODE_BASE_RATE                               as                             MANUAL_CLASS_BASE_RATE
		, BASE_PREMIUM_AMOUNT                                as                                BASE_PREMIUM_AMOUNT
		, PURE_PREMIUM_RATE                                  as                                  PURE_PREMIUM_RATE
		, PURE_PREMIUM_AMOUNT                                as                                PURE_PREMIUM_AMOUNT
		, BWC_ASSESSMENT_FEE_RATE                            as                                BWC_ASSESSMENT_RATE
		, BWC_ASSESSMENT_FEE_AMOUNT                          as                                 BWC_ASSESSMENT_FEE
		, IC_ASSESSMENT_FEE_RATE                             as                                 IC_ASSESSMENT_RATE
		, IC_ASSESSMENT_FEE_AMOUNT                           as                                  IC_ASSESSMENT_FEE
		, DWRF_FEE_RATE                                      as                                          DWRF_RATE
		, DWRF_FEE_AMOUNT                                    as                                           DWRF_FEE
		, DWRF_II_FEE_RATE                                   as                                       DWRF_II_RATE
		, DWRF_II_FEE_AMOUNT                                 as                                        DWRF_II_FEE
		, TOTAL_DERIVED_PREMIUM_AMOUNT                       as                             WRITTEN_PREMIUM_AMOUNT
		, COV_TYP_CD                                         as                      INDIVIDUAL_COVERAGE_TYPE_CODE
		, COV_TYP_NM                                         as                      INDIVIDUAL_COVERAGE_TYPE_DESC
		, TTL_TYP_CD                                         as                     INDIVIDUAL_COVERAGE_TITLE_CODE
		, TTL_TYP_NM                                         as                          INDIVIDUAL_COVERAGE_TITLE
		, PPPIE_COV_IND                                      as                  INDIVIDUAL_COVERAGE_INCLUSION_IND
		, WRITTEN_PREMIUM_ELEMENT_CODE                       as                       WRITTEN_PREMIUM_ELEMENT_CODE
		, WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE                as                WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE
		, WRITTEN_PREMIUM_ELEMENT_DESC                       as                       WRITTEN_PREMIUM_ELEMENT_DESC
		, WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE             as             WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE
		, WRITTEN_PREMIUM_ELEMENT_END_DATE                   as                   WRITTEN_PREMIUM_ELEMENT_END_DATE
		, PREMIUM_CALCULATION_EFFECTIVE_DATE                 as                 PREMIUM_CALCULATION_EFFECTIVE_DATE
		, PREMIUM_CALCULATION_END_DATE                       as                       PREMIUM_CALCULATION_END_DATE
		, PREMIUM_CALCULATION_TYPE_DESC                       as                     PREMIUM_CALCULATION_TYPE_DESC
		, CURRENT_PREMIUM_CALCULATION_IND                    as                    CURRENT_PREMIUM_CALCULATION_IND
		, SIC_TYP_CD                                         as                                INDUSTRY_GROUP_CODE
		, PREM_TYP_CD                                        as                                 EXPOSURE_TYPE_CODE
		, PREM_TYP_NM                                        as                                 EXPOSURE_TYPE_DESC
		, PLCY_AUDT_TYP_CD                                   as                           EXPOSURE_AUDIT_TYPE_CODE
		, PLCY_AUDT_TYP_NM                                   as                           EXPOSURE_AUDIT_TYPE_DESC 
				FROM     LOGIC_WP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_WP                             as ( SELECT * FROM    RENAME_WP   ),

---- JOIN LAYER ----

 JOIN_WP          as  ( SELECT * 
				FROM  FILTER_WP )
 SELECT 
  POLICY_NUMBER
, PLCY_PRD_ID
, POLICY_PERIOD_EFFECTIVE_DATE
, POLICY_PERIOD_END_DATE
, EMPLOYER_CUSTOMER_NUMBER
, POLICY_TYPE_CODE
, POLICY_TYPE_DESC
, PEC_POLICY_IND
, NEW_POLICY_IND
, POLICY_STATUS_CODE
, POLICY_STATUS_DESC
, STATUS_REASON_CODE
, STATUS_REASON_DESC
, POLICY_ACTIVE_IND
, POLICY_STATUS_EFFECTIVE_DATE
, LAST_AUDIT_DATE
, COVERED_INDIVIDUAL_CUSTOMER_NUMBER
, COVERAGE_EFFECTIVE_DATE
, COVERAGE_END_DATE
, CREATE_USER_LOGIN_NAME
, CREATE_USER_NAME
, RATING_PLAN_CODE
, RATING_PLAN_RATE
, EXPOSURE_EMPLOYEE_COUNT
, EXPOSURE_AMOUNT
, MANUAL_CLASS_BASE_RATE
, BASE_PREMIUM_AMOUNT
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
, INDIVIDUAL_COVERAGE_TYPE_CODE
, INDIVIDUAL_COVERAGE_TYPE_DESC
, INDIVIDUAL_COVERAGE_TITLE_CODE
, INDIVIDUAL_COVERAGE_TITLE
, INDIVIDUAL_COVERAGE_INCLUSION_IND
, WRITTEN_PREMIUM_ELEMENT_CODE
, WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE
, WRITTEN_PREMIUM_ELEMENT_DESC
, WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE
, WRITTEN_PREMIUM_ELEMENT_END_DATE
, PREMIUM_CALCULATION_EFFECTIVE_DATE
, PREMIUM_CALCULATION_END_DATE
, PREMIUM_CALCULATION_TYPE_DESC
, CURRENT_PREMIUM_CALCULATION_IND
, INDUSTRY_GROUP_CODE
, EXPOSURE_TYPE_CODE
, EXPOSURE_TYPE_DESC
, EXPOSURE_AUDIT_TYPE_CODE
, EXPOSURE_AUDIT_TYPE_DESC
  FROM  JOIN_WP