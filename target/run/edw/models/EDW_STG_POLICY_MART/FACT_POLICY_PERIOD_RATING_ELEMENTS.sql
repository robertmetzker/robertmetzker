

      create or replace  table DEV_EDW.EDW_STG_POLICY_MART.FACT_POLICY_PERIOD_RATING_ELEMENTS  as
      (

---- SRC LAYER ----
WITH
SRC_ELE as ( SELECT *     from     STAGING.DSV_POLICY_PERIOD_RATING_ELEMENT ),
SRC_EMP as ( SELECT *     from     EDW_STAGING_DIM.DIM_EMPLOYER ),
//SRC_ELE as ( SELECT *     from     DSV_POLICY_PERIOD_RATING_ELEMENT) ,
//SRC_EMP as ( SELECT *     from     DIM_EMPLOYER) ,

---- LOGIC LAYER ----

LOGIC_ELE as ( SELECT 
		  PLCY_NO                                            as                                            PLCY_NO 
		,  md5(cast(
    
    coalesce(cast(RT_ELEM_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                                     RT_ELEM_TYP_CD
        , PPRE_EFF_DT                                        as                                         PPRE_EFF_DT															       
		, case when PPRE_EFF_DT is null then -1
			when replace(cast(PPRE_EFF_DT::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
			when replace(cast(PPRE_EFF_DT::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
			else replace(cast(PPRE_EFF_DT::DATE as varchar),'-','')::INTEGER 
		     END 												 as 		   RATING_ELEMENT_EFFECTIVE_DATE_KEY   
		, case when PPRE_END_DT is null then -1
			when replace(cast(PPRE_END_DT::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
			when replace(cast(PPRE_END_DT::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
			else replace(cast(PPRE_END_DT::DATE as varchar),'-','')::INTEGER 
		     END 												 as                   RATING_ELEMENT_END_DATE_KEY 
		,  md5(cast(
    
    coalesce(cast(PLCY_PRD_EFF_DT as 
    varchar
), '') || '-' || coalesce(cast(PLCY_PRD_END_DT as 
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
                                                             as                                 POLICY_PERIOD_HKEY  			 
		,  md5(cast(
    
    coalesce(cast(POLICY_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(PAYMENT_PLAN_CODE as 
    varchar
), '') || '-' || coalesce(cast(LEASE_TYPE_CODE as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                          YEAR_CONTROL_ELEMENT_HKEY                                  
		, PPRE_RT                                            as                                            PPRE_RT 
		, EXPRN_MOD_FCTR                                     as                                     EXPRN_MOD_FCTR 
		, CUST_NO                                            as                                            CUST_NO 
		, LEASE_TYPE_CODE                                    as                                    LEASE_TYPE_CODE 
		, PAYMENT_PLAN_CODE                                  as                                  PAYMENT_PLAN_CODE 
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE 
		, PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT 
		, PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT 
		, POLICY_PERIOD_DESC                                 as                                 POLICY_PERIOD_DESC 
		, NEW_POLICY_IND                                     as                                     NEW_POLICY_IND 
		, PEC_POLICY_IND                                     as                                     PEC_POLICY_IND 
		, (COUNT(RT_ELEM_TYP_CD) OVER (PARTITION BY PLCY_NO, PPRE_EFF_DT)) 
		                                                     as                                 PROGRAM_YEAR_COUNT

		from SRC_ELE
            ),
LOGIC_EMP as ( SELECT 
		  CASE WHEN EMPLOYER_HKEY   IS NULL THEN MD5('99999') ELSE EMPLOYER_HKEY 
		          END                                        as                                      EMPLOYER_HKEY 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER 
		from SRC_EMP
            )

---- RENAME LAYER ----
,

RENAME_ELE as ( SELECT 
		  PLCY_NO                                            as                                      POLICY_NUMBER
		, RT_ELEM_TYP_CD                                     as                                RATING_ELEMENT_HKEY
		, PPRE_EFF_DT                                        as                                        PPRE_EFF_DT
		, RATING_ELEMENT_EFFECTIVE_DATE_KEY                  as                  RATING_ELEMENT_EFFECTIVE_DATE_KEY
		, RATING_ELEMENT_END_DATE_KEY                        as                        RATING_ELEMENT_END_DATE_KEY
		, POLICY_PERIOD_HKEY                                 as                                 POLICY_PERIOD_HKEY
		, YEAR_CONTROL_ELEMENT_HKEY                          as                          YEAR_CONTROL_ELEMENT_HKEY
		, PPRE_RT                                            as                                RATING_ELEMENT_RATE
		, EXPRN_MOD_FCTR                                     as                         EXPERIENCE_MODIFIER_FACTOR
		, CUST_NO                                            as                                            CUST_NO
		, LEASE_TYPE_CODE                                    as                                    LEASE_TYPE_CODE
		, PAYMENT_PLAN_CODE                                  as                                  PAYMENT_PLAN_CODE
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE
		, PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT
		, PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT
		, POLICY_PERIOD_DESC                                 as                                 POLICY_PERIOD_DESC
		, NEW_POLICY_IND                                     as                                     NEW_POLICY_IND
		, PEC_POLICY_IND                                     as                                     PEC_POLICY_IND 
		, PROGRAM_YEAR_COUNT                                 as                                 PROGRAM_YEAR_COUNT
				FROM     LOGIC_ELE   ), 
RENAME_EMP as ( SELECT 
		  EMPLOYER_HKEY                                      as                                      EMPLOYER_HKEY
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER 
				FROM     LOGIC_EMP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ELE                            as ( SELECT * from    RENAME_ELE   ),
FILTER_EMP                            as ( SELECT * from    RENAME_EMP   ),

---- JOIN LAYER ----

ELE as ( SELECT * 
				FROM  FILTER_ELE
				LEFT JOIN FILTER_EMP ON  COALESCE(FILTER_ELE.CUST_NO,'99999') =  FILTER_EMP.CUSTOMER_NUMBER AND FILTER_ELE.PPRE_EFF_DT BETWEEN FILTER_EMP.RECORD_EFFECTIVE_DATE AND COALESCE(FILTER_EMP.RECORD_END_DATE,'2099-12-31')  )
SELECT 
		  POLICY_NUMBER
		, coalesce( RATING_ELEMENT_HKEY, MD5( '99999' )) as RATING_ELEMENT_HKEY
		, RATING_ELEMENT_EFFECTIVE_DATE_KEY
		, RATING_ELEMENT_END_DATE_KEY
		, coalesce( EMPLOYER_HKEY, MD5( '99999' )) as EMPLOYER_HKEY
		, POLICY_PERIOD_HKEY
		, YEAR_CONTROL_ELEMENT_HKEY
		, RATING_ELEMENT_RATE
		, EXPERIENCE_MODIFIER_FACTOR 
		, PROGRAM_YEAR_COUNT
		, CURRENT_TIMESTAMP AS  LOAD_DATETIME
		, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
		, 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 			
from ELE
      );
    