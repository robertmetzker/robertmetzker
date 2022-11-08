

---- SRC LAYER ----
WITH
SRC_DATE as ( SELECT *     from      STAGING.DST_DATE ),
//SRC_DATE as ( SELECT *     from     DST_DATE) ,

---- LOGIC LAYER ----

LOGIC_DATE as ( SELECT 
		  DATE_DIM_KEY                                       as                                       DATE_DIM_KEY 
		, ACTUAL_DT                                          as                                          ACTUAL_DT 
		, FULL_DT_NM                                         as                                         FULL_DT_NM 
		, HOL_IND                                            as                                            HOL_IND 
		, WKEND_IND                                          as                                          WKEND_IND 
		, LAST_DAY_IN_WK_IND                                 as                                 LAST_DAY_IN_WK_IND 
		, LAST_DAY_IN_MM_IND                                 as                                 LAST_DAY_IN_MM_IND 
		, CAL_YYYY_MM_NO                                     as                                     CAL_YYYY_MM_NO 
		, MONTH_MM_NO                                        as                                        MONTH_MM_NO 
		, MONTH_MM_NO_STRING                                 as                                 MONTH_MM_NO_STRING 
		, MONTH_NM                                           as                                           MONTH_NM 
		, MONTH_SHORT_NM                                     as                                     MONTH_SHORT_NM 
		, CRNT_MONTH_BGNG_DATE                               as                               CRNT_MONTH_BGNG_DATE 
		, CRNT_MONTH_FIRST_BSNS_DATE                         as                         CRNT_MONTH_FIRST_BSNS_DATE 
		, CRNT_MONTH_LAST_BSNS_DATE                          as                          CRNT_MONTH_LAST_BSNS_DATE 
		, CAL_QUARTER_NO                                     as                                     CAL_QUARTER_NO 
		, CAL_QUARTER_NM                                     as                                     CAL_QUARTER_NM 
		, CAL_HALF_YR                                        as                                        CAL_HALF_YR 
		, YEAR_YYYY_NO                                       as                                       YEAR_YYYY_NO 
		, WKDY_IND                                           as                                           WKDY_IND 
		, FISCAL_YEAR_BEGIN_DATE                             as                             FISCAL_YEAR_BEGIN_DATE 
		, FISCAL_YEAR_END_DATE                               as                               FISCAL_YEAR_END_DATE 
		, PREVIOUS_FISCAL_YEAR_BEGIN_DATE                    as                    PREVIOUS_FISCAL_YEAR_BEGIN_DATE 
		, PREVIOUS_FISCAL_YEAR_END_DATE                      as                      PREVIOUS_FISCAL_YEAR_END_DATE 
		, FISCAL_YYYY_NUMBER                                 as                                 FISCAL_YYYY_NUMBER 
		, FISCAL_QUARTER_NAME                                as                                FISCAL_QUARTER_NAME 
		, FISCAL_QUARTER_NUMBER                              as                              FISCAL_QUARTER_NUMBER 
		from SRC_DATE
            )

---- RENAME LAYER ----
,

RENAME_DATE as ( SELECT 
		  DATE_DIM_KEY                                       as                                       DATE_DIM_KEY
		, ACTUAL_DT                                          as                                        ACTUAL_DATE
		, FULL_DT_NM                                         as                                          DATE_DESC
		, HOL_IND                                            as                                        HOLIDAY_IND
		, WKEND_IND                                          as                                        WEEKEND_IND
		, LAST_DAY_IN_WK_IND                                 as                                    WEEK_ENDING_IND
		, LAST_DAY_IN_MM_IND                                 as                                   MONTH_ENDING_IND
		, CAL_YYYY_MM_NO                                     as                                  YEAR_MONTH_NUMBER
		, MONTH_MM_NO                                        as                                       MONTH_NUMBER
		, MONTH_MM_NO_STRING                                 as                                MONTH_NUMBER_STRING
		, MONTH_NM                                           as                                         MONTH_NAME
		, MONTH_SHORT_NM                                     as                                   MONTH_SHORT_NAME
		, CRNT_MONTH_BGNG_DATE                               as                                   MONTH_BEGIN_DATE
		, CRNT_MONTH_FIRST_BSNS_DATE                         as                          MONTH_FIRST_BUSINESS_DATE
		, CRNT_MONTH_BGNG_DATE                               as                                     MONTH_END_DATE
		, CRNT_MONTH_LAST_BSNS_DATE                          as                           MONTH_LAST_BUSINESS_DATE
		, CAL_QUARTER_NO                                     as                                     QUARTER_NUMBER
		, CAL_QUARTER_NM                                     as                                       QUARTER_NAME
		, CAL_HALF_YR                                        as                                          HALF_YEAR
		, YEAR_YYYY_NO                                       as                                        YEAR_NUMBER
		, WKDY_IND                                           as                                        WEEKDAY_IND
		, FISCAL_YEAR_BEGIN_DATE                             as                                FSCL_YEAR_BGNG_DATE
		, FISCAL_YEAR_END_DATE                               as                              FSCL_YEAR_ENDING_DATE
		, PREVIOUS_FISCAL_YEAR_BEGIN_DATE                    as                           PRVS_FSCL_YEAR_BGNG_DATE
		, PREVIOUS_FISCAL_YEAR_END_DATE                      as                         PRVS_FSCL_YEAR_ENDING_DATE
		, FISCAL_YYYY_NUMBER                                 as                                       FISC_YYYY_NO
		, FISCAL_QUARTER_NAME                                as                                    FISC_QUARTER_NM
		, FISCAL_QUARTER_NUMBER                              as                                    FISC_QUARTER_NO
				FROM     LOGIC_DATE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DATE                           as ( SELECT * from    RENAME_DATE   ),

---- JOIN LAYER ----

 JOIN_DATE  as  ( SELECT * 
				FROM  FILTER_DATE )
 SELECT * FROM  JOIN_DATE