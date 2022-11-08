---- SRC LAYER ----
WITH
SRC_DATE as ( SELECT *     from     DEV_VIEWS.BWC_FILES.DATE_DIM ),
//SRC_DATE as ( SELECT *     from     DATE_DIM) ,

---- LOGIC LAYER ----

LOGIC_DATE as ( SELECT 
		  DATE_DIM_KEY                                             as                                       DATE_DIM_KEY 
		, CAST (ACTUAL_DT  AS DATE)                                as                                          ACTUAL_DT 
		, UPPER(TRIM( WKDY_IND ))                                   as                                           WKDY_IND 
		, UPPER(TRIM( WKEND_IND ))                                  as                                          WKEND_IND 
		, UPPER(TRIM( HOL_IND ) )                                   as                                            HOL_IND 
		, UPPER(TRIM( FULL_DT_NM ))                                 as                                         FULL_DT_NM 
		, FSCL_YEAR_BGNG_DATE                                	   as                                FSCL_YEAR_BGNG_DATE 
		, FSCL_YEAR_ENDNG_DATE                                     as                               FSCL_YEAR_ENDNG_DATE 
		, PRVS_FSCL_YEAR_BGNG_DATE                                 as                           PRVS_FSCL_YEAR_BGNG_DATE 
		, PRVS_FSCL_YEAR_ENDNG_DATE                                as                          PRVS_FSCL_YEAR_ENDNG_DATE 
		, FISC_YYYY_NO                                             as                                       FISC_YYYY_NO 
		, UPPER(TRIM( FISC_QUARTER_NM ))                            as                                    FISC_QUARTER_NM 
		, FISC_QUARTER_NO                                          as                                    FISC_QUARTER_NO 
		, UPPER(TRIM( LAST_DAY_IN_WK_IND ))                         as                                 LAST_DAY_IN_WK_IND 
		, UPPER(TRIM( LAST_DAY_IN_MM_IND ))                         as                                 LAST_DAY_IN_MM_IND 
		, UPPER(TRIM( CAL_YYYY_MM_NO ))                             as                                     CAL_YYYY_MM_NO 
		, UPPER(TRIM( MONTH_MM_NO ))                                as                                        MONTH_MM_NO 
		, UPPER(TRIM( MONTH_NM ))                                   as                                           MONTH_NM 
		, UPPER(TRIM( MONTH_SHORT_NM ))                             as                                     MONTH_SHORT_NM 
		, CRNT_MONTH_BGNG_DATE                                     as                               CRNT_MONTH_BGNG_DATE 
		, CRNT_MONTH_FIRST_BSNS_DATE                               as                         CRNT_MONTH_FIRST_BSNS_DATE 
		, CRNT_MONTH_LAST_BSNS_DATE                                as                          CRNT_MONTH_LAST_BSNS_DATE 
		, CAL_QUARTER_NO                                           as                                     CAL_QUARTER_NO 
		, UPPER(TRIM( CAL_QUARTER_NM ))                             as                                     CAL_QUARTER_NM 
		, UPPER(TRIM( CAL_HALF_YR ))                                as                                        CAL_HALF_YR 
		, YEAR_YYYY_NO                                             as                                       YEAR_YYYY_NO 
		from SRC_DATE
            )

---- RENAME LAYER ----
,

RENAME_DATE as ( SELECT 
		  DATE_DIM_KEY                                       as                                       DATE_DIM_KEY
		, ACTUAL_DT                                          as                                          ACTUAL_DT
		, WKDY_IND                                           as                                           WKDY_IND
		, WKEND_IND                                          as                                          WKEND_IND
		, HOL_IND                                            as                                            HOL_IND
		, FULL_DT_NM                                         as                                         FULL_DT_NM
		, FSCL_YEAR_BGNG_DATE                                as                                FSCL_YEAR_BGNG_DATE
		, FSCL_YEAR_ENDNG_DATE                               as                               FSCL_YEAR_ENDNG_DATE
		, PRVS_FSCL_YEAR_BGNG_DATE                           as                           PRVS_FSCL_YEAR_BGNG_DATE
		, PRVS_FSCL_YEAR_ENDNG_DATE                          as                          PRVS_FSCL_YEAR_ENDNG_DATE
		, FISC_YYYY_NO                                       as                                       FISC_YYYY_NO
		, FISC_QUARTER_NM                                    as                                    FISC_QUARTER_NM
		, FISC_QUARTER_NO                                    as                                    FISC_QUARTER_NO
		, LAST_DAY_IN_WK_IND                                 as                                 LAST_DAY_IN_WK_IND
		, LAST_DAY_IN_MM_IND                                 as                                 LAST_DAY_IN_MM_IND
		, CAL_YYYY_MM_NO                                     as                                     CAL_YYYY_MM_NO
		, MONTH_MM_NO                                        as                                        MONTH_MM_NO
		, MONTH_MM_NO                                        as                                 MONTH_MM_NO_STRING
		, MONTH_NM                                           as                                           MONTH_NM
		, MONTH_SHORT_NM                                     as                                     MONTH_SHORT_NM
		, CRNT_MONTH_BGNG_DATE                               as                               CRNT_MONTH_BGNG_DATE
		, CRNT_MONTH_FIRST_BSNS_DATE                         as                         CRNT_MONTH_FIRST_BSNS_DATE
		, CRNT_MONTH_LAST_BSNS_DATE                          as                          CRNT_MONTH_LAST_BSNS_DATE
		, CAL_QUARTER_NO                                     as                                     CAL_QUARTER_NO
		, CAL_QUARTER_NM                                     as                                     CAL_QUARTER_NM
		, CAL_HALF_YR                                        as                                        CAL_HALF_YR
		, YEAR_YYYY_NO                                       as                                       YEAR_YYYY_NO 
				FROM     LOGIC_DATE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DATE                           as ( SELECT * from    RENAME_DATE   ),

---- JOIN LAYER ----

 JOIN_DATE  as  ( SELECT * 
				FROM  FILTER_DATE )
 SELECT * FROM  JOIN_DATE