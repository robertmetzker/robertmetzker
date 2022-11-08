

      create or replace  table DEV_EDW.STAGING.DST_DATE  as
      (---- SRC LAYER ----
WITH
SRC_DATE as ( SELECT *     from      STAGING.STG_DATE ),
//SRC_DATE as ( SELECT *     from     STG_DATE) ,

---- LOGIC LAYER ----

LOGIC_DATE as ( SELECT 
		  DATE_DIM_KEY                                       as                                       DATE_DIM_KEY 
		, ACTUAL_DT                                          as                                          ACTUAL_DT 
		, TRIM( WKDY_IND )                                   as                                           WKDY_IND 
		, TRIM( WKEND_IND )                                  as                                          WKEND_IND 
		, TRIM( HOL_IND )                                    as                                            HOL_IND 
		, TRIM( FULL_DT_NM )                                 as                                         FULL_DT_NM 
		, FSCL_YEAR_BGNG_DATE                                as                                FSCL_YEAR_BGNG_DATE 
		, FSCL_YEAR_ENDNG_DATE                               as                               FSCL_YEAR_ENDNG_DATE 
		, PRVS_FSCL_YEAR_BGNG_DATE                           as                           PRVS_FSCL_YEAR_BGNG_DATE 
		, PRVS_FSCL_YEAR_ENDNG_DATE                          as                          PRVS_FSCL_YEAR_ENDNG_DATE 
		, FISC_YYYY_NO                                       as                                       FISC_YYYY_NO 
		, TRIM( FISC_QUARTER_NM )                            as                                    FISC_QUARTER_NM 
		, FISC_QUARTER_NO                                    as                                    FISC_QUARTER_NO 
		, TRIM( LAST_DAY_IN_MM_IND )                         as                                 LAST_DAY_IN_MM_IND 
		, TRIM( LAST_DAY_IN_WK_IND )                         as                                 LAST_DAY_IN_WK_IND 
		, TRIM( CAL_YYYY_MM_NO )                             as                                     CAL_YYYY_MM_NO 
		, MONTH_MM_NO                                        as                                        MONTH_MM_NO 
		, TRIM( MONTH_MM_NO_STRING )                         as                                 MONTH_MM_NO_STRING 
		, TRIM( MONTH_NM )                                   as                                           MONTH_NM 
		, TRIM( MONTH_SHORT_NM )                             as                                     MONTH_SHORT_NM 
		, CRNT_MONTH_BGNG_DATE                               as                               CRNT_MONTH_BGNG_DATE 
		, CRNT_MONTH_FIRST_BSNS_DATE                         as                         CRNT_MONTH_FIRST_BSNS_DATE 
		, CRNT_MONTH_LAST_BSNS_DATE                          as                          CRNT_MONTH_LAST_BSNS_DATE 
		, CAL_QUARTER_NO                                     as                                     CAL_QUARTER_NO 
		, TRIM( CAL_QUARTER_NM )                             as                                     CAL_QUARTER_NM 
		, TRIM( CAL_HALF_YR )                                as                                        CAL_HALF_YR 
		, YEAR_YYYY_NO                                       as                                       YEAR_YYYY_NO 
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
		, FSCL_YEAR_BGNG_DATE                                as                             FISCAL_YEAR_BEGIN_DATE
		, FSCL_YEAR_ENDNG_DATE                               as                               FISCAL_YEAR_END_DATE
		, PRVS_FSCL_YEAR_BGNG_DATE                           as                    PREVIOUS_FISCAL_YEAR_BEGIN_DATE
		, PRVS_FSCL_YEAR_ENDNG_DATE                          as                      PREVIOUS_FISCAL_YEAR_END_DATE
		, FISC_YYYY_NO                                       as                                 FISCAL_YYYY_NUMBER
		, FISC_QUARTER_NM                                    as                                FISCAL_QUARTER_NAME
		, FISC_QUARTER_NO                                    as                              FISCAL_QUARTER_NUMBER
		, LAST_DAY_IN_MM_IND                                 as                                 LAST_DAY_IN_MM_IND
		, LAST_DAY_IN_WK_IND                                 as                                 LAST_DAY_IN_WK_IND
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
				FROM     LOGIC_DATE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DATE                           as ( SELECT * from    RENAME_DATE   ),

---- JOIN LAYER ----

 JOIN_DATE  as  ( SELECT * 
				FROM  FILTER_DATE )
 SELECT * FROM  JOIN_DATE
      );
    