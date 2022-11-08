

---- SRC LAYER ----
WITH
SRC_PP             as ( SELECT *     FROM     STAGING.DST_POLICY_PERIOD ),
//SRC_PP             as ( SELECT *     FROM     DST_POLICY_PERIOD) ,

---- LOGIC LAYER ----


LOGIC_PP as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, POLICY_PERIOD_DESC                                 as                                 POLICY_PERIOD_DESC 
		, PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT 
		, PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT 
		, PEC_POLICY_IND                                     as                                     PEC_POLICY_IND 
		, NEW_POLICY_IND                                     as                                     NEW_POLICY_IND 
		, REPORTING_YEAR_EFFECTIVE_DATE                      as                      REPORTING_YEAR_EFFECTIVE_DATE 
		, REPORTING_YEAR_END_DATE                            as                            REPORTING_YEAR_END_DATE 
		, REPORTING_YEAR_DESC                                as                                REPORTING_YEAR_DESC 
		FROM SRC_PP
            )

---- RENAME LAYER ----
,

RENAME_PP         as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, POLICY_PERIOD_DESC                                 as                                 POLICY_PERIOD_DESC
		, PLCY_PRD_EFF_DT                                    as                       POLICY_PERIOD_EFFECTIVE_DATE
		, PLCY_PRD_END_DT                                    as                             POLICY_PERIOD_END_DATE
		, PEC_POLICY_IND                                     as                                     PEC_POLICY_IND
		, NEW_POLICY_IND                                     as                                     NEW_POLICY_IND
		, REPORTING_YEAR_EFFECTIVE_DATE                      as                      REPORTING_YEAR_EFFECTIVE_DATE
		, REPORTING_YEAR_END_DATE                            as                            REPORTING_YEAR_END_DATE
		, REPORTING_YEAR_DESC                                as                                REPORTING_YEAR_DESC 
				FROM     LOGIC_PP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PP                             as ( SELECT * FROM    RENAME_PP   ),

---- JOIN LAYER ----

 JOIN_PP          as  ( SELECT * 
				FROM  FILTER_PP )
 SELECT * FROM  JOIN_PP