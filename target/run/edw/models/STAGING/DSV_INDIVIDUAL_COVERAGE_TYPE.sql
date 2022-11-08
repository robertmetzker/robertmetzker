
  create or replace  view DEV_EDW.STAGING.DSV_INDIVIDUAL_COVERAGE_TYPE  as (
    

---- SRC LAYER ----
WITH
SRC_ICT            as ( SELECT *     FROM     STAGING.DST_INDIVIDUAL_COVERAGE_TYPE ),
//SRC_ICT            as ( SELECT *     FROM     DST_INDIVIDUAL_COVERAGE_TYPE) ,

---- LOGIC LAYER ----


LOGIC_ICT as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, COV_TYP_CD                                         as                                         COV_TYP_CD 
		, COV_TYP_NM                                         as                                         COV_TYP_NM 
		, TTL_TYP_CD                                         as                                         TTL_TYP_CD 
		, TTL_TYP_NM                                         as                                         TTL_TYP_NM 
		, PPPIE_COV_IND                                      as                                      PPPIE_COV_IND 
		FROM SRC_ICT
            )

---- RENAME LAYER ----
,

RENAME_ICT        as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, COV_TYP_CD                                         as                      INDIVIDUAL_COVERAGE_TYPE_CODE
		, COV_TYP_NM                                         as                      INDIVIDUAL_COVERAGE_TYPE_DESC
		, TTL_TYP_CD                                         as                     INDIVIDUAL_COVERAGE_TITLE_CODE
		, TTL_TYP_NM                                         as                          INDIVIDUAL_COVERAGE_TITLE
		, PPPIE_COV_IND                                      as                  INDIVIDUAL_COVERAGE_INCLUSION_IND 
				FROM     LOGIC_ICT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ICT                            as ( SELECT * FROM    RENAME_ICT   ),

---- JOIN LAYER ----

 JOIN_ICT         as  ( SELECT * 
				FROM  FILTER_ICT )
 SELECT * FROM  JOIN_ICT
  );
