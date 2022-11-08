

      create or replace  table DEV_EDW.STAGING.DST_INDIVIDUAL_COVERAGE_TYPE  as
      (---- SRC LAYER ----
WITH
SRC_PPP            as ( SELECT *     FROM     STAGING.STG_POLICY_PERIOD_PARTICIPATION ),
//SRC_PPP            as ( SELECT *     FROM     STG_POLICY_PERIOD_PARTICIPATION) ,

---- LOGIC LAYER ----


LOGIC_PPP as ( SELECT 
		  TRIM( COV_TYP_CD )                                 as                                         COV_TYP_CD 
		, TRIM( COV_TYP_NM )                                 as                                         COV_TYP_NM 
		, TRIM( TTL_TYP_CD )                                 as                                         TTL_TYP_CD 
		, TRIM( TTL_TYP_NM )                                 as                                         TTL_TYP_NM 
		, TRIM( PPPIE_COV_IND )                              as                                      PPPIE_COV_IND 
		, TRIM( PTCP_TYP_CD )                                as                                        PTCP_TYP_CD 
		FROM SRC_PPP
            )

---- RENAME LAYER ----
,

RENAME_PPP        as ( SELECT 
		  COV_TYP_CD                                         as                                         COV_TYP_CD
		, COV_TYP_NM                                         as                                         COV_TYP_NM
		, TTL_TYP_CD                                         as                                         TTL_TYP_CD
		, TTL_TYP_NM                                         as                                         TTL_TYP_NM
		, PPPIE_COV_IND                                      as                                      PPPIE_COV_IND
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD 
				FROM     LOGIC_PPP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PPP                            as ( SELECT * FROM    RENAME_PPP 
                                            WHERE PTCP_TYP_CD = 'COV_INDV'  ),

---- JOIN LAYER ----

 JOIN_PPP         as  ( SELECT * 
				FROM  FILTER_PPP )

---- ETL LAYER ----
,
ETL AS (
 SELECT DISTINCT   
         md5(cast(
    
    coalesce(cast(COV_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(TTL_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(PPPIE_COV_IND as 
    varchar
), '')

 as 
    varchar
)) AS  UNIQUE_ID_KEY
		 , *
 
 FROM  JOIN_PPP
)

SELECT * FROM ETL
      );
    