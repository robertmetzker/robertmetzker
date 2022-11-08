---- SRC LAYER ----
WITH
SRC_CS as ( SELECT *     from     STAGING.STG_CASES ),
//SRC_CS as ( SELECT *     from     STG_CASES) ,

---- LOGIC LAYER ----

LOGIC_CS as ( SELECT 
		   md5(cast(
    
    coalesce(cast(APP_CNTX_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CASE_CTG_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CASE_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CASE_PRTY_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CASE_RSOL_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                                      UNIQUE_ID_KEY                           
		, TRIM( APP_CNTX_TYP_CD )                            as                                    APP_CNTX_TYP_CD 
		, TRIM( CASE_CTG_TYP_CD )                            as                                    CASE_CTG_TYP_CD 
		, TRIM( CASE_TYP_CD )                                as                                        CASE_TYP_CD 
		, TRIM( CASE_PRTY_TYP_CD )                           as                                   CASE_PRTY_TYP_CD 
		, TRIM( CASE_RSOL_TYP_CD )                           as                                   CASE_RSOL_TYP_CD 
		, TRIM( APP_CNTX_TYP_NM )                            as                                    APP_CNTX_TYP_NM 
		, TRIM( CASE_CTG_TYP_NM )                            as                                    CASE_CTG_TYP_NM 
		, TRIM( CASE_TYP_NM )                                as                                        CASE_TYP_NM 
		, TRIM( CASE_PRTY_TYP_NM )                           as                                   CASE_PRTY_TYP_NM 
		, TRIM( CASE_RSOL_TYP_NM )                           as                                   CASE_RSOL_TYP_NM 
		from SRC_CS
            )

---- RENAME LAYER ----
,

RENAME_CS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD
		, CASE_CTG_TYP_CD                                    as                                    CASE_CTG_TYP_CD
		, CASE_TYP_CD                                        as                                        CASE_TYP_CD
		, CASE_PRTY_TYP_CD                                   as                                   CASE_PRTY_TYP_CD
		, CASE_RSOL_TYP_CD                                   as                                   CASE_RSOL_TYP_CD
		, APP_CNTX_TYP_NM                                    as                                    APP_CNTX_TYP_NM
		, CASE_CTG_TYP_NM                                    as                                    CASE_CTG_TYP_NM
		, CASE_TYP_NM                                        as                                        CASE_TYP_NM
		, CASE_PRTY_TYP_NM                                   as                                   CASE_PRTY_TYP_NM
		, CASE_RSOL_TYP_NM                                   as                                   CASE_RSOL_TYP_NM 
				FROM     LOGIC_CS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CS                             as ( SELECT * from    RENAME_CS   ),

---- JOIN LAYER ----

 JOIN_CS  as  ( SELECT * 
				FROM  FILTER_CS )
 SELECT DISTINCT
  UNIQUE_ID_KEY
, APP_CNTX_TYP_CD
, CASE_CTG_TYP_CD
, CASE_TYP_CD
, CASE_PRTY_TYP_CD
, CASE_RSOL_TYP_CD
, APP_CNTX_TYP_NM
, CASE_CTG_TYP_NM
, CASE_TYP_NM
, CASE_PRTY_TYP_NM
, CASE_RSOL_TYP_NM
 FROM  JOIN_CS
 GROUP BY
   UNIQUE_ID_KEY
, APP_CNTX_TYP_CD
, CASE_CTG_TYP_CD
, CASE_TYP_CD
, CASE_PRTY_TYP_CD
, CASE_RSOL_TYP_CD
, APP_CNTX_TYP_NM
, CASE_CTG_TYP_NM
, CASE_TYP_NM
, CASE_PRTY_TYP_NM
, CASE_RSOL_TYP_NM