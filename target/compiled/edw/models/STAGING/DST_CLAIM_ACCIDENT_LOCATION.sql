---- SRC LAYER ----
WITH
SRC_C as ( SELECT *     from     STAGING.STG_CLAIM ),
//SRC_C as ( SELECT *     from     STG_CLAIM) ,

---- LOGIC LAYER ----

LOGIC_C as ( SELECT 
		    md5(cast(
    
    coalesce(cast(OCCR_PRMS_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_CNTRY_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_STT_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_STT_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_CNTY_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_CITY_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_POST_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_STR_1 as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_STR_2 as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_COMT as 
    varchar
), '')

 as 
    varchar
))  
                                                             as                                      UNIQUE_ID_KEY
		, TRIM( OCCR_PRMS_TYP_NM )                           as                                   OCCR_PRMS_TYP_NM 
		, TRIM( CLM_OCCR_LOC_CNTRY_NM )                      as                              CLM_OCCR_LOC_CNTRY_NM 
		, TRIM( CLM_OCCR_LOC_STT_CD )                        as                                CLM_OCCR_LOC_STT_CD 
		, TRIM( CLM_OCCR_LOC_STT_NM )                        as                                CLM_OCCR_LOC_STT_NM 
		, TRIM( CLM_OCCR_LOC_CNTY_NM )                       as                               CLM_OCCR_LOC_CNTY_NM 
		, TRIM( CLM_OCCR_LOC_CITY_NM )                       as                               CLM_OCCR_LOC_CITY_NM 
		, TRIM( CLM_OCCR_LOC_POST_CD )                       as                               CLM_OCCR_LOC_POST_CD 
		, TRIM( CLM_OCCR_LOC_NM )                            as                                    CLM_OCCR_LOC_NM 
		, TRIM( CLM_OCCR_LOC_STR_1 )                         as                                 CLM_OCCR_LOC_STR_1 
		, TRIM( CLM_OCCR_LOC_STR_2 )                         as                                 CLM_OCCR_LOC_STR_2 
		, TRIM( CLM_OCCR_LOC_COMT )                          as                                  CLM_OCCR_LOC_COMT 
		from SRC_C
            )

---- RENAME LAYER ----
,

RENAME_C as ( SELECT 
		   UNIQUE_ID_KEY                                     as                                      UNIQUE_ID_KEY
		, OCCR_PRMS_TYP_NM                                   as                                   OCCR_PRMS_TYP_NM
		, CLM_OCCR_LOC_CNTRY_NM                              as                              CLM_OCCR_LOC_CNTRY_NM
		, CLM_OCCR_LOC_STT_CD                                as                                CLM_OCCR_LOC_STT_CD
		, CLM_OCCR_LOC_STT_NM                                as                                CLM_OCCR_LOC_STT_NM
		, CLM_OCCR_LOC_CNTY_NM                               as                               CLM_OCCR_LOC_CNTY_NM
		, CLM_OCCR_LOC_CITY_NM                               as                               CLM_OCCR_LOC_CITY_NM
		, CLM_OCCR_LOC_POST_CD                               as                               CLM_OCCR_LOC_POST_CD
		, CLM_OCCR_LOC_NM                                    as                                    CLM_OCCR_LOC_NM
		, CLM_OCCR_LOC_STR_1                                 as                                 CLM_OCCR_LOC_STR_1
		, CLM_OCCR_LOC_STR_2                                 as                                 CLM_OCCR_LOC_STR_2
		, CLM_OCCR_LOC_COMT                                  as                                  CLM_OCCR_LOC_COMT 
				FROM     LOGIC_C   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_C                              as ( SELECT * from    RENAME_C   ),

---- JOIN LAYER ----

 JOIN_C  as  ( SELECT * 
				FROM  FILTER_C )
 SELECT DISTINCT
  UNIQUE_ID_KEY
, OCCR_PRMS_TYP_NM
, CLM_OCCR_LOC_CNTRY_NM
, CLM_OCCR_LOC_STT_CD
, CLM_OCCR_LOC_STT_NM
, CLM_OCCR_LOC_CNTY_NM
, CLM_OCCR_LOC_CITY_NM
, CLM_OCCR_LOC_POST_CD
, CLM_OCCR_LOC_NM
, CLM_OCCR_LOC_STR_1
, CLM_OCCR_LOC_STR_2
, CLM_OCCR_LOC_COMT
  FROM  JOIN_C