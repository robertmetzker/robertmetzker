

      create or replace  table DEV_EDW.STAGING.DST_HEALTHCARE_AUTHORIZATION_STATUS  as
      (---- SRC LAYER ----
WITH
SRC_CS as ( SELECT *     from     STAGING.STG_CASE_SERVICE ),
//SRC_CS as ( SELECT *     from     STG_CASE_SERVICE) ,

---- LOGIC LAYER ----

LOGIC_CS as ( SELECT 
		  md5(cast(
    
    coalesce(cast(CASE_SERV_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CASE_SERV_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(VOID_IND as 
    varchar
), '')

 as 
    varchar
)) AS                                      UNIQUE_ID_KEY 
		, TRIM( CASE_SERV_STS_TYP_CD )                       AS                               CASE_SERV_STS_TYP_CD 
		, TRIM( CASE_SERV_TYP_CD )                           AS                                   CASE_SERV_TYP_CD 
		, TRIM( CASE_SERV_STS_TYP_NM )                       AS                               CASE_SERV_STS_TYP_NM 
		, TRIM( CASE_SERV_TYP_NM )                           AS                                   CASE_SERV_TYP_NM
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_CS
            )

---- RENAME LAYER ----
,

RENAME_CS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CASE_SERV_STS_TYP_CD                               as                               CASE_SERV_STS_TYP_CD
		, CASE_SERV_TYP_CD                                   as                                   CASE_SERV_TYP_CD
		, CASE_SERV_STS_TYP_NM                               as                               CASE_SERV_STS_TYP_NM
		, CASE_SERV_TYP_NM                                   as                                   CASE_SERV_TYP_NM 
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CS                             as ( SELECT * from    RENAME_CS   ),

---- JOIN LAYER ----

 JOIN_CS  as  ( SELECT * 
				FROM  FILTER_CS )
 SELECT DISTINCT 
  UNIQUE_ID_KEY
, CASE_SERV_STS_TYP_CD
, CASE_SERV_TYP_CD
, CASE_SERV_STS_TYP_NM
, CASE_SERV_TYP_NM
, VOID_IND
  FROM  JOIN_CS
      );
    