

      create or replace  table DEV_EDW.STAGING.DST_CASE_STATUS  as
      (---- SRC LAYER ----
WITH
SRC_CS             as ( SELECT *     FROM     STAGING.STG_CASE_STATUS_HISTORY ),
//SRC_CS             as ( SELECT *     FROM     STG_CASE_STATUS_HISTORY) ,

---- LOGIC LAYER ----


LOGIC_CS as ( SELECT 
		  TRIM( CASE_STT_TYP_CD )                            as                                    CASE_STT_TYP_CD 
		, TRIM( CASE_STS_TYP_CD )                            as                                    CASE_STS_TYP_CD 
		, TRIM( CASE_STS_RSN_TYP_CD )                        as                                CASE_STS_RSN_TYP_CD 
		, TRIM( CASE_STT_TYP_NM )                            as                                    CASE_STT_TYP_NM 
		, TRIM( CASE_STS_TYP_NM )                            as                                    CASE_STS_TYP_NM 
		, TRIM( CASE_STS_RSN_TYP_NM )                        as                                CASE_STS_RSN_TYP_NM 
		FROM SRC_CS
            )

---- RENAME LAYER ----
,

RENAME_CS         as ( SELECT 
		  CASE_STT_TYP_CD                                    as                                    CASE_STT_TYP_CD
		, CASE_STS_TYP_CD                                    as                                    CASE_STS_TYP_CD
		, CASE_STS_RSN_TYP_CD                                as                                CASE_STS_RSN_TYP_CD
		, CASE_STT_TYP_NM                                    as                                    CASE_STT_TYP_NM
		, CASE_STS_TYP_NM                                    as                                    CASE_STS_TYP_NM
		, CASE_STS_RSN_TYP_NM                                as                                CASE_STS_RSN_TYP_NM 
				FROM     LOGIC_CS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CS                             as ( SELECT * FROM    RENAME_CS   ),

---- JOIN LAYER ----

 JOIN_CS          as  ( SELECT * FROM  FILTER_CS ),
 
 ------ETL LAYER------------
ETL AS(SELECT DISTINCT
md5(cast(
    
    coalesce(cast(CASE_STT_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CASE_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CASE_STS_RSN_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
,CASE_STT_TYP_CD
,CASE_STS_TYP_CD
,CASE_STS_RSN_TYP_CD
,CASE_STT_TYP_NM
,CASE_STS_TYP_NM
,CASE_STS_RSN_TYP_NM
FROM JOIN_CS)

SELECT * FROM ETL
      );
    