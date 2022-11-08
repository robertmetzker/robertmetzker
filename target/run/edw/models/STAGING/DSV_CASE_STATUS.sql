
  create or replace  view DEV_EDW.STAGING.DSV_CASE_STATUS  as (
    

---- SRC LAYER ----
WITH
SRC_CS             as ( SELECT *     FROM     STAGING.DST_CASE_STATUS ),
//SRC_CS             as ( SELECT *     FROM     DST_CASE_STATUS) ,

---- LOGIC LAYER ----


LOGIC_CS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, CASE_STT_TYP_CD                                    as                                    CASE_STT_TYP_CD 
		, CASE_STS_TYP_CD                                    as                                    CASE_STS_TYP_CD 
		, CASE_STS_RSN_TYP_CD                                as                                CASE_STS_RSN_TYP_CD 
		, CASE_STT_TYP_NM                                    as                                    CASE_STT_TYP_NM 
		, CASE_STS_TYP_NM                                    as                                    CASE_STS_TYP_NM 
		, CASE_STS_RSN_TYP_NM                                as                                CASE_STS_RSN_TYP_NM 
		FROM SRC_CS
            )

---- RENAME LAYER ----
,

RENAME_CS         as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CASE_STT_TYP_CD                                    as                                    CASE_STATE_CODE
		, CASE_STS_TYP_CD                                    as                                   CASE_STATUS_CODE
		, CASE_STS_RSN_TYP_CD                                as                            CASE_STATUS_REASON_CODE
		, CASE_STT_TYP_NM                                    as                                    CASE_STATE_DESC
		, CASE_STS_TYP_NM                                    as                                   CASE_STATUS_DESC
		, CASE_STS_RSN_TYP_NM                                as                            CASE_STATUS_REASON_DESC 
				FROM     LOGIC_CS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CS                             as ( SELECT * FROM    RENAME_CS   ),

---- JOIN LAYER ----

 JOIN_CS          as  ( SELECT * 
				FROM  FILTER_CS )
 SELECT * FROM  JOIN_CS
  );
