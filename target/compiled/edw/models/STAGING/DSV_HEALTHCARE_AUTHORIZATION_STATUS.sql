

---- SRC LAYER ----
WITH
SRC_CS as ( SELECT *     from     STAGING.DST_HEALTHCARE_AUTHORIZATION_STATUS ),
//SRC_CS as ( SELECT *     from     DST_HEALTHCARE_AUTHORIZATION_STATUS) ,

---- LOGIC LAYER ----

LOGIC_CS as ( SELECT 
		  UNIQUE_ID_KEY                                      AS                                      UNIQUE_ID_KEY 
		, CASE_SERV_STS_TYP_CD                               AS                               CASE_SERV_STS_TYP_CD 
		, CASE_SERV_TYP_CD                                   AS                                   CASE_SERV_TYP_CD 
		, CASE_SERV_STS_TYP_NM                               AS                               CASE_SERV_STS_TYP_NM 
		, CASE_SERV_TYP_NM                                   AS                                   CASE_SERV_TYP_NM 
		, VOID_IND                                           as                                           VOID_IND 
		from SRC_CS
            )

---- RENAME LAYER ----
,

RENAME_CS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CASE_SERV_STS_TYP_CD                               as                          AUTHORIZATION_STATUS_CODE
		, CASE_SERV_TYP_CD                                   as                    AUTHORIZATION_SERVICE_TYPE_CODE
		, CASE_SERV_STS_TYP_NM                               as                          AUTHORIZATION_STATUS_DESC
		, CASE_SERV_TYP_NM                                   as                    AUTHORIZATION_SERVICE_TYPE_DESC
		, VOID_IND                                           as                     AUTHORIZATION_SERVICE_VOID_IND
				FROM     LOGIC_CS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CS                             as ( SELECT * from    RENAME_CS   ),

---- JOIN LAYER ----

 JOIN_CS  as  ( SELECT  * 
				FROM  FILTER_CS )
 SELECT 
  
  UNIQUE_ID_KEY
, AUTHORIZATION_STATUS_CODE
, AUTHORIZATION_SERVICE_TYPE_CODE
, AUTHORIZATION_STATUS_DESC
, AUTHORIZATION_SERVICE_TYPE_DESC
, AUTHORIZATION_SERVICE_VOID_IND
FROM  JOIN_CS