
  create or replace  view DEV_EDW.STAGING.DSV_EXAM_CASE_DETAIL  as (
    

---- SRC LAYER ----
WITH
SRC_EXAM           as ( SELECT *     FROM     STAGING.DST_CASE_EXAM_SCHEDULE ),
//SRC_EXAM           as ( SELECT *     FROM     DST_CASE_EXAM_SCHEDULE) ,

---- LOGIC LAYER ----


LOGIC_EXAM as ( SELECT 
		  CD_EXM_REQS_TYP_CD                                 as                                 CD_EXM_REQS_TYP_CD 
		, CPS_TYP_CD                                         as                                         CPS_TYP_CD 
		, CPS_TYP_CD_SCND                                    as                                    CPS_TYP_CD_SCND 
		, CD_ADNDM_REQS_TYP_CD                               as                               CD_ADNDM_REQS_TYP_CD 
		, LANG_TYP_CD                                        as                                        LANG_TYP_CD 
		, CD_EXM_REQS_TYP_NM                                 as                                 CD_EXM_REQS_TYP_NM 
		, CPS_TYP_NM                                         as                                         CPS_TYP_NM 
		, CPS_TYP_NM_SCND                                    as                                    CPS_TYP_NM_SCND 
		, CD_ADNDM_REQS_TYP_NM                               as                               CD_ADNDM_REQS_TYP_NM 
		, LANG_TYP_NM                                        as                                        LANG_TYP_NM 
		FROM SRC_EXAM
            )

---- RENAME LAYER ----
,

RENAME_EXAM       as ( SELECT 
		  CD_EXM_REQS_TYP_CD                                 as                             EXAM_REQUEST_TYPE_CODE
		, CPS_TYP_CD                                         as               PRIMARY_PROVIDER_SPECIALTY_TYPE_CODE
		, CPS_TYP_CD_SCND                                    as             SECONDARY_PROVIDER_SPECIALTY_TYPE_CODE
		, CD_ADNDM_REQS_TYP_CD                               as                         ADDENDUM_REQUEST_TYPE_CODE
		, LANG_TYP_CD                                        as                                 LANGUAGE_TYPE_CODE
		, CD_EXM_REQS_TYP_NM                                 as                             EXAM_REQUEST_TYPE_DESC
		, CPS_TYP_NM                                         as                PRIMARY_PROVIDER_SPEIALTY_TYPE_DESC
		, CPS_TYP_NM_SCND                                    as              SECONDARY_PROVIDER_SPEIALTY_TYPE_DESC
		, CD_ADNDM_REQS_TYP_NM                               as                         ADDENDUM_REQUEST_TYPE_DESC
		, LANG_TYP_NM                                        as                                 LANGUAGE_TYPE_DESC 
				FROM     LOGIC_EXAM   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_EXAM                           as ( SELECT * FROM    RENAME_EXAM   ),

---- JOIN LAYER ----

 JOIN_EXAM        as  ( SELECT * FROM  FILTER_EXAM ),
 ------ETL LAYER------------
 ETL AS(SELECT DISTINCT
md5(cast(
    
    coalesce(cast(EXAM_REQUEST_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(PRIMARY_PROVIDER_SPECIALTY_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(SECONDARY_PROVIDER_SPECIALTY_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(ADDENDUM_REQUEST_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(LANGUAGE_TYPE_CODE as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
,EXAM_REQUEST_TYPE_CODE
,PRIMARY_PROVIDER_SPECIALTY_TYPE_CODE
,SECONDARY_PROVIDER_SPECIALTY_TYPE_CODE
,ADDENDUM_REQUEST_TYPE_CODE
,LANGUAGE_TYPE_CODE
,EXAM_REQUEST_TYPE_DESC
,PRIMARY_PROVIDER_SPEIALTY_TYPE_DESC
,SECONDARY_PROVIDER_SPEIALTY_TYPE_DESC
,ADDENDUM_REQUEST_TYPE_DESC
,LANGUAGE_TYPE_DESC
FROM JOIN_EXAM)

SELECT * FROM ETL
  );
