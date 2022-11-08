

      create or replace  table DEV_EDW.STAGING.DST_DOCUMENT_TYPE  as
      (---- SRC LAYER ----
WITH
SRC_DOC as ( SELECT *     from     STAGING.STG_DOCUMENT ),
//SRC_DOC as ( SELECT *     from     STG_DOCUMENT) ,

---- LOGIC LAYER ----

LOGIC_DOC as ( SELECT 
		  TRIM( DOCM_TYP_REF_NO )                            as                                    DOCM_TYP_REF_NO 
		, TRIM( DOCM_TYP_NM )                                as                                        DOCM_TYP_NM 
		, TRIM( DOCM_TYP_DESC )                              as                                      DOCM_TYP_DESC 
		, DOCM_TYP_VER_NO                                    as                                    DOCM_TYP_VER_NO 
		, TRIM( DOCM_TYP_VER_EVNT_DESC )                     as                             DOCM_TYP_VER_EVNT_DESC 
		, TRIM( DOCM_OCCR_TYP_CD )                           as                                   DOCM_OCCR_TYP_CD 
		, TRIM( DOCM_OCCR_TYP_NM )                           as                                   DOCM_OCCR_TYP_NM 
		, TRIM( DOCM_PPR_STK_TYP_CD )                        as                                DOCM_PPR_STK_TYP_CD 
		, TRIM( DOCM_PPR_STK_TYP_NM )                        as                                DOCM_PPR_STK_TYP_NM 
		, TRIM( DOCM_CTG_TYP_CD )                            as                                    DOCM_CTG_TYP_CD 
		, TRIM( DOCM_CTG_TYP_NM )                            as                                    DOCM_CTG_TYP_NM 
		, TRIM( DOCM_TYP_SRC_TYP_CD )                        as                                DOCM_TYP_SRC_TYP_CD 
		, TRIM( DOCM_TYP_SRC_TYP_NM )                        as                                DOCM_TYP_SRC_TYP_NM 
		, DOCM_TYP_VER_EFF_DT                                as                                DOCM_TYP_VER_EFF_DT 
		, DOCM_TYP_VER_END_DT                                as                                DOCM_TYP_VER_END_DT 
		, TRIM( DOCM_TYP_VER_TEMPL_FILE_NM )                 as                         DOCM_TYP_VER_TEMPL_FILE_NM 
		from SRC_DOC
            )

---- RENAME LAYER ----
,

RENAME_DOC as ( SELECT 
		  DOCM_TYP_REF_NO                                    as                                    DOCM_TYP_REF_NO
		, DOCM_TYP_NM                                        as                                        DOCM_TYP_NM
		, DOCM_TYP_DESC                                      as                                      DOCM_TYP_DESC
		, DOCM_TYP_VER_NO                                    as                                    DOCM_TYP_VER_NO
		, DOCM_TYP_VER_EVNT_DESC                             as                             DOCM_TYP_VER_EVNT_DESC
		, DOCM_OCCR_TYP_CD                                   as                                   DOCM_OCCR_TYP_CD
		, DOCM_OCCR_TYP_NM                                   as                                   DOCM_OCCR_TYP_NM
		, DOCM_PPR_STK_TYP_CD                                as                                DOCM_PPR_STK_TYP_CD
		, DOCM_PPR_STK_TYP_NM                                as                                DOCM_PPR_STK_TYP_NM
		, DOCM_CTG_TYP_CD                                    as                                    DOCM_CTG_TYP_CD
		, DOCM_CTG_TYP_NM                                    as                                    DOCM_CTG_TYP_NM
		, DOCM_TYP_SRC_TYP_CD                                as                                DOCM_TYP_SRC_TYP_CD
		, DOCM_TYP_SRC_TYP_NM                                as                                DOCM_TYP_SRC_TYP_NM
		, DOCM_TYP_VER_EFF_DT                                as                                DOCM_TYP_VER_EFF_DT
		, DOCM_TYP_VER_END_DT                                as                                DOCM_TYP_VER_END_DT
		, DOCM_TYP_VER_TEMPL_FILE_NM                         as                         DOCM_TYP_VER_TEMPL_FILE_NM 
				FROM     LOGIC_DOC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DOC                            as ( SELECT * from    RENAME_DOC   ),

---- JOIN LAYER ----

 JOIN_DOC  as  ( SELECT * 
				FROM  FILTER_DOC )
 SELECT 
 md5(cast(
    
    coalesce(cast(DOCM_TYP_REF_NO as 
    varchar
), '') || '-' || coalesce(cast(DOCM_TYP_VER_NO as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
,DOCM_TYP_REF_NO
,DOCM_TYP_NM
,DOCM_TYP_DESC
,DOCM_TYP_VER_NO
,DOCM_OCCR_TYP_CD
,DOCM_OCCR_TYP_NM
,DOCM_PPR_STK_TYP_CD
,DOCM_PPR_STK_TYP_NM
,DOCM_CTG_TYP_CD
,DOCM_CTG_TYP_NM
,DOCM_TYP_SRC_TYP_CD
,DOCM_TYP_SRC_TYP_NM
,DOCM_TYP_VER_EVNT_DESC
,DOCM_TYP_VER_TEMPL_FILE_NM
,DOCM_TYP_VER_EFF_DT
,Case when DOCM_TYP_VER_END_DT = Lead(DOCM_TYP_VER_EFF_DT)over(partition by DOCM_TYP_REF_NO--, DOCM_TYP_VER_NO
 order by DOCM_TYP_VER_NO,DOCM_TYP_VER_EFF_DT) then DOCM_TYP_VER_END_DT-1 else DOCM_TYP_VER_END_DT end as DOCM_TYP_VER_END_DT
 from (
select DISTINCT
  
  DOCM_TYP_REF_NO
, DOCM_TYP_NM
, DOCM_TYP_DESC
, DOCM_TYP_VER_NO
, DOCM_TYP_VER_EVNT_DESC
, DOCM_OCCR_TYP_CD
, DOCM_OCCR_TYP_NM
, DOCM_PPR_STK_TYP_CD
, DOCM_PPR_STK_TYP_NM
, DOCM_CTG_TYP_CD
, DOCM_CTG_TYP_NM
, DOCM_TYP_SRC_TYP_CD
, DOCM_TYP_SRC_TYP_NM
, DOCM_TYP_VER_EFF_DT
, DOCM_TYP_VER_END_DT
, DOCM_TYP_VER_TEMPL_FILE_NM

 from JOIN_DOC
 order by DOCM_TYP_REF_NO,DOCM_TYP_VER_NO,DOCM_TYP_VER_EFF_DT)
      );
    