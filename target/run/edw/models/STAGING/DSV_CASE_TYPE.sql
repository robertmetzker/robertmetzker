
  create or replace  view DEV_EDW.STAGING.DSV_CASE_TYPE  as (
    

---- SRC LAYER ----
WITH
SRC_CS as ( SELECT *     from     STAGING.DST_CASE_TYPE ),
//SRC_CS as ( SELECT *     from     DST_CASE_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CS as ( SELECT 
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
		from SRC_CS
            )

---- RENAME LAYER ----
,

RENAME_CS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, APP_CNTX_TYP_CD                                    as                                  CONTEXT_TYPE_CODE
		, CASE_CTG_TYP_CD                                    as                            CASE_CATEGORY_TYPE_CODE
		, CASE_TYP_CD                                        as                                     CASE_TYPE_CODE
		, CASE_PRTY_TYP_CD                                   as                            CASE_PRIORITY_TYPE_CODE
		, CASE_RSOL_TYP_CD                                   as                          CASE_RESOLUTION_TYPE_CODE
		, APP_CNTX_TYP_NM                                    as                                  CONTEXT_TYPE_DESC
		, CASE_CTG_TYP_NM                                    as                            CASE_CATEGORY_TYPE_DESC
		, CASE_TYP_NM                                        as                                     CASE_TYPE_DESC
		, CASE_PRTY_TYP_NM                                   as                            CASE_PRIORITY_TYPE_DESC
		, CASE_RSOL_TYP_NM                                   as                          CASE_RESOLUTION_TYPE_DESC 
				FROM     LOGIC_CS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CS                             as ( SELECT * from    RENAME_CS   ),

---- JOIN LAYER ----

 JOIN_CS  as  ( SELECT * 
				FROM  FILTER_CS )
 SELECT 
   UNIQUE_ID_KEY
, CONTEXT_TYPE_CODE
, CASE_CATEGORY_TYPE_CODE
, CASE_TYPE_CODE
, CASE_PRIORITY_TYPE_CODE
, CASE_RESOLUTION_TYPE_CODE
, CONTEXT_TYPE_DESC
, CASE_CATEGORY_TYPE_DESC
, CASE_TYPE_DESC
, CASE_PRIORITY_TYPE_DESC
, CASE_RESOLUTION_TYPE_DESC
 FROM  JOIN_CS
  );
