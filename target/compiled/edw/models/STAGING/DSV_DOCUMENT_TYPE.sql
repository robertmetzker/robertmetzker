

---- SRC LAYER ----
WITH
SRC_DOC as ( SELECT *     from     STAGING.DST_DOCUMENT_TYPE ),
//SRC_DOC as ( SELECT *     from     DST_DOCUMENT_TYPE) ,

---- LOGIC LAYER ----

LOGIC_DOC as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, DOCM_TYP_REF_NO                                    as                                    DOCM_TYP_REF_NO 
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
		from SRC_DOC
            )

---- RENAME LAYER ----
,

RENAME_DOC as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, DOCM_TYP_REF_NO                                    as                     DOCUMENT_TYPE_REFERENCE_NUMBER
		, DOCM_TYP_NM                                        as                                 DOCUMENT_TYPE_NAME
		, DOCM_TYP_DESC                                      as                                 DOCUMENT_TYPE_DESC
		, DOCM_TYP_VER_NO                                    as                            DOCUMENT_VERSION_NUMBER
		, DOCM_TYP_VER_EVNT_DESC                             as                        DOCUMENT_VERSION_EVENT_DESC
		, DOCM_OCCR_TYP_CD                                   as                      DOCUMENT_OCCURRENCE_TYPE_CODE
		, DOCM_OCCR_TYP_NM                                   as                      DOCUMENT_OCCURRENCE_TYPE_DESC
		, DOCM_PPR_STK_TYP_CD                                as                     DOCUMENT_PAPER_STOCK_TYPE_CODE
		, DOCM_PPR_STK_TYP_NM                                as                     DOCUMENT_PAPER_STOCK_TYPE_DESC
		, DOCM_CTG_TYP_CD                                    as                        DOCUMENT_CATEGORY_TYPE_CODE
		, DOCM_CTG_TYP_NM                                    as                        DOCUMENT_CATEGORY_TYPE_DESC
		, DOCM_TYP_SRC_TYP_CD                                as                          DOCUMENT_SOURCE_TYPE_CODE
		, DOCM_TYP_SRC_TYP_NM                                as                          DOCUMENT_SOURCE_TYPE_DESC
		, DOCM_TYP_VER_EFF_DT                                as               DOCUMENT_TYPE_VERSION_EFFECTIVE_DATE
		, DOCM_TYP_VER_END_DT                                as                     DOCUMENT_TYPE_VERSION_END_DATE
		, DOCM_TYP_VER_TEMPL_FILE_NM                         as           DOCUMENT_TYPE_VERSION_TEMPLATE_FILE_NAME 
				FROM     LOGIC_DOC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DOC                            as ( SELECT * from    RENAME_DOC   ),

---- JOIN LAYER ----

 JOIN_DOC  as  ( SELECT * 
				FROM  FILTER_DOC )
 SELECT 
  UNIQUE_ID_KEY
, DOCUMENT_TYPE_REFERENCE_NUMBER
, DOCUMENT_TYPE_NAME
, DOCUMENT_TYPE_DESC
, DOCUMENT_VERSION_NUMBER
, DOCUMENT_VERSION_EVENT_DESC
, DOCUMENT_OCCURRENCE_TYPE_CODE
, DOCUMENT_OCCURRENCE_TYPE_DESC
, DOCUMENT_PAPER_STOCK_TYPE_CODE
, DOCUMENT_PAPER_STOCK_TYPE_DESC
, DOCUMENT_CATEGORY_TYPE_CODE
, DOCUMENT_CATEGORY_TYPE_DESC
, DOCUMENT_SOURCE_TYPE_CODE
, DOCUMENT_SOURCE_TYPE_DESC
, DOCUMENT_TYPE_VERSION_EFFECTIVE_DATE
, DOCUMENT_TYPE_VERSION_END_DATE
, DOCUMENT_TYPE_VERSION_TEMPLATE_FILE_NAME

 FROM  JOIN_DOC