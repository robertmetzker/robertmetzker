---- SRC LAYER ----
WITH
SRC_DETV as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT_ELEMENT_TABLE_VALUE ),
SRC_DET as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT_ELEMENT_TYPE ),
//SRC_DETV as ( SELECT *     from     DOCUMENT_ELEMENT_TABLE_VALUE) ,
//SRC_DET as ( SELECT *     from     DOCUMENT_ELEMENT_TYPE) ,

---- LOGIC LAYER ----

LOGIC_DETV as ( SELECT 
		  DOCM_ELEM_TBL_VAL_ID                               as                               DOCM_ELEM_TBL_VAL_ID 
		, DOCM_ELEM_VAL_ID                                   as                                   DOCM_ELEM_VAL_ID 
		, TRIM( DOCM_ELEM_TYP_CD )                           as                                   DOCM_ELEM_TYP_CD 
		, NULLIF(UPPER(TRIM(DOCM_ELEM_TYP_CD_COL)), '')      as                               DOCM_ELEM_TYP_CD_COL 
		, DOCM_ELEM_TBL_VAL_COL_NO                           as                           DOCM_ELEM_TBL_VAL_COL_NO 
		, DOCM_ELEM_TBL_VAL_ROW_NO                           as                           DOCM_ELEM_TBL_VAL_ROW_NO 
		, NULLIF(UPPER(TRIM(DOCM_ELEM_TBL_VAL_VAL)), '')     as                              DOCM_ELEM_TBL_VAL_VAL 
		, NULLIF(UPPER(TRIM(VOID_IND)), '')                  as                                           VOID_IND  
		from SRC_DETV
            ),
LOGIC_DET as ( SELECT 
          NULLIF(UPPER(TRIM(DOCM_ELEM_TYP_NM)), '')          as                                   DOCM_ELEM_TYP_NM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		, TRIM( DOCM_ELEM_TYP_CD )                           as                                   DOCM_ELEM_TYP_CD 
		from SRC_DET
            )

---- RENAME LAYER ----
,

RENAME_DETV as ( SELECT 
		  DOCM_ELEM_TBL_VAL_ID                               as                               DOCM_ELEM_TBL_VAL_ID
		, DOCM_ELEM_VAL_ID                                   as                                   DOCM_ELEM_VAL_ID
		, DOCM_ELEM_TYP_CD                                   as                                   DOCM_ELEM_TYP_CD
		, DOCM_ELEM_TYP_CD_COL                               as                               DOCM_ELEM_TYP_CD_COL
		, DOCM_ELEM_TBL_VAL_COL_NO                           as                           DOCM_ELEM_TBL_VAL_COL_NO
		, DOCM_ELEM_TBL_VAL_ROW_NO                           as                           DOCM_ELEM_TBL_VAL_ROW_NO
		, DOCM_ELEM_TBL_VAL_VAL                              as                              DOCM_ELEM_TBL_VAL_VAL
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_DETV   ), 
RENAME_DET as ( SELECT 
		  DOCM_ELEM_TYP_NM                                   as                                   DOCM_ELEM_TYP_NM
		, VOID_IND                                           as                                       DET_VOID_IND
		, DOCM_ELEM_TYP_CD                                   as                               DET_DOCM_ELEM_TYP_CD 
				FROM     LOGIC_DET   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DETV                           as ( SELECT * from    RENAME_DETV 
                                            WHERE VOID_IND = 'N'  ),
FILTER_DET                            as ( SELECT * from    RENAME_DET 
                                            WHERE DET_VOID_IND = 'N'  ),

---- JOIN LAYER ----

DETV as ( SELECT * 
				FROM  FILTER_DETV
				LEFT JOIN FILTER_DET ON  FILTER_DETV.DOCM_ELEM_TYP_CD =  FILTER_DET.DET_DOCM_ELEM_TYP_CD  )
SELECT 
		  DOCM_ELEM_TBL_VAL_ID
		, DOCM_ELEM_VAL_ID
		, NULLIF( UPPER(TRIM (DOCM_ELEM_TYP_CD) ) , '')  as DOCM_ELEM_TYP_CD
		-- , UPPER(DOCM_ELEM_TYP_CD) as DOCM_ELEM_TYP_CD
		, DOCM_ELEM_TYP_NM
		, DOCM_ELEM_TYP_CD_COL
		-- , UPPER(DOCM_ELEM_TYP_CD_COL) as DOCM_ELEM_TYP_CD_COL
		, DOCM_ELEM_TBL_VAL_COL_NO
		, DOCM_ELEM_TBL_VAL_ROW_NO
		, DOCM_ELEM_TBL_VAL_VAL
		, VOID_IND 
from DETV