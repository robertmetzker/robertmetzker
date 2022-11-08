---- SRC LAYER ----
WITH
SRC_DEV as ( SELECT *     from      DEV_VIEWS.PCMP.DOCUMENT_TYPE_VERSION_ELEMENT ),
SRC_DET as ( SELECT *     from      DEV_VIEWS.PCMP.DOCUMENT_ELEMENT_TYPE ),
//SRC_DEV as ( SELECT *     from     DOCUMENT_TYPE_VERSION_ELEMENT) ,
//SRC_DET as ( SELECT *     from     DOCUMENT_ELEMENT_TYPE) ,

---- LOGIC LAYER ----

LOGIC_DEV as ( SELECT 
		  DTVE_ID                                            as                                            DTVE_ID 
		, CNCURNCY_ID                                        as                                        CNCURNCY_ID 
		, DOCM_TYP_VER_ID                                    as                                    DOCM_TYP_VER_ID 
		, TRIM( DOCM_ELEM_TYP_CD )                           as                                   DOCM_ELEM_TYP_CD 
		, DTVE_DSPLY_ORD                                     as                                     DTVE_DSPLY_ORD 
		, upper( DTVE_REQ_IND )                              as                                       DTVE_REQ_IND 
		, upper( DTVE_ADD_ADTNL_ROW_IND )                    as                             DTVE_ADD_ADTNL_ROW_IND 
		, DTVE_PAR_ID                                        as                                        DTVE_PAR_ID 
		, upper( TRIM( DOCM_ENUM_VAL_TYP_CD_PAR_VAL ) )      as                       DOCM_ENUM_VAL_TYP_CD_PAR_VAL 
		, upper( TRIM( DTVE_PAR_ELE_VAL_TXT ) )              as                               DTVE_PAR_ELE_VAL_TXT 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_DEV
            ),
LOGIC_DET as ( SELECT 
		  upper( TRIM( DOCM_ELEM_TYP_NM ) )                  as                                   DOCM_ELEM_TYP_NM 
		, upper( TRIM( DOCM_ELEM_TYP_DESC ) )                as                                 DOCM_ELEM_TYP_DESC 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		, TRIM( DOCM_ELEM_TYP_CD )                           as                                   DOCM_ELEM_TYP_CD 
		from SRC_DET
            )

---- RENAME LAYER ----
,

RENAME_DEV as ( SELECT 
		  DTVE_ID                                            as                                            DTVE_ID
		, CNCURNCY_ID                                        as                                        CNCURNCY_ID
		, DOCM_TYP_VER_ID                                    as                                    DOCM_TYP_VER_ID
		, DOCM_ELEM_TYP_CD                                   as                                   DOCM_ELEM_TYP_CD
		, DTVE_DSPLY_ORD                                     as                                     DTVE_DSPLY_ORD
		, DTVE_REQ_IND                                       as                                       DTVE_REQ_IND
		, DTVE_ADD_ADTNL_ROW_IND                             as                             DTVE_ADD_ADTNL_ROW_IND
		, DTVE_PAR_ID                                        as                                        DTVE_PAR_ID
		, DOCM_ENUM_VAL_TYP_CD_PAR_VAL                       as                       DOCM_ENUM_VAL_TYP_CD_PAR_VAL
		, DTVE_PAR_ELE_VAL_TXT                               as                               DTVE_PAR_ELE_VAL_TXT
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_DEV   ), 
RENAME_DET as ( SELECT 
		  DOCM_ELEM_TYP_NM                                   as                                   DOCM_ELEM_TYP_NM
		, DOCM_ELEM_TYP_DESC                                 as                                 DOCM_ELEM_TYP_DESC
		, VOID_IND                                           as                                       DET_VOID_IND
		, DOCM_ELEM_TYP_CD                                   as                               DET_DOCM_ELEM_TYP_CD 
				FROM     LOGIC_DET   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DEV                            as ( SELECT * from    RENAME_DEV   ),
FILTER_DET                            as ( SELECT * from    RENAME_DET   ),

---- JOIN LAYER ----

DEV as ( SELECT * 
				FROM  FILTER_DEV
				LEFT JOIN FILTER_DET ON  FILTER_DEV.DOCM_ELEM_TYP_CD =  FILTER_DET.DET_DOCM_ELEM_TYP_CD  )
SELECT 
		  DTVE_ID
		, CNCURNCY_ID
		, DOCM_TYP_VER_ID
		, UPPER(DOCM_ELEM_TYP_CD) AS DOCM_ELEM_TYP_CD
		, DOCM_ELEM_TYP_NM
		, DOCM_ELEM_TYP_DESC
		, DTVE_DSPLY_ORD
		, DTVE_REQ_IND
		, DTVE_ADD_ADTNL_ROW_IND
		, DTVE_PAR_ID
		, DOCM_ENUM_VAL_TYP_CD_PAR_VAL
		, DTVE_PAR_ELE_VAL_TXT
		, VOID_IND 
from DEV