---- SRC LAYER ----
WITH
SRC_DEV as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT_ELEMENT_VALUE ),
SRC_DET as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT_ELEMENT_TYPE ),
SRC_DEVT as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT_ENUMERATION_VALUE_TYP ),
SRC_DECT as ( SELECT *     from     DEV_VIEWS.PCMP.DOCUMENT_ELEMENT_CLSFCTN_TYP ),
SRC_ADT as ( SELECT *     from     DEV_VIEWS.PCMP.APPLICATION_DATA_TYPE ),
//SRC_DEV as ( SELECT *     from     DOCUMENT_ELEMENT_VALUE) ,
//SRC_DET as ( SELECT *     from     DOCUMENT_ELEMENT_TYPE) ,
//SRC_DEVT as ( SELECT *     from     DOCUMENT_ENUMERATION_VALUE_TYP) ,
//SRC_DECT as ( SELECT *     from     DOCUMENT_ELEMENT_CLSFCTN_TYP) ,
//SRC_ADT as ( SELECT *     from     APPLICATION_DATA_TYPE) ,

---- LOGIC LAYER ----

LOGIC_DEV as ( SELECT 
		  DOCM_ELEM_VAL_ID                                 as                                   DOCM_ELEM_VAL_ID 
		, DOCM_ID                                          as                                            DOCM_ID 
		, TRIM( DOCM_ELEM_TYP_CD )                         as                                   DOCM_ELEM_TYP_CD 
		, TRIM( DOCM_ELEM_VAL_VAL )                        as                                  DOCM_ELEM_VAL_VAL 
		, upper( VOID_IND )                                as                                           VOID_IND 
		from SRC_DEV
            ),
LOGIC_DET as ( SELECT 
		  NULLIF(TRIM(DOCM_ELEM_TYP_NM), '')              as                                  DOCM_ELEM_TYP_NM 
		, NULLIF(TRIM(DOCM_ELEM_TYP_DESC), '')            as                                DOCM_ELEM_TYP_DESC 
		, UPPER(TRIM( DOCM_ELEM_CLSFCTN_TYP_CD ))         as                          DOCM_ELEM_CLSFCTN_TYP_CD 
		, upper(TRIM( APP_DATA_TYP_CD ))                  as                                   APP_DATA_TYP_CD 		
		, NULLIF(TRIM( XML_TYP_CD ), '')                  as                                        XML_TYP_CD 
		, NULLIF( TRIM( DOCM_ELEM_TYP_XML_TAG_NM ) , '')  as                          DOCM_ELEM_TYP_XML_TAG_NM 
		, NULLIF( TRIM( DOCM_FRMT_TYP_CD ) , '')          as                                  DOCM_FRMT_TYP_CD 
		, DOCM_ELEM_TYP_LNGTH                             as                               DOCM_ELEM_TYP_LNGTH 
		, TRIM( DOCM_ELEM_TYP_CD )                        as                                  DOCM_ELEM_TYP_CD 
		,  UPPER(VOID_IND )                               as                                          VOID_IND 
		, DATA_TYP_CD                                     as                                       DATA_TYP_CD
		from SRC_DET
            ),
LOGIC_DEVT as ( SELECT 
		  NULLIF(TRIM(DOCM_ENUM_VAL_TYP_NM), '')          as                               DOCM_ENUM_VAL_TYP_NM 
		, NULLIF(TRIM(DOCM_ENUM_VAL_TYP_DESC), '')        as                             DOCM_ENUM_VAL_TYP_DESC   
		, TRIM( DOCM_ENUM_VAL_TYP_CD )                    as                               DOCM_ENUM_VAL_TYP_CD 
		, upper( VOID_IND )                               as                                           VOID_IND 
		from SRC_DEVT
            ),
LOGIC_DECT as ( SELECT 
		   NULLIF(TRIM(DOCM_ELEM_CLSFCTN_TYP_NM), '')        as                           DOCM_ELEM_CLSFCTN_TYP_NM 
		, upper( TRIM( DOCM_ELEM_CLSFCTN_TYP_CD ) )          as                           DOCM_ELEM_CLSFCTN_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_DECT
            ),
LOGIC_ADT as ( SELECT 
		  NULLIF(TRIM(APP_DATA_TYP_NM), '')                  as                                    APP_DATA_TYP_NM 
		, upper(TRIM( APP_DATA_TYP_CD ))                     as                                    APP_DATA_TYP_CD 
		, upper( APP_DATA_TYP_VOID_IND )                     as                              APP_DATA_TYP_VOID_IND 
		from SRC_ADT
            )

---- RENAME LAYER ----
,

RENAME_DEV as ( SELECT 
		  DOCM_ELEM_VAL_ID                                   as                                   DOCM_ELEM_VAL_ID
		, DOCM_ID                                            as                                            DOCM_ID
		, DOCM_ELEM_TYP_CD                                   as                                   DOCM_ELEM_TYP_CD
		, DOCM_ELEM_VAL_VAL                                  as                                  DOCM_ELEM_VAL_VAL
		, VOID_IND                                           as                             DOCM_ELEM_VAL_VOID_IND 
				FROM     LOGIC_DEV   ), 
RENAME_DET as ( SELECT 
		  DOCM_ELEM_TYP_NM                                   as                                   DOCM_ELEM_TYP_NM
		, DOCM_ELEM_TYP_DESC                                 as                                 DOCM_ELEM_TYP_DESC
		, DOCM_ELEM_CLSFCTN_TYP_CD                           as                           DOCM_ELEM_CLSFCTN_TYP_CD
		, APP_DATA_TYP_CD                                    as                                    APP_DATA_TYP_CD
		, XML_TYP_CD                                         as                                         XML_TYP_CD
		, DOCM_ELEM_TYP_XML_TAG_NM                           as                           DOCM_ELEM_TYP_XML_TAG_NM
		, DOCM_FRMT_TYP_CD                                   as                                   DOCM_FRMT_TYP_CD
		, DOCM_ELEM_TYP_LNGTH                                as                                DOCM_ELEM_TYP_LNGTH
		, DOCM_ELEM_TYP_CD                                   as                               DET_DOCM_ELEM_TYP_CD
		, VOID_IND                                           as                                       DET_VOID_IND 
		, DATA_TYP_CD                                        as                                        DATA_TYP_CD
				FROM     LOGIC_DET   ), 
RENAME_DEVT as ( SELECT 
		  DOCM_ENUM_VAL_TYP_NM                               as                               DOCM_ENUM_VAL_TYP_NM
		, DOCM_ENUM_VAL_TYP_DESC                             as                             DOCM_ENUM_VAL_TYP_DESC
		, DOCM_ENUM_VAL_TYP_CD                               as                               DOCM_ENUM_VAL_TYP_CD
		, VOID_IND                                           as                                      DEVT_VOID_IND 
				FROM     LOGIC_DEVT   ), 
RENAME_DECT as ( SELECT 
		  DOCM_ELEM_CLSFCTN_TYP_NM                           as                           DOCM_ELEM_CLSFCTN_TYP_NM
		, DOCM_ELEM_CLSFCTN_TYP_CD                           as                      DECT_DOCM_ELEM_CLSFCTN_TYP_CD
		, VOID_IND                                           as                                      DECT_VOID_IND 
				FROM     LOGIC_DECT   ), 
RENAME_ADT as ( SELECT 
		  APP_DATA_TYP_NM                                    as                                    APP_DATA_TYP_NM
		, APP_DATA_TYP_CD                                    as                                ADT_APP_DATA_TYP_CD
		, APP_DATA_TYP_VOID_IND                              as                              APP_DATA_TYP_VOID_IND 
				FROM     LOGIC_ADT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DEV                            as ( SELECT * from    RENAME_DEV   ),
FILTER_DEVT                           as ( SELECT * from    RENAME_DEVT 
                                            WHERE DEVT_VOID_IND = 'N'  ),
FILTER_DET                            as ( SELECT * from    RENAME_DET 
                                            WHERE DET_VOID_IND = 'N'  ),
FILTER_DECT                           as ( SELECT * from    RENAME_DECT 
                                            WHERE DECT_VOID_IND = 'N'  ),
FILTER_ADT                            as ( SELECT * from    RENAME_ADT 
                                            WHERE APP_DATA_TYP_VOID_IND ='N'  ),

---- JOIN LAYER ----

DET as ( SELECT * 
				FROM  FILTER_DET
				LEFT JOIN  FILTER_DECT ON  FILTER_DET.DOCM_ELEM_CLSFCTN_TYP_CD =  FILTER_DECT.DECT_DOCM_ELEM_CLSFCTN_TYP_CD 
				LEFT JOIN  FILTER_ADT ON  FILTER_DET.APP_DATA_TYP_CD =  FILTER_ADT.ADT_APP_DATA_TYP_CD  ),
DEV as ( SELECT * 
				FROM  FILTER_DEV
				LEFT JOIN  FILTER_DEVT ON  FILTER_DEV.DOCM_ELEM_VAL_VAL =  FILTER_DEVT.DOCM_ENUM_VAL_TYP_CD 
						LEFT JOIN  DET ON  FILTER_DEV.DOCM_ELEM_TYP_CD = DET.DET_DOCM_ELEM_TYP_CD  
						)				

SELECT 
  DOCM_ELEM_VAL_ID
, DOCM_ID
, NULLIF(UPPER(DOCM_ELEM_TYP_CD), '') AS DOCM_ELEM_TYP_CD
, UPPER(DOCM_ELEM_TYP_NM) as DOCM_ELEM_TYP_NM
, UPPER(DOCM_ELEM_TYP_DESC)  as DOCM_ELEM_TYP_DESC
, NULLIF(UPPER(DOCM_ELEM_VAL_VAL), '') AS DOCM_ELEM_VAL_VAL
, UPPER(DOCM_ENUM_VAL_TYP_NM) as DOCM_ENUM_VAL_TYP_NM
, UPPER(DOCM_ENUM_VAL_TYP_DESC) as DOCM_ENUM_VAL_TYP_DESC
, NULLIF(UPPER(NVL(TRIM(DOCM_ENUM_VAL_TYP_NM),TRIM(DOCM_ELEM_VAL_VAL))), '') AS DOCM_ELEM_VAL_TXT
, CASE WHEN DATA_TYP_CD = 'date' and try_to_numeric(DOCM_ELEM_VAL_VAL) and LENGTH(TRIM(DOCM_ELEM_VAL_VAL)) = 8 then   TO_DATE(DOCM_ELEM_VAL_VAL, 'MMDDYYYY') 
       WHEN DATA_TYP_CD = 'date' and LENGTH(TRIM(DOCM_ELEM_VAL_VAL)) = 10 then   CAST(DOCM_ELEM_VAL_VAL AS DATE) else NULL END  AS DOCM_ELEM_VAL_DATE
, DOCM_ELEM_VAL_VOID_IND
, NULLIF(DOCM_ELEM_CLSFCTN_TYP_CD, '') as DOCM_ELEM_CLSFCTN_TYP_CD
, UPPER(DOCM_ELEM_CLSFCTN_TYP_NM) as DOCM_ELEM_CLSFCTN_TYP_NM
, NULLIF(UPPER(TRIM( APP_DATA_TYP_CD) ), '') as APP_DATA_TYP_CD 
, UPPER(APP_DATA_TYP_NM) as APP_DATA_TYP_NM
, UPPER(XML_TYP_CD) as XML_TYP_CD
, UPPER(DOCM_ELEM_TYP_XML_TAG_NM) as DOCM_ELEM_TYP_XML_TAG_NM
, UPPER(DOCM_FRMT_TYP_CD) as DOCM_FRMT_TYP_CD
, UPPER(DOCM_ELEM_TYP_LNGTH) as DOCM_ELEM_TYP_LNGTH
, NULLIF(UPPER(DOCM_ENUM_VAL_TYP_CD), '') as DOCM_ENUM_VAL_TYP_CD 
from DEV